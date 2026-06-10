"""
agents/research_agent.py — LangGraph pre-fight research agent.

For each fight on the upcoming card the agent:
  1. Searches for recent news, injuries, and previews (Tavily → DuckDuckGo fallback)
  2. Extracts a structured signal via Claude (flags, adjustment, confidence)
  3. Loops over all fights, then compiles a report and writes to BigQuery

Graph topology:
  START → search_fighter_news → analyze_fight → next_fight
          ↑                                         │
          └──────────── (more fights) ──────────────┘
                        (done) → compile_report → END

Usage:
  ANTHROPIC_API_KEY=... TAVILY_API_KEY=... python3 agents/research_agent.py
"""

import json
import logging
import os
import sys
import warnings
from datetime import datetime, timezone
from operator import add
from typing import Annotated

import pandas as pd
from google.cloud import bigquery
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

warnings.filterwarnings("ignore", category=DeprecationWarning)

from config import DATASET, PROJECT_ID, TABLE_COMING_EVENT  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

LLM_MODEL          = "claude-haiku-4-5-20251001"   # cost-efficient for bulk per-fight calls
SEARCH_MAX_RESULTS = 5
TABLE_SIGNALS      = "research_signals"

SYSTEM_PROMPT = """\
You are a UFC fight analyst. Given web search results about two fighters and their \
upcoming matchup, extract a structured research signal.

Return ONLY valid JSON matching this exact schema — no markdown fences, no extra keys:
{
  "fight": "<fighter_1> vs <fighter_2>",
  "weight_class": "<weight class>",
  "flags": ["<tag>", ...],
  "summary": "<2-3 sentence analysis>",
  "adjustment": <float between -0.15 and 0.15>,
  "confidence": "<low|medium|high>",
  "sources": ["<snippet or URL>", ...]
}

Allowed flag values (use any that apply):
  injury_concern, short_notice, weight_cut_issues,
  momentum_positive, momentum_negative, stylistic_advantage,
  public_perception_gap, camp_change, layoff_concern,
  home_crowd_advantage, title_pressure, debut_nerves

adjustment: suggested delta to the model's win probability for fighter_1.
  Negative → fighter_1 is worse than the model estimates.
  Use 0.0 when the search results are insufficient to justify a change.
confidence: how much usable signal was found — low / medium / high.
"""


# ── State ─────────────────────────────────────────────────────────────────────

class AgentState(TypedDict):
    fights:            list[dict]                 # full upcoming card (read-only)
    current_fight_idx: int                        # pointer into fights[]
    search_results:    dict[str, list[str]]       # fight_key → [snippet, ...]
    signals:           Annotated[list[dict], add] # accumulated per-fight signals


# ── Search helpers ─────────────────────────────────────────────────────────────

def _fight_key(fight: dict) -> str:
    return f"{fight['fighter_1']}|{fight['fighter_2']}".replace(" ", "_")


def _tavily_search(query: str) -> list[str]:
    from langchain_community.tools.tavily_search import TavilySearchResults  # noqa: F401
    tool = TavilySearchResults(max_results=SEARCH_MAX_RESULTS)
    results = tool.invoke(query)
    return [r.get("content", r.get("snippet", "")) for r in results if isinstance(r, dict)]


def _duckduckgo_search(query: str) -> list[str]:
    from langchain_community.utilities import DuckDuckGoSearchAPIWrapper
    ddg = DuckDuckGoSearchAPIWrapper(max_results=SEARCH_MAX_RESULTS)
    return [ddg.run(query)]


def _search(query: str) -> list[str]:
    """Run a web search; prefer Tavily when key is set, fall back to DuckDuckGo."""
    try:
        if os.environ.get("TAVILY_API_KEY"):
            return _tavily_search(query)
        return _duckduckgo_search(query)
    except Exception as exc:
        log.warning("Search failed for %r: %s", query, exc)
        return []


# ── BQ helpers ────────────────────────────────────────────────────────────────

def _load_card(client: bigquery.Client) -> list[dict]:
    sql = f"""
        SELECT DISTINCT
            fighter1_name  AS fighter_1,
            fighter2_name  AS fighter_2,
            weight_class,
            event_name,
            event_date
        FROM `{PROJECT_ID}.{DATASET}.{TABLE_COMING_EVENT}`
        ORDER BY event_date, fighter1_name
    """
    df = client.query(sql).to_dataframe()
    log.info("Loaded %d fights from %s", len(df), TABLE_COMING_EVENT)
    return df.to_dict("records")


def _save_signals(signals: list[dict]) -> None:
    if not signals:
        return
    rows = [
        {
            "fight":        s.get("fight", ""),
            "event_name":   s.get("event_name", ""),
            "event_date":   s.get("event_date", ""),
            "weight_class": s.get("weight_class", ""),
            "flags":        json.dumps(s.get("flags", [])),
            "summary":      s.get("summary", ""),
            "adjustment":   float(s.get("adjustment", 0.0)),
            "confidence":   s.get("confidence", "low"),
            "sources":      json.dumps(s.get("sources", [])),
            "analyzed_at":  s.get("analyzed_at", ""),
        }
        for s in signals
    ]
    client = bigquery.Client(project=PROJECT_ID)
    dest   = f"{PROJECT_ID}.{DATASET}.{TABLE_SIGNALS}"
    job    = client.load_table_from_dataframe(
        pd.DataFrame(rows),
        dest,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        ),
    )
    job.result()
    log.info("Saved %d signals → %s", len(rows), dest)


# ── Graph nodes ───────────────────────────────────────────────────────────────

def search_fighter_news(state: AgentState) -> dict:
    """Collect search snippets for the current fight."""
    idx   = state["current_fight_idx"]
    fight = state["fights"][idx]
    f1    = fight["fighter_1"]
    f2    = fight["fighter_2"]
    wc    = fight["weight_class"]
    key   = _fight_key(fight)

    log.info("[%d/%d] Searching: %s vs %s (%s)", idx + 1, len(state["fights"]), f1, f2, wc)

    snippets: list[str] = []
    snippets += _search(f"{f1} UFC news injury 2025 2026")
    snippets += _search(f"{f2} UFC news injury 2025 2026")
    snippets += _search(f"{f1} vs {f2} UFC prediction analysis")
    log.info("  → %d snippets collected", len(snippets))

    updated = {**state.get("search_results", {}), key: snippets}
    return {"search_results": updated}


def analyze_fight(state: AgentState) -> dict:
    """Call Claude to extract a structured signal from search results."""
    idx   = state["current_fight_idx"]
    fight = state["fights"][idx]
    f1    = fight["fighter_1"]
    f2    = fight["fighter_2"]
    wc    = fight["weight_class"]
    key   = _fight_key(fight)

    snippets = state["search_results"].get(key, [])
    context  = "\n\n".join(snippets[:12]) if snippets else "No search results available."

    llm = ChatAnthropic(
        model=LLM_MODEL,
        api_key=os.environ["ANTHROPIC_API_KEY"],
        max_tokens=512,
        temperature=0,
    )

    log.info("  Analyzing %s vs %s via %s ...", f1, f2, LLM_MODEL)
    response = llm.invoke([
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=(
            f"Fight: {f1} vs {f2} — {wc}\n\n"
            f"Search results:\n{context}\n\n"
            f"Generate the JSON signal."
        )),
    ])

    # Parse JSON — strip markdown fences if the model adds them
    raw = response.content.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw   = parts[1].lstrip("json").strip() if len(parts) > 1 else raw

    try:
        signal = json.loads(raw)
    except json.JSONDecodeError as exc:
        log.warning("  JSON parse failed (%s) — using null signal", exc)
        signal = {
            "fight":        f"{f1} vs {f2}",
            "weight_class": wc,
            "flags":        [],
            "summary":      "Analysis unavailable — JSON parse error.",
            "adjustment":   0.0,
            "confidence":   "low",
            "sources":      snippets[:2],
        }

    signal["event_name"]  = fight.get("event_name", "")
    signal["event_date"]  = str(fight.get("event_date", ""))
    signal["analyzed_at"] = datetime.now(timezone.utc).isoformat()

    log.info(
        "  → flags=%s  adj=%+.3f  confidence=%s",
        signal.get("flags"), signal.get("adjustment"), signal.get("confidence"),
    )
    return {"signals": [signal]}   # Annotated[list, add] → appended to state


def next_fight(state: AgentState) -> dict:
    """Advance the fight pointer by one."""
    return {"current_fight_idx": state["current_fight_idx"] + 1}


def compile_report(state: AgentState) -> dict:
    """Print the full report and write signals to BigQuery."""
    signals = state["signals"]

    print("\n" + "=" * 70)
    print("  UFC PRE-FIGHT RESEARCH REPORT")
    print("=" * 70)
    for s in signals:
        adj        = s.get("adjustment", 0.0)
        confidence = s.get("confidence", "?")
        flags      = ", ".join(s.get("flags", [])) or "—"
        print(f"\n  {s['fight']}  [{s['weight_class']}]")
        print(f"  Flags      : {flags}")
        print(f"  Adjustment : {adj:+.3f}  (confidence: {confidence})")
        print(f"  Summary    : {s.get('summary', '')}")
    print("\n" + "=" * 70 + "\n")

    try:
        _save_signals(signals)
    except Exception as exc:
        log.error("Failed to save signals to BigQuery: %s", exc)

    return {}


# ── Routing ───────────────────────────────────────────────────────────────────

def _route_next(state: AgentState) -> str:
    if state["current_fight_idx"] < len(state["fights"]):
        return "search_fighter_news"
    return "compile_report"


# ── Graph assembly ────────────────────────────────────────────────────────────

def build_graph() -> StateGraph:
    g = StateGraph(AgentState)

    g.add_node("search_fighter_news", search_fighter_news)
    g.add_node("analyze_fight",       analyze_fight)
    g.add_node("next_fight",          next_fight)
    g.add_node("compile_report",      compile_report)

    g.add_edge(START,                 "search_fighter_news")
    g.add_edge("search_fighter_news", "analyze_fight")
    g.add_edge("analyze_fight",       "next_fight")
    g.add_conditional_edges(
        "next_fight",
        _route_next,
        {
            "search_fighter_news": "search_fighter_news",
            "compile_report":      "compile_report",
        },
    )
    g.add_edge("compile_report", END)

    return g


# ── Entry ─────────────────────────────────────────────────────────────────────

def main() -> None:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        log.error("ANTHROPIC_API_KEY not set — export it before running")
        sys.exit(1)

    log.info("=" * 60)
    log.info("research_agent.py  —  UFC pre-fight research agent (LangGraph)")
    log.info("Search backend: %s", "Tavily" if os.environ.get("TAVILY_API_KEY") else "DuckDuckGo (fallback)")
    log.info("=" * 60)

    client = bigquery.Client(project=PROJECT_ID)
    fights = _load_card(client)

    if not fights:
        log.warning("No upcoming fights found in %s — nothing to research.", TABLE_COMING_EVENT)
        sys.exit(0)

    initial_state: AgentState = {
        "fights":            fights,
        "current_fight_idx": 0,
        "search_results":    {},
        "signals":           [],
    }

    app = build_graph().compile()
    log.info("Graph compiled — processing %d fights", len(fights))
    app.invoke(initial_state)


if __name__ == "__main__":
    main()
