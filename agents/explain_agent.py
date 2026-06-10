"""
agents/explain_agent.py — LangGraph fight explanation agent.

For each fight in UFC_data.predictions the agent:
  1. Gathers model probability, market probability, edge,
     top-5 per-fight feature contributions (XGBoost pred_contribs / SHAP),
     and the matching research signal from UFC_data.research_signals
  2. Generates a 3-5 sentence analyst-style explanation via Claude Haiku
  3. Compiles all explanations into reports/latest_card.md

Graph topology:
  START → gather_context → generate_explanation → next_prediction
          ↑                                              │
          └──────────── (more fights) ──────────────────┘
                        (done) → compile_report → END
"""

import logging
import os
import sys
import warnings
from operator import add
from pathlib import Path
from typing import Annotated

import numpy as np
import xgboost as xgb
from google.cloud import bigquery
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DATASET, PROJECT_ID, TABLE_FEATURES  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

MODEL_PATH             = Path(__file__).parent.parent / "models" / "xgb_v2.joblib"
REPORTS_DIR            = Path(__file__).parent.parent / "reports"
REPORT_PATH            = REPORTS_DIR / "latest_card.md"
LLM_MODEL              = "claude-haiku-4-5-20251001"
TABLE_PREDICTIONS      = "predictions"
TABLE_RESEARCH_SIGNALS = "research_signals"
TOP_N_FEATURES         = 5
CAT_COLS               = ["weight_class", "f_1_fighter_stance", "f_2_fighter_stance"]

SYSTEM_PROMPT = """\
You are a UFC fight analyst writing concise pre-fight breakdowns for a sports betting audience.
Write 3-5 sentences in English. Be direct and analytical — no hype, no filler.
Focus on: what the model sees, what the market prices, and whether the edge is actionable.
Reference specific fighter attributes or recent form when the research signal is available.
"""


# ── State ─────────────────────────────────────────────────────────────────────

class AgentState(TypedDict):
    predictions:      list[dict]               # rows from UFC_data.predictions
    research_signals: list[dict]               # rows from UFC_data.research_signals
    current_idx:      int
    current_context:  dict                     # ephemeral — overwritten each iteration
    explanations:     Annotated[list[dict], add]


# ── Model helpers ─────────────────────────────────────────────────────────────

_MODEL_CACHE: dict = {}


def _load_model() -> dict:
    if not _MODEL_CACHE:
        import joblib
        payload = joblib.load(MODEL_PATH)
        _MODEL_CACHE.update(payload)
        log.info("Model loaded: %d features", len(payload["feat_names"]))
    return _MODEL_CACHE


def _top_features_global(n: int = TOP_N_FEATURES) -> list[tuple[str, float]]:
    """Global feature importances — used when no per-fight feature row is available."""
    payload      = _load_model()
    feat_names   = payload["feat_names"]
    importances  = payload["model"].base.feature_importances_
    idx          = np.argsort(importances)[::-1][:n]
    return [(feat_names[i], float(importances[i])) for i in idx]


def _top_features_for_fight(
    client: bigquery.Client,
    fight_url: str,
    n: int = TOP_N_FEATURES,
) -> list[tuple[str, float]]:
    """
    Per-fight contributions via XGBoost pred_contribs (SHAP leaf values).
    Positive contribution → fighter_1 advantage. Negative → fighter_2 advantage.
    Falls back to global importances when the feature row is absent from UFC_features.
    """
    if not fight_url:
        return _top_features_global(n)

    payload    = _load_model()
    feat_names = payload["feat_names"]
    encoder    = payload["encoder"]

    sql = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.{TABLE_FEATURES}`
        WHERE fight_url = @fight_url
        LIMIT 1
    """
    job_cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("fight_url", "STRING", fight_url)]
    )
    df = client.query(sql, job_config=job_cfg).to_dataframe()

    if df.empty:
        log.debug("No feature row for fight_url=%s — using global importances", fight_url)
        return _top_features_global(n)

    # Ensure all model features are present (fill missing with NaN)
    for c in feat_names:
        if c not in df.columns:
            df[c] = np.nan

    X = df[feat_names].copy()
    for c in X.select_dtypes(include="Int64").columns:
        X[c] = X[c].astype("float64")

    active_cats = [c for c in CAT_COLS if c in feat_names]
    if active_cats:
        X[active_cats] = encoder.transform(X[active_cats].fillna("__missing__"))

    booster  = payload["model"].base.get_booster()
    dm       = xgb.DMatrix(X.values.astype(np.float32), feature_names=feat_names)
    contribs = booster.predict(dm, pred_contribs=True)   # (1, n_features + 1)
    row      = contribs[0, :-1]                           # drop bias column

    idx = np.argsort(np.abs(row))[::-1][:n]
    return [(feat_names[i], float(row[i])) for i in idx]


# ── Data helpers ──────────────────────────────────────────────────────────────

def _bq() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def _load_predictions(client: bigquery.Client) -> list[dict]:
    sql = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.{TABLE_PREDICTIONS}`
        ORDER BY event_date, fighter_1
    """
    df = client.query(sql).to_dataframe()
    log.info("Loaded %d predictions from %s", len(df), TABLE_PREDICTIONS)
    return df.to_dict("records")


def _load_research_signals(client: bigquery.Client) -> list[dict]:
    try:
        sql = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{TABLE_RESEARCH_SIGNALS}`"
        df  = client.query(sql).to_dataframe()
        log.info("Loaded %d research signals", len(df))
        return df.to_dict("records")
    except Exception as exc:
        log.warning("Could not load research signals (%s) — proceeding without them", exc)
        return []


def _match_signal(prediction: dict, signals: list[dict]) -> dict | None:
    """Match a research signal to a prediction by fighter names (case-insensitive)."""
    f1 = prediction.get("fighter_1", "").strip().lower()
    f2 = prediction.get("fighter_2", "").strip().lower()
    for s in signals:
        fight_str = s.get("fight", "").lower()
        if f1 in fight_str and f2 in fight_str:
            return s
    return None


def _fmt_contributions(features: list[tuple[str, float]]) -> str:
    lines = []
    for name, contrib in features:
        direction = "↑ favours f1" if contrib > 0 else "↓ favours f2"
        lines.append(f"  {name}: {contrib:+.4f}  ({direction})")
    return "\n".join(lines)


# ── Graph nodes ───────────────────────────────────────────────────────────────

def gather_context(state: AgentState) -> dict:
    """Assemble model output, market odds, feature contributions, and research signal."""
    idx  = state["current_idx"]
    pred = state["predictions"][idx]
    f1   = pred.get("fighter_1", "?")
    f2   = pred.get("fighter_2", "?")

    log.info("[%d/%d] Gathering context: %s vs %s", idx + 1, len(state["predictions"]), f1, f2)

    client   = _bq()
    features = _top_features_for_fight(client, str(pred.get("fight_url") or ""))
    signal   = _match_signal(pred, state["research_signals"])

    context = {
        "fighter_1":      f1,
        "fighter_2":      f2,
        "weight_class":   pred.get("weight_class", ""),
        "event_name":     pred.get("event_name", ""),
        "model_prob_f1":  pred.get("model_prob_f1"),
        "model_prob_f2":  pred.get("model_prob_f2"),
        "market_prob_f1": pred.get("market_prob_f1"),
        "market_prob_f2": pred.get("market_prob_f2"),
        "edge":           pred.get("edge"),
        "recommended":    pred.get("recommended", False),
        "top_features":   features,
        "signal":         signal,
    }
    return {"current_context": context}


def generate_explanation(state: AgentState) -> dict:
    """Call Claude Haiku to produce a natural-language fight breakdown."""
    ctx = state["current_context"]
    f1  = ctx["fighter_1"]
    f2  = ctx["fighter_2"]

    prob_f1  = ctx.get("model_prob_f1") or 0.0
    mkt_f1   = ctx.get("market_prob_f1")
    edge     = ctx.get("edge")
    signal   = ctx.get("signal") or {}
    features = ctx.get("top_features", [])

    prob_str = f"{prob_f1 * 100:.1f}%"
    mkt_str  = f"{mkt_f1 * 100:.1f}%" if mkt_f1 is not None else "N/A"
    edge_str = f"{edge * 100:+.1f}%" if edge is not None else "N/A"

    feat_block = _fmt_contributions(features) if features else "  (not available)"

    if signal:
        signal_block = (
            f"Flags: {', '.join(signal.get('flags', [])) or '—'}\n"
            f"Summary: {signal.get('summary', '')}\n"
            f"Suggested adjustment to model: {signal.get('adjustment', 0.0):+.3f} "
            f"(confidence: {signal.get('confidence', 'N/A')})"
        )
    else:
        signal_block = "No research signal available."

    user_msg = (
        f"Fight: {f1} vs {f2} — {ctx.get('weight_class', '')}\n"
        f"Event: {ctx.get('event_name', '')}\n\n"
        f"Model win probability for {f1}: {prob_str}\n"
        f"Market implied probability for {f1}: {mkt_str}\n"
        f"Model edge: {edge_str}  |  Bet recommended: {'YES' if ctx.get('recommended') else 'NO'}\n\n"
        f"Top {TOP_N_FEATURES} model features for this fight (XGBoost leaf contributions):\n"
        f"{feat_block}\n\n"
        f"Research signal:\n{signal_block}\n\n"
        f"Write the explanation."
    )

    log.info("  Calling %s for %s vs %s ...", LLM_MODEL, f1, f2)
    llm = ChatAnthropic(
        model=LLM_MODEL,
        api_key=os.environ["ANTHROPIC_API_KEY"],
        max_tokens=300,
        temperature=0.3,
    )
    response = llm.invoke([
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=user_msg),
    ])

    explanation = {
        "fighter_1":      f1,
        "fighter_2":      f2,
        "weight_class":   ctx.get("weight_class", ""),
        "event_name":     ctx.get("event_name", ""),
        "model_prob_f1":  prob_f1,
        "market_prob_f1": mkt_f1,
        "edge":           edge,
        "recommended":    ctx.get("recommended", False),
        "top_features":   features,
        "signal_flags":   signal.get("flags", []),
        "text":           response.content.strip(),
    }
    log.info("  Generated (%d chars)", len(explanation["text"]))
    return {"explanations": [explanation]}   # Annotated[list, add] → appended


def next_prediction(state: AgentState) -> dict:
    return {"current_idx": state["current_idx"] + 1}


def compile_report(state: AgentState) -> dict:
    """Render markdown report → reports/latest_card.md and stdout."""
    explanations = state["explanations"]
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    first = explanations[0] if explanations else {}
    event = first.get("event_name", "UFC Card")

    lines: list[str] = [
        f"# {event} — Pre-Fight Analysis\n",
        "*Generated by ufc-pipeline · explain_agent.py*\n",
        "---\n",
    ]

    for ex in explanations:
        f1       = ex["fighter_1"]
        f2       = ex["fighter_2"]
        wc       = ex["weight_class"]
        prob     = ex.get("model_prob_f1") or 0.0
        mkt      = ex.get("market_prob_f1")
        edge     = ex.get("edge")
        rec      = ex.get("recommended", False)
        flags    = ex.get("signal_flags", [])
        features = ex.get("top_features", [])

        prob_str = f"{prob * 100:.1f}%"
        mkt_str  = f"{mkt * 100:.1f}%" if mkt is not None else "N/A"
        edge_str = f"{edge * 100:+.1f}%" if edge is not None else "N/A"
        rec_str  = "✅ **RECOMMENDED**" if rec else "—"

        lines += [
            f"## {f1} vs {f2}  `{wc}`\n",
            "| | |",
            "|---|---|",
            f"| Model — {f1} | **{prob_str}** |",
            f"| Market | {mkt_str} |",
            f"| Edge | {edge_str} |",
            f"| Bet | {rec_str} |\n",
        ]

        if flags:
            flag_badges = " ".join(f"`{f}`" for f in flags)
            lines.append(f"**Research flags:** {flag_badges}\n")

        if features:
            lines += [
                "**Top model features (per-fight contribution):**\n",
                "| Feature | Contribution | Direction |",
                "|---|---|---|",
            ]
            for name, contrib in features:
                direction = "↑ favours f1" if contrib > 0 else "↓ favours f2"
                lines.append(f"| `{name}` | `{contrib:+.4f}` | {direction} |")
            lines.append("")

        lines += [ex["text"] + "\n", "---\n"]

    report = "\n".join(lines)
    REPORT_PATH.write_text(report, encoding="utf-8")
    log.info("Report saved → %s", REPORT_PATH)
    print("\n" + report)
    return {}


# ── Routing ───────────────────────────────────────────────────────────────────

def _route_next(state: AgentState) -> str:
    if state["current_idx"] < len(state["predictions"]):
        return "gather_context"
    return "compile_report"


# ── Graph assembly ────────────────────────────────────────────────────────────

def build_graph() -> StateGraph:
    g = StateGraph(AgentState)

    g.add_node("gather_context",       gather_context)
    g.add_node("generate_explanation", generate_explanation)
    g.add_node("next_prediction",      next_prediction)
    g.add_node("compile_report",       compile_report)

    g.add_edge(START,                  "gather_context")
    g.add_edge("gather_context",       "generate_explanation")
    g.add_edge("generate_explanation", "next_prediction")
    g.add_conditional_edges(
        "next_prediction",
        _route_next,
        {
            "gather_context": "gather_context",
            "compile_report": "compile_report",
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
    log.info("explain_agent.py  —  UFC fight explanation agent (LangGraph)")
    log.info("=" * 60)

    client  = _bq()
    preds   = _load_predictions(client)
    signals = _load_research_signals(client)

    if not preds:
        log.warning(
            "No predictions in %s.%s — run predict_upcoming.py first.",
            DATASET, TABLE_PREDICTIONS,
        )
        sys.exit(0)

    initial_state: AgentState = {
        "predictions":      preds,
        "research_signals": signals,
        "current_idx":      0,
        "current_context":  {},
        "explanations":     [],
    }

    app = build_graph().compile()
    log.info("Graph compiled — explaining %d fights", len(preds))
    app.invoke(initial_state)


if __name__ == "__main__":
    main()
