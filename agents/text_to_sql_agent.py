"""
agents/text_to_sql_agent.py — LangGraph text-to-SQL agent for UFC data.

Translates natural-language questions into BigQuery SQL,
executes the query, and returns a polished Polish-language answer.

Graph topology:
  START → load_schema → generate_sql → execute_sql → format_answer → END
                              ↑              │ (error, retry < 2)
                              └──────────────┘

Usage:
  python3 agents/text_to_sql_agent.py "Ile walk wygrał Israel Adesanya przez KO?"
  python3 agents/text_to_sql_agent.py   # interactive mode

Example questions:
  - "Ile walk wygrał Israel Adesanya przez KO?"
  - "Kto ma najdłuższą serię zwycięstw w wadze lekkiej?"
  - "Jaki jest średni reach zawodników w top 15 wagi ciężkiej?"
  - "Które gale miały najwięcej walk kończonych przez poddanie?"
  - "Jakie są ostatnie predykcje modelu i który zawodnik ma największy edge?"
"""

import logging
import os
import sys
import warnings
from pathlib import Path

from google.cloud import bigquery
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DATASET, PROJECT_ID  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

LLM_MODEL        = "claude-haiku-4-5-20251001"
MAX_RETRIES      = 2
MAX_RESULT_ROWS  = 25     # cap displayed rows to keep the answer prompt concise
MAX_COLS_SCHEMA  = 25     # cap columns shown per table in schema context

SCHEMA_TABLES = [
    "UFC_fights_data",
    "UFC_fighters_data",
    "UFC_events_data",
    "UFC_rankings",
    "odds_snapshots",
    "UFC_features",
    "predictions",
]

SQL_SYSTEM_PROMPT = f"""\
You are a BigQuery SQL expert. The database is Google BigQuery project `{PROJECT_ID}`, dataset `{DATASET}`.

Rules:
- Use fully-qualified table names: `{PROJECT_ID}.{DATASET}.<table>`
- Return ONLY the SQL statement — no markdown, no backtick fences, no explanation.
- Use standard BigQuery SQL syntax (SAFE_DIVIDE, DATE functions, etc.).
- Column names are case-sensitive; use them exactly as shown in the schema.
- For fighter names, use LOWER() and LIKE for fuzzy matching.
- Always add LIMIT 50 unless the query is an aggregation returning a single row.
- UFC_features has 2 400+ rolling-window columns; avoid SELECT * on it.
"""

ANSWER_SYSTEM_PROMPT = """\
Jesteś analitykiem danych UFC. Na podstawie pytania i wyników zapytania SQL \
sformułuj zwięzłą, czytelną odpowiedź w języku polskim.
Jeśli wynik jest pusty, powiedz że brak danych.
Jeśli SQL zakończył się błędem, wyjaśnij krótko co poszło nie tak.
Nie cytuj SQL — skup się na odpowiedzi merytorycznej.
"""


# ── State ─────────────────────────────────────────────────────────────────────

class AgentState(TypedDict):
    question:       str
    schema_context: str
    generated_sql:  str
    sql_error:      str    # last execution error; empty on success
    retry_count:    int
    query_result:   str
    answer:         str


# ── Helpers ───────────────────────────────────────────────────────────────────

def _bq() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def _fetch_schema(client: bigquery.Client) -> str:
    """Build a compact schema description for the LLM prompt."""
    blocks: list[str] = []
    for table_name in SCHEMA_TABLES:
        try:
            ref    = client.get_table(f"{PROJECT_ID}.{DATASET}.{table_name}")
            fields = ref.schema
            lines  = [f"Table: `{PROJECT_ID}.{DATASET}.{table_name}`", "Columns:"]
            for f in fields[:MAX_COLS_SCHEMA]:
                lines.append(f"  {f.name}  {f.field_type}")
            if len(fields) > MAX_COLS_SCHEMA:
                lines.append(f"  ... ({len(fields) - MAX_COLS_SCHEMA} more columns)")
            blocks.append("\n".join(lines))
        except Exception as exc:
            log.warning("Could not fetch schema for %s: %s", table_name, exc)
    return "\n\n".join(blocks)


def _strip_sql_fences(raw: str) -> str:
    """Remove markdown code fences if the LLM adds them despite instructions."""
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw   = parts[1].lstrip("sql").strip() if len(parts) > 1 else raw
    return raw


def _run_query(client: bigquery.Client, sql: str) -> tuple[str, str]:
    """
    Execute SQL and return (result_str, error_str).
    result_str is empty on error; error_str is empty on success.
    """
    try:
        df = client.query(sql).to_dataframe()
        if df.empty:
            return "(query returned 0 rows)", ""
        if len(df) > MAX_RESULT_ROWS:
            preview = df.head(MAX_RESULT_ROWS).to_string(index=False)
            return f"{preview}\n... ({len(df) - MAX_RESULT_ROWS} more rows truncated)", ""
        return df.to_string(index=False), ""
    except Exception as exc:
        return "", str(exc)


def _llm(temperature: float = 0) -> ChatAnthropic:
    return ChatAnthropic(
        model=LLM_MODEL,
        api_key=os.environ["ANTHROPIC_API_KEY"],
        max_tokens=512,
        temperature=temperature,
    )


# ── Graph nodes ───────────────────────────────────────────────────────────────

def load_schema(state: AgentState) -> dict:
    """Fetch table schemas from BigQuery and store in state."""
    log.info("Loading schema for %d tables...", len(SCHEMA_TABLES))
    client = _bq()
    schema = _fetch_schema(client)
    log.info("Schema loaded (%d chars)", len(schema))
    return {"schema_context": schema}


def generate_sql(state: AgentState) -> dict:
    """Generate (or regenerate with error context) a BigQuery SQL query."""
    retry = state.get("retry_count", 0)
    error = state.get("sql_error", "")

    if retry == 0:
        log.info("Generating SQL for: %r", state["question"])
    else:
        log.info("Retrying SQL generation (attempt %d/%d) after error: %s", retry, MAX_RETRIES, error[:120])

    # On retry: include the previous SQL and error message so the LLM can fix it
    error_ctx = ""
    if error:
        error_ctx = (
            f"\n\nThe previous SQL statement failed with this error:\n{error}\n"
            f"Previous SQL:\n{state.get('generated_sql', '')}\n"
            f"Please write a corrected SQL that avoids this error."
        )

    user_msg = (
        f"Schema:\n{state['schema_context']}\n\n"
        f"Question: {state['question']}"
        f"{error_ctx}"
    )

    response = _llm().invoke([
        SystemMessage(content=SQL_SYSTEM_PROMPT),
        HumanMessage(content=user_msg),
    ])
    sql = _strip_sql_fences(response.content)
    log.info("Generated SQL:\n%s", sql)
    return {"generated_sql": sql, "sql_error": ""}


def execute_sql(state: AgentState) -> dict:
    """Run the SQL in BigQuery; on failure set sql_error and increment retry_count."""
    sql     = state["generated_sql"]
    client  = _bq()
    result, error = _run_query(client, sql)

    if error:
        retry = state.get("retry_count", 0) + 1
        log.warning("SQL execution failed (retry %d/%d): %s", retry, MAX_RETRIES, error[:200])
        return {"sql_error": error, "retry_count": retry, "query_result": ""}

    log.info("Query OK — %d chars of results", len(result))
    return {"query_result": result, "sql_error": "", "retry_count": state.get("retry_count", 0)}


def format_answer(state: AgentState) -> dict:
    """Ask Claude Haiku to turn the raw query result into a readable Polish answer."""
    error  = state.get("sql_error", "")
    result = state.get("query_result", "")

    if error:
        result_ctx = f"Zapytanie SQL nie powiodło się po {MAX_RETRIES} próbach. Ostatni błąd:\n{error}"
    elif not result:
        result_ctx = "Zapytanie zwróciło pusty wynik."
    else:
        result_ctx = result

    user_msg = (
        f"Pytanie użytkownika: {state['question']}\n\n"
        f"Wynik SQL:\n{result_ctx}"
    )

    log.info("Formatting answer...")
    response = _llm(temperature=0.2).invoke([
        SystemMessage(content=ANSWER_SYSTEM_PROMPT),
        HumanMessage(content=user_msg),
    ])
    answer = response.content.strip()
    return {"answer": answer}


# ── Routing ───────────────────────────────────────────────────────────────────

def _route_after_execute(state: AgentState) -> str:
    if state.get("sql_error") and state.get("retry_count", 0) < MAX_RETRIES:
        return "generate_sql"
    return "format_answer"


# ── Graph assembly ────────────────────────────────────────────────────────────

def build_graph() -> StateGraph:
    g = StateGraph(AgentState)

    g.add_node("load_schema",    load_schema)
    g.add_node("generate_sql",   generate_sql)
    g.add_node("execute_sql",    execute_sql)
    g.add_node("format_answer",  format_answer)

    g.add_edge(START,            "load_schema")
    g.add_edge("load_schema",    "generate_sql")
    g.add_edge("generate_sql",   "execute_sql")
    g.add_conditional_edges(
        "execute_sql",
        _route_after_execute,
        {
            "generate_sql":  "generate_sql",
            "format_answer": "format_answer",
        },
    )
    g.add_edge("format_answer",  END)

    return g


# ── Entry ─────────────────────────────────────────────────────────────────────

def main() -> None:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        log.error("ANTHROPIC_API_KEY not set — export it before running")
        sys.exit(1)

    # Accept question from CLI args or interactive prompt
    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
    else:
        try:
            question = input("Pytanie o dane UFC: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)

    if not question:
        log.error("Empty question — nothing to do.")
        sys.exit(1)

    log.info("=" * 60)
    log.info("text_to_sql_agent.py  —  UFC text-to-SQL (LangGraph)")
    log.info("Question: %s", question)
    log.info("=" * 60)

    initial_state: AgentState = {
        "question":       question,
        "schema_context": "",
        "generated_sql":  "",
        "sql_error":      "",
        "retry_count":    0,
        "query_result":   "",
        "answer":         "",
    }

    app    = build_graph().compile()
    result = app.invoke(initial_state)

    print("\n" + "─" * 60)
    print(f"Q: {question}")
    print("─" * 60)
    print(f"\n{result['answer']}\n")
    print("─" * 60)
    print(f"SQL:\n{result['generated_sql']}")
    print("─" * 60 + "\n")


if __name__ == "__main__":
    main()
