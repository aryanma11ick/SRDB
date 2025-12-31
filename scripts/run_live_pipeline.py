"""
Live pipeline runner:
- Fetches recent Gmail messages via the Gmail API (using IngestionAgent)
- Classifies unprocessed emails via OpenAI
- Stores dispute-classified emails into the pgvector store
- Retrieves similar disputes for a sample query

Requires:
- credentials.json / token.json for Gmail API auth
- OPENAI_API_KEY for classification + embeddings
- Postgres with the expected schema (emails table must include label, confidence, classification_reason)
"""

import os
from typing import List
from psycopg2.extras import RealDictCursor

from db.db import get_db_connection
from app.ingestion.ingestion_agent import IngestionAgent
from app.classification.classification_agent import ClassificationAgent
from app.rag.vector_store import store_dispute_document
from app.rag.retrieval_agent import RetrievalAgent


def _ensure_classification_columns(conn):
    """Verify the emails table has the columns used by the classifier."""
    required = {"label", "confidence", "classification_reason"}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'emails'
            """
        )
        cols = {row["column_name"] for row in cur.fetchall()}
    missing = required - cols
    if missing:
        raise RuntimeError(
            f"emails table missing required columns for classification: {sorted(missing)}"
        )


def _fetch_recent_disputes(conn, days: int, limit: int) -> List[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT email_id, thread_id, sender, subject, body, received_at
            FROM emails
            WHERE processed = TRUE
              AND label = 'dispute'
              AND received_at >= NOW() - (%s || ' days')::interval
            ORDER BY received_at DESC
            LIMIT %s
            """,
            (days, limit),
        )
        return cur.fetchall()


def main():
    days = int(os.getenv("PIPELINE_DAYS", "3"))
    max_results = int(os.getenv("PIPELINE_MAX_RESULTS", "5"))
    top_k = int(os.getenv("PIPELINE_TOP_K", "3"))

    print(f"[pipeline] ingesting Gmail messages newer than {days}d (max {max_results})")
    ingested = IngestionAgent().ingest(days=days, max_results=max_results)
    print(f"[pipeline] ingested {ingested} new emails")

    print("[pipeline] classifying unprocessed emails")
    classified = ClassificationAgent().classify_pending_emails(limit=max_results)
    print(f"[pipeline] classified {classified} emails")

    with get_db_connection() as conn:
        _ensure_classification_columns(conn)
        disputes = _fetch_recent_disputes(conn, days=days, limit=max_results)
        print(f"[pipeline] storing {len(disputes)} dispute emails into vector store")
        for row in disputes:
            store_dispute_document(row)
        conn.commit()

    print("[pipeline] retrieving similar disputes for sample query: 'payment discrepancy'")
    results = RetrievalAgent().retrieve_similar_disputes(
        query_text="payment discrepancy",
        top_k=top_k,
    )
    if not results:
        print("[pipeline] no results returned (do you have any dispute embeddings stored?)")
    else:
        print(f"[pipeline] retrieved {len(results)} results")
        for r in results:
            dispute_id = r.get("dispute_id") if isinstance(r, dict) else r[0]
            sim = r.get("similarity") if isinstance(r, dict) else r[3]
            print(f"  - dispute_id={dispute_id}, similarity={sim}")


if __name__ == "__main__":
    main()
