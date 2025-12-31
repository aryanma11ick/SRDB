import os
from unittest.mock import patch
from psycopg2.extras import RealDictCursor

from db.db import get_db_connection
from app.classification.classification_agent import ClassificationAgent
from app.rag.vector_store import store_dispute_document
from app.rag.retrieval_agent import RetrievalAgent


def _reset_tables():
    """Clean up any prior pipeline test rows."""
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("DELETE FROM dispute_embeddings WHERE dispute_id IN (SELECT dispute_id FROM dispute_emails WHERE email_id IN (%s, %s))", ("pipeline1", "pipeline2"))
            cur.execute("DELETE FROM dispute_documents WHERE dispute_id IN (SELECT dispute_id FROM dispute_emails WHERE email_id IN (%s, %s))", ("pipeline1", "pipeline2"))
            cur.execute("DELETE FROM dispute_emails WHERE email_id IN (%s, %s)", ("pipeline1", "pipeline2"))
            cur.execute("DELETE FROM emails WHERE email_id IN (%s, %s)", ("pipeline1", "pipeline2"))
            cur.execute("DELETE FROM canonical_disputes WHERE dispute_id NOT IN (SELECT dispute_id FROM dispute_emails)")
        conn.commit()


def _default_supplier_id():
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT supplier_id FROM suppliers WHERE name = %s", ("Unknown Supplier",))
            row = cur.fetchone()
            if row:
                return row["supplier_id"]
            cur.execute(
                "INSERT INTO suppliers (name) VALUES (%s) ON CONFLICT DO NOTHING RETURNING supplier_id",
                ("Unknown Supplier",),
            )
            inserted = cur.fetchone()
            if inserted:
                return inserted["supplier_id"]
            cur.execute("SELECT supplier_id FROM suppliers WHERE name = %s", ("Unknown Supplier",))
            return cur.fetchone()["supplier_id"]


def _seed_emails():
    supplier_id = _default_supplier_id()
    sample_rows = [
        {
            "email_id": "pipeline1",
            "thread_id": "t1",
            "sender": "ap@supplier.com",
            "subject": "Short payment on INV-100",
            "body": "Payment received short by 5,000. Please advise.",
            "supplier_id": supplier_id,
        },
        {
            "email_id": "pipeline2",
            "thread_id": "t2",
            "sender": "ap@supplier.com",
            "subject": "Invoice status",
            "body": "Following up on invoice INV-200 status.",
            "supplier_id": supplier_id,
        },
    ]

    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for row in sample_rows:
                cur.execute(
                    """
                    INSERT INTO emails (
                        email_id, thread_id, sender, subject, body, received_at, processed, supplier_id
                    )
                    VALUES (%s, %s, %s, %s, %s, NOW(), FALSE, %s)
                    ON CONFLICT (email_id) DO NOTHING
                    """,
                    (
                        row["email_id"],
                        row["thread_id"],
                        row["sender"],
                        row["subject"],
                        row["body"],
                        row["supplier_id"],
                    ),
                )
        conn.commit()


def test_end_to_end_classification_and_retrieval():
    """
    Integration test of classification -> vector store -> retrieval using local stubs
    to avoid external network calls.
    """
    # Ensure the key is present to satisfy the ClassificationAgent constructor.
    os.environ.setdefault("OPENAI_API_KEY", "test-key")

    _reset_tables()
    _seed_emails()

    # Stub LLM and embedding calls to avoid network.
    classify_side_effect = [
        {"label": "dispute", "confidence": 0.9, "reason": "Short payment mentioned."},
        {"label": "ambiguous", "confidence": 0.6, "reason": "Status follow-up."},
    ]
    fake_embedding = [0.1, 0.2, 0.3]

    with patch.object(ClassificationAgent, "_classify_text", side_effect=classify_side_effect), \
         patch("app.rag.vector_store.embed_text", return_value=fake_embedding), \
         patch("app.rag.retrieval_agent.embed_text", return_value=fake_embedding):

        agent = ClassificationAgent()
        classified_count = agent.classify_pending_emails(limit=5)
        assert classified_count == 2

        # Push the now-processed emails into the vector store.
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT email_id, thread_id, sender, subject, body, received_at, supplier_id FROM emails WHERE email_id IN (%s, %s)",
                    ("pipeline1", "pipeline2"),
                )
                for row in cur.fetchall():
                    store_dispute_document(row)
            conn.commit()

        # Retrieve similar disputes using the same fake embedding.
        retrieval = RetrievalAgent()
        results = retrieval.retrieve_similar_disputes("short payment", top_k=2)

        assert len(results) == 2
        assert all("similarity" in row for row in results)

    # Cleanup
    _reset_tables()
