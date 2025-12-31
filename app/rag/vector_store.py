import os
from typing import Iterable
from db.db import get_db_connection
from app.rag.embedding import embed_text


def _to_pgvector_literal(vector: Iterable[float]) -> str:
    """Convert a Python iterable of floats into a pgvector literal string."""
    return "[" + ",".join(f"{v:.6f}" for v in vector) + "]"


def _ensure_dispute(cursor, email: dict) -> int:
    """
    Get or reuse a canonical_dispute for the given email.
    If the email is already linked in dispute_emails, reuse that dispute_id.
    """
    cursor.execute(
        """
        SELECT de.dispute_id
        FROM dispute_emails de
        WHERE de.email_id = %s
        """,
        (email["email_id"],),
    )
    row = cursor.fetchone()
    if row:
        return row["dispute_id"]

    cursor.execute(
        """
        INSERT INTO canonical_disputes (supplier_id, dispute_summary)
        VALUES (%s, %s)
        RETURNING dispute_id
        """,
        (email.get("supplier_id"), email.get("subject")),
    )
    dispute_id = cursor.fetchone()["dispute_id"]

    cursor.execute(
        """
        INSERT INTO dispute_emails (dispute_id, email_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """,
        (dispute_id, email["email_id"]),
    )
    return dispute_id


def store_dispute_document(email: dict):
    """
    Persist an email into the dispute_documents/dispute_embeddings tables,
    keyed by canonical_dispute and supplier.
    """
    text = f"""
Subject: {email['subject']}
Sender: {email['sender']}
Date: {email['received_at']}

Email Content:
{email['body']}
""".strip()

    model_name = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            dispute_id = _ensure_dispute(cur, email)

            embedding = embed_text(text)
            embedding_literal = _to_pgvector_literal(embedding)

            cur.execute(
                """
                INSERT INTO dispute_documents (dispute_id, supplier_id, document_text)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (
                    dispute_id,
                    email.get("supplier_id"),
                    text,
                )
            )

            cur.execute(
                """
                INSERT INTO dispute_embeddings (dispute_id, supplier_id, embedding, model_name)
                VALUES (%s, %s, %s::vector, %s)
                """,
                (
                    dispute_id,
                    email.get("supplier_id"),
                    embedding_literal,
                    model_name,
                )
            )

        conn.commit()
