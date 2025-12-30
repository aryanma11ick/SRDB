from typing import Iterable
from db.db import get_db_connection
from app.rag.embedding import embed_text


def _to_pgvector_literal(vector: Iterable[float]) -> str:
    """Convert a Python iterable of floats into a pgvector literal string."""
    return "[" + ",".join(f"{v:.6f}" for v in vector) + "]"


def store_dispute_document(email: dict):
    """
    Persist an email into the dispute_documents vector store, with
    lightweight deduplication by email_id.
    """
    text = f"""
Subject: {email['subject']}
Sender: {email['sender']}
Date: {email['received_at']}

Email Content:
{email['body']}
""".strip()

    with get_db_connection() as conn:
        with conn.cursor() as cur:

            # Deduplication guard
            cur.execute(
                "SELECT 1 FROM dispute_documents WHERE email_id = %s",
                (email["email_id"],)
            )
            if cur.fetchone():
                return

            embedding = embed_text(text)
            embedding_literal = _to_pgvector_literal(embedding)

            cur.execute(
                """
                INSERT INTO dispute_documents (
                    email_id, thread_id, document_text, embedding
                )
                VALUES (%s, %s, %s, %s::vector)
                """,
                (
                    email["email_id"],
                    email["thread_id"],
                    text,
                    embedding_literal,
                )
            )

        conn.commit()
