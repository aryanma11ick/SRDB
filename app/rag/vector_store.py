import os
import logging
from typing import Iterable
from db.db import get_db_connection
from app.rag.embedding import embed_text


logger = logging.getLogger(__name__)


def _to_pgvector_literal(vector: Iterable[float]) -> str:
    """Convert a Python iterable of floats into a pgvector literal string."""
    return "[" + ",".join(f"{v:.6f}" for v in vector) + "]"


def _similarity_threshold() -> float:
    raw = os.getenv("DISPUTE_SIMILARITY_THRESHOLD")
    try:
        return float(raw) if raw is not None else 0.82
    except ValueError:
        return 0.82


def _document_exists(cursor, dispute_id: int, text: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM dispute_documents
        WHERE dispute_id = %s
          AND document_text = %s
        LIMIT 1
        """,
        (dispute_id, text),
    )
    return cursor.fetchone() is not None


def _find_dispute_by_text(cursor, supplier_id: int, text: str):
    cursor.execute(
        """
        SELECT dispute_id
        FROM dispute_documents
        WHERE supplier_id = %s
          AND document_text = %s
        LIMIT 1
        """,
        (supplier_id, text),
    )
    return cursor.fetchone()


def _default_supplier_id(cursor) -> int:
    cursor.execute(
        """
        SELECT supplier_id
        FROM suppliers
        WHERE name = %s
        ORDER BY supplier_id ASC
        LIMIT 1
        """,
        ("Unknown Supplier",),
    )
    row = cursor.fetchone()
    if row:
        return row["supplier_id"]

    # Seed a default supplier row if it does not exist.
    cursor.execute(
        """
        INSERT INTO suppliers (name)
        VALUES (%s)
        RETURNING supplier_id
        """,
        ("Unknown Supplier",),
    )
    inserted = cursor.fetchone()
    if inserted:
        return inserted["supplier_id"]

    raise ValueError("Default supplier 'Unknown Supplier' missing and could not be created.")


def _resolve_supplier_id(cursor, supplier_id: int) -> int:
    if supplier_id is not None:
        return supplier_id
    resolved = _default_supplier_id(cursor)
    logger.warning(
        "Missing supplier_id on email; defaulting to 'unknown' supplier",
        extra={"resolved_supplier_id": resolved},
    )
    return resolved


def _link_email_to_dispute(cursor, dispute_id: int, email_id: str):
    cursor.execute(
        """
        INSERT INTO dispute_emails (dispute_id, email_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """,
        (dispute_id, email_id),
    )


def _merge_duplicate_dispute(cursor, source_dispute_id: int, target_dispute_id: int):
    """
    Move all dispute artifacts from a duplicate dispute into the target,
    deduping identical documents and preferring the most descriptive summary.
    """
    # Preserve the richer summary if present on the duplicate.
    cursor.execute(
        "SELECT dispute_summary FROM canonical_disputes WHERE dispute_id = %s",
        (target_dispute_id,),
    )
    target_summary_row = cursor.fetchone()
    target_summary = target_summary_row["dispute_summary"] if target_summary_row else None

    cursor.execute(
        "SELECT dispute_summary FROM canonical_disputes WHERE dispute_id = %s",
        (source_dispute_id,),
    )
    source_summary_row = cursor.fetchone()
    source_summary = source_summary_row["dispute_summary"] if source_summary_row else None

    if (not target_summary or (source_summary and len(source_summary) > len(target_summary))):
        cursor.execute(
            "UPDATE canonical_disputes SET dispute_summary = %s WHERE dispute_id = %s",
            (source_summary, target_dispute_id),
        )
        logger.info(
            "Canonical summary updated during merge",
            extra={"target_dispute_id": target_dispute_id, "source_dispute_id": source_dispute_id},
        )

    cursor.execute(
        "UPDATE dispute_emails SET dispute_id = %s WHERE dispute_id = %s",
        (target_dispute_id, source_dispute_id),
    )

    cursor.execute(
        "SELECT id, document_text FROM dispute_documents WHERE dispute_id = %s",
        (source_dispute_id,),
    )
    source_docs = cursor.fetchall() or []
    for doc in source_docs:
        if _document_exists(cursor, target_dispute_id, doc["document_text"]):
            cursor.execute("DELETE FROM dispute_documents WHERE id = %s", (doc["id"],))
        else:
            cursor.execute(
                "UPDATE dispute_documents SET dispute_id = %s WHERE id = %s",
                (target_dispute_id, doc["id"]),
            )

    cursor.execute(
        "UPDATE dispute_embeddings SET dispute_id = %s WHERE dispute_id = %s",
        (target_dispute_id, source_dispute_id),
    )
    cursor.execute(
        "DELETE FROM canonical_disputes WHERE dispute_id = %s",
        (source_dispute_id,),
    )
    logger.warning(
        "Merged duplicate canonical dispute",
        extra={"kept_dispute_id": target_dispute_id, "merged_dispute_id": source_dispute_id},
    )


def _merge_similar_disputes(cursor, dispute_id: int, supplier_id: int, embedding_literal: str, similarity_threshold: float):
    """
    Merge other disputes for this supplier that are highly similar to the new embedding.
    """
    cursor.execute(
        """
        SELECT dispute_id, 1 - (embedding <=> %s) AS similarity
        FROM dispute_embeddings
        WHERE supplier_id = %s
          AND dispute_id <> %s
        ORDER BY embedding <=> %s
        LIMIT 5
        """,
        (embedding_literal, supplier_id, dispute_id, embedding_literal),
    )
    rows = cursor.fetchall() or []
    for row in rows:
        if row.get("similarity") is None or row["similarity"] < similarity_threshold:
            logger.info(
                "Similarity below threshold; not merging disputes",
                extra={
                    "candidate_dispute_id": row.get("dispute_id"),
                    "target_dispute_id": dispute_id,
                    "similarity": row.get("similarity"),
                    "threshold": similarity_threshold,
                },
            )
            continue
        _merge_duplicate_dispute(cursor, row["dispute_id"], dispute_id)
        logger.info(
            "Merged similar dispute based on RAG similarity",
            extra={
                "merged_dispute_id": row["dispute_id"],
                "target_dispute_id": dispute_id,
                "similarity": row["similarity"],
                "threshold": similarity_threshold,
            },
        )


def _update_dispute_summary(cursor, dispute_id: int, email: dict):
    """
    Keep the canonical summary current with the latest meaningful subject/body snippet.
    """
    candidate = (email.get("subject") or "") or (email.get("body") or "")
    candidate = candidate.strip()
    candidate = candidate[:280]
    if not candidate:
        return

    cursor.execute(
        "SELECT dispute_summary FROM canonical_disputes WHERE dispute_id = %s",
        (dispute_id,),
    )
    row = cursor.fetchone()
    current = row["dispute_summary"] if row else None

    if not current:
        cursor.execute(
            "UPDATE canonical_disputes SET dispute_summary = %s WHERE dispute_id = %s",
            (candidate, dispute_id),
        )
    elif candidate.lower() not in current.lower() and len(candidate) >= len(current):
        cursor.execute(
            "UPDATE canonical_disputes SET dispute_summary = %s WHERE dispute_id = %s",
            (candidate, dispute_id),
        )


def _ensure_dispute(cursor, email: dict, embedding_literal: str, similarity_threshold: float) -> int:
    """
    Get or reuse a canonical_dispute for the given email.
    If the email is already linked in dispute_emails, reuse that dispute_id.
    Otherwise, attempt to find a similar dispute for the same supplier before
    creating a new canonical record.
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

    supplier_id = email.get("supplier_id")
    if supplier_id is None:
        raise ValueError("Email missing supplier_id; required for dispute canonicalization.")

    cursor.execute(
        """
        SELECT dispute_id, 1 - (embedding <=> %s) AS similarity
        FROM dispute_embeddings
        WHERE supplier_id = %s
        ORDER BY embedding <=> %s
        LIMIT 1
        """,
        (embedding_literal, supplier_id, embedding_literal),
    )
    match = cursor.fetchone()
    if match and match.get("similarity") is not None and match["similarity"] >= similarity_threshold:
        logger.info(
            "Reusing dispute based on similarity",
            extra={
                "reused_dispute_id": match["dispute_id"],
                "similarity": match["similarity"],
                "threshold": similarity_threshold,
                "supplier_id": supplier_id,
            },
        )
        dispute_id = match["dispute_id"]
    else:
        logger.warning(
            "Creating new canonical dispute after RAG check",
            extra={
                "supplier_id": supplier_id,
                "top_similarity": match["similarity"] if match and match.get("similarity") is not None else None,
                "threshold": similarity_threshold,
            },
        )
        cursor.execute(
            """
            INSERT INTO canonical_disputes (supplier_id, dispute_summary)
            VALUES (%s, %s)
            RETURNING dispute_id
            """,
            (supplier_id, email.get("subject")),
        )
        dispute_id = cursor.fetchone()["dispute_id"]

    _link_email_to_dispute(cursor, dispute_id, email["email_id"])
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
    similarity_threshold = _similarity_threshold()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            supplier_id = _resolve_supplier_id(cur, email.get("supplier_id"))
            if email.get("supplier_id") is None:
                email["supplier_id"] = supplier_id
                cur.execute(
                    "UPDATE emails SET supplier_id = %s WHERE email_id = %s",
                    (supplier_id, email["email_id"]),
                )

            # Fast-path: identical text already stored for this supplier; avoid re-embedding.
            existing = _find_dispute_by_text(cur, supplier_id, text)
            if existing:
                dispute_id = existing["dispute_id"]
                _link_email_to_dispute(cur, dispute_id, email["email_id"])
                _update_dispute_summary(cur, dispute_id, email)
                logger.info(
                    "Skipped embedding duplicate dispute document",
                    extra={
                        "dispute_id": dispute_id,
                        "email_id": email["email_id"],
                        "supplier_id": supplier_id,
                    },
                )
                conn.commit()
                return

            embedding = embed_text(text)
            embedding_literal = _to_pgvector_literal(embedding)

            dispute_id = _ensure_dispute(cur, email, embedding_literal, similarity_threshold)
            _merge_similar_disputes(cur, dispute_id, supplier_id, embedding_literal, similarity_threshold)

            if not _document_exists(cur, dispute_id, text):
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

            _update_dispute_summary(cur, dispute_id, email)

            # Lightweight monitoring hook: track canonical dispute volume per supplier.
            cur.execute(
                "SELECT COUNT(*) AS total FROM canonical_disputes WHERE supplier_id = %s",
                (supplier_id,),
            )
            row = cur.fetchone()
            logger.info(
                "Canonical dispute count by supplier",
                extra={"supplier_id": supplier_id, "count": row["total"] if row else None},
            )

        conn.commit()
