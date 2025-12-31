from typing import Optional
from db.db import get_db_connection
from app.rag.embedding import embed_text
from app.rag.vector_store import _to_pgvector_literal


class RetrievalAgent:
    def retrieve_similar_disputes(
        self,
        query_text: str,
        top_k: int = 5,
        supplier_id: Optional[int] = None,
    ):
        embedding = embed_text(query_text)
        embedding_literal = _to_pgvector_literal(embedding)

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if supplier_id is not None:
                    cur.execute(
                        """
                        SELECT
                            e.dispute_id,
                            e.supplier_id,
                            d.document_text,
                            1 - (e.embedding <=> %s::vector) AS similarity
                        FROM dispute_embeddings e
                        JOIN LATERAL (
                            SELECT document_text
                            FROM dispute_documents d
                            WHERE d.dispute_id = e.dispute_id
                            ORDER BY d.created_at DESC
                            LIMIT 1
                        ) d ON TRUE
                        WHERE e.supplier_id = %s
                        ORDER BY e.embedding <=> %s::vector
                        LIMIT %s
                        """,
                        (embedding_literal, supplier_id, embedding_literal, top_k)
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                            e.dispute_id,
                            e.supplier_id,
                            d.document_text,
                            1 - (e.embedding <=> %s::vector) AS similarity
                        FROM dispute_embeddings e
                        JOIN LATERAL (
                            SELECT document_text
                            FROM dispute_documents d
                            WHERE d.dispute_id = e.dispute_id
                            ORDER BY d.created_at DESC
                            LIMIT 1
                        ) d ON TRUE
                        ORDER BY e.embedding <=> %s::vector
                        LIMIT %s
                        """,
                        (embedding_literal, embedding_literal, top_k)
                    )
                return cur.fetchall()
