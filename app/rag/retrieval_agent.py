from db.db import get_db_connection
from app.rag.embedding import embed_text
from app.rag.vector_store import _to_pgvector_literal

class RetrievalAgent:
    def retrieve_similar_disputes(
        self,
        query_text: str,
        top_k: int = 5
    ):
        embedding = embed_text(query_text)
        embedding_literal = _to_pgvector_literal(embedding)

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        email_id,
                        thread_id,
                        document_text,
                        1 - (embedding <=> %s) AS similarity
                    FROM dispute_documents
                    ORDER BY embedding <=> %s
                    LIMIT %s
                    """,
                    (embedding_literal, embedding_literal, top_k)
                )
                return cur.fetchall()
