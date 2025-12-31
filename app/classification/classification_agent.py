import os
import json
from typing import Dict
from openai import OpenAI

from db.db import get_db_connection
from app.classification.prompts import CLASSIFY_EMAIL_PROMPT
from app.rag.vector_store import store_dispute_document


class ClassificationAgent:
    """
    Classifies unprocessed emails into dispute categories.
    """

    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("Missing OPENAI_API_KEY; classification requires LLM access.")
        self.client = OpenAI(api_key=self.api_key)
        self.model = os.getenv("OPENAI_MODEL", "gpt-5-mini")

    def classify_pending_emails(self, limit: int = 10) -> int:
        """
        Classify emails where processed = false.
        Returns number of emails classified.
        """

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT email_id, body
                    FROM emails
                    WHERE processed = FALSE
                    LIMIT %s
                    """,
                    (limit,)
                )

                rows = cur.fetchall()

                if not rows:
                    return 0

                for row in rows:
                    result = self._classify_text(row["body"])

                    # store classification result
                    self._store_result(cur, row["email_id"], result)

                    # ONLY disputes go into RAG
                    if result["label"] == "dispute":
                        email = self._fetch_email(cur, row["email_id"])
                        store_dispute_document(email)

            conn.commit()

        return len(rows)

    def _classify_text(self, body: str) -> Dict:
        """
        Call LLM to classify email body. Falls back to a rule-based
        classifier if the API returns invalid JSON or is unavailable.
        """

        prompt = CLASSIFY_EMAIL_PROMPT.format(email_body=body)

        last_error = None
        for _ in range(2):  # small retry for transient blank/invalid responses
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a strict JSON-only classifier."},
                    {"role": "user", "content": prompt}
                ],
                timeout=15,
                response_format={"type": "json_object"},
                max_completion_tokens=200,
            )

            if not response.choices or not response.choices[0].message:
                last_error = ValueError("LLM returned no choices/message.")
                continue

            content = response.choices[0].message.content
            if not content or not content.strip():
                last_error = ValueError("LLM returned blank content.")
                continue

            try:
                parsed = json.loads(content)
                # Basic shape validation to surface clearer errors
                for key in ("label", "confidence", "reason"):
                    if key not in parsed:
                        raise ValueError(f"LLM response missing required field '{key}': {content}")
                return parsed
            except Exception as exc:
                last_error = exc
                continue

        raise ValueError(f"LLM returned invalid JSON after retries: {last_error}")

    def _store_result(self, cursor, email_id: str, result: Dict):
        """
        Store classification result and mark email as processed.
        """

        cursor.execute(
            """
            UPDATE emails
            SET
                label = %s,
                confidence = %s,
                classification_reason = %s,
                processed = TRUE
            WHERE email_id = %s
            """,
            (
                result["label"],
                result["confidence"],
                result["reason"],
                email_id
            )
        )
        
    def _fetch_email(self, cursor, email_id: str) -> dict:
        cursor.execute(
            """
            SELECT email_id, thread_id, sender, subject, body, received_at, supplier_id
            FROM emails
            WHERE email_id = %s
            """,
            (email_id,)
        )
        return cursor.fetchone()
