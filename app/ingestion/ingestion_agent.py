from app.ingestion.gmail_client import GmailClient
from app.ingestion.email_parser import parse_gmail_message
from db.db import get_db_connection


class IngestionAgent:
    """
    Responsible for:
    - Fetching emails from Gmail
    - Parsing them
    - Deduplicating
    - Storing raw emails in Postgres
    """

    def __init__(self):
        self.client = GmailClient()

    def ingest(self, days: int = 7, max_results: int = 100) -> int:
        """
        Ingest emails newer than N days.
        Returns number of newly ingested emails.
        """

        raw_messages = self.client.fetch_messages_batch(
            days=days,
            max_results=max_results
        )

        if not raw_messages:
            return 0

        inserted_count = 0

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for msg in raw_messages:
                    parsed = parse_gmail_message(msg)

                    if self._insert(cur, parsed):
                        inserted_count += 1

            conn.commit()

        return inserted_count

    def _insert(self, cursor, email: dict) -> bool:
        """
        Insert parsed email into DB.
        Relies on the primary key constraint on email_id to deduplicate.
        """
        cursor.execute(
            """
            INSERT INTO emails (
                email_id,
                thread_id,
                sender,
                subject,
                body,
                received_at,
                processed
            )
            VALUES (%s, %s, %s, %s, %s, %s, FALSE)
            ON CONFLICT (email_id) DO NOTHING
            RETURNING email_id
            """,
            (
                email["email_id"],
                email["thread_id"],
                email["sender"],
                email["subject"],
                email["body"],
                email["received_at"],
            ),
        )
        return cursor.fetchone() is not None
