import os
from email.utils import parseaddr

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
        default_supplier_id = os.getenv("DEFAULT_SUPPLIER_ID")
        try:
            self.default_supplier_id = int(default_supplier_id) if default_supplier_id else None
        except ValueError:
            self.default_supplier_id = None

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
                    parsed["supplier_id"] = self._get_supplier_id(cur, parsed.get("sender"))

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
                processed,
                supplier_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, FALSE, %s)
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
                email.get("supplier_id"),
            ),
        )
        return cursor.fetchone() is not None

    def _get_supplier_id(self, cursor, sender: str):
        """
        Resolve supplier_id by sender domain. If no domain is found or
        the domain is unknown, fall back to a manually configured default
        supplier ID (set via DEFAULT_SUPPLIER_ID) or return None.
        """
        domain = self._extract_domain(sender)
        if not domain:
            return self.default_supplier_id

        cursor.execute("SELECT supplier_id FROM suppliers WHERE email_domain = %s", (domain,))
        row = cursor.fetchone()
        if row:
            return row["supplier_id"]

        return self.default_supplier_id

    @staticmethod
    def _extract_domain(sender: str):
        """
        Pull the domain from the sender header (e.g., user@example.com -> example.com)
        """
        if not sender:
            return None
        _, email = parseaddr(sender)
        if "@" not in email:
            return None
        return email.split("@", 1)[1].lower()
