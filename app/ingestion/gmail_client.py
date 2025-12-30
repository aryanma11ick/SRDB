from typing import List, Dict, Optional
from googleapiclient.discovery import Resource
from .gmail_auth import get_gmail_service


class GmailClient:
    """
    Thin wrapper around Gmail API.
    Responsible ONLY for fetching messages.
    """

    def __init__(self, service: Optional[Resource] = None):
        self.service = service or get_gmail_service()

    def list_message_ids(
        self,
        days: int = 7,
        max_results: int = 100
    ) -> List[Dict[str, str]]:
        """
        Fetch message IDs from inbox newer than N days.

        Returns:
            List of dicts: [{ "id": "...", "threadId": "..." }]
        """

        query = f"newer_than:{days}d"

        response = self.service.users().messages().list(
            userId="me",
            q=query,
            maxResults=max_results
        ).execute()

        return response.get("messages", [])

    def fetch_message(self, message_id: str) -> Dict:
        """
        Fetch full message payload by message ID.
        """

        return self.service.users().messages().get(
            userId="me",
            id=message_id,
            format="full"
        ).execute()

    def fetch_messages_batch(
        self,
        days: int = 7,
        max_results: int = 100
    ) -> List[Dict]:
        """
        Convenience method:
        - Lists message IDs
        - Fetches full messages

        Returns:
            List of full Gmail message objects
        """

        messages = []
        message_ids = self.list_message_ids(
            days=days,
            max_results=max_results
        )

        for msg in message_ids:
            full_message = self.fetch_message(msg["id"])
            messages.append(full_message)

        return messages
