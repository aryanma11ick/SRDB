import base64
from typing import Dict, Optional
from email.utils import parsedate_to_datetime
from bs4 import BeautifulSoup


def _get_header(headers, name: str) -> Optional[str]:
    """
    Extract a specific header value from Gmail headers.
    """
    for header in headers:
        if header.get("name", "").lower() == name.lower():
            return header.get("value")
    return None


def _decode_base64(data: str) -> str:
    """
    Safely decode base64-encoded Gmail payload.
    """
    try:
        return base64.urlsafe_b64decode(data).decode(
            "utf-8", errors="replace"
        )
    except Exception:
        return ""


def _extract_body(payload: Dict) -> str:
    """
    Extract email body from Gmail payload.

    Priority:
    1. text/plain
    2. text/html (converted to text)
    """

    if "parts" in payload:
        plain_text = None
        html_text = None

        for part in payload["parts"]:
            mime_type = part.get("mimeType")
            body_data = part.get("body", {}).get("data")

            if not body_data:
                continue

            decoded = _decode_base64(body_data)

            if mime_type == "text/plain":
                plain_text = decoded
            elif mime_type == "text/html":
                html_text = decoded

        if plain_text:
            return plain_text.strip()

        if html_text:
            soup = BeautifulSoup(html_text, "html.parser")
            return soup.get_text(separator="\n").strip()

    # Fallback for single-part emails
    body_data = payload.get("body", {}).get("data")
    if body_data:
        return _decode_base64(body_data).strip()

    return ""


def parse_gmail_message(message: Dict) -> Dict:
    """
    Convert a Gmail API message (format='full') into
    the internal normalized email schema.
    """

    payload = message.get("payload", {})
    headers = payload.get("headers", [])

    raw_date = _get_header(headers, "Date")
    sender = _get_header(headers, "From")
    subject = _get_header(headers, "Subject")

    try:
        received_at = (
            parsedate_to_datetime(raw_date)
            if raw_date
            else None
        )
    except Exception:
        received_at = None

    body = _extract_body(payload)

    return {
        "email_id": message.get("id"),
        "thread_id": message.get("threadId"),
        "sender": sender,
        "subject": subject,
        "body": body,
        "received_at": received_at,
    }
