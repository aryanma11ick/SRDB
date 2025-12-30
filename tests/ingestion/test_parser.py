from app.ingestion.gmail_client import GmailClient
from app.ingestion.email_parser import parse_gmail_message

client = GmailClient()
messages = client.fetch_messages_batch(days=1, max_results=1)

parsed = parse_gmail_message(messages[0])
print(parsed)