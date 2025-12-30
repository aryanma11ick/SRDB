from app.ingestion.gmail_client import GmailClient

client = GmailClient()
messages = client.fetch_messages_batch(days=1, max_results=5)

print(f"Fetched {len(messages)} messages")

for m in messages:
    print(m["id"], m["threadId"])