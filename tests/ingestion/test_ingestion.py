from app.ingestion.ingestion_agent import IngestionAgent

def test_ingestion_agent():
    agent = IngestionAgent()
    count = agent.ingest(days=1, max_results=5)
    print(f"Ingested {count} new emails")