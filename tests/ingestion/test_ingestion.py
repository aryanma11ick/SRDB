from app.ingestion.ingestion_agent import IngestionAgent

def test_ingestion_agent():
    agent = IngestionAgent()
    count = agent.ingest(days=7, max_results=5)
    print(f"Ingested {count} new emails")


if __name__ == "__main__":
    # Allow running directly via `python tests/ingestion/test_ingestion.py`
    test_ingestion_agent()
