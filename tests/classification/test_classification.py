from app.classification.classification_agent import ClassificationAgent

if __name__ == "__main__":
    agent = ClassificationAgent()
    count = agent.classify_pending_emails(limit=5)
    print(f"Classified {count} emails")
