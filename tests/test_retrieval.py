from app.rag.retrieval_agent import RetrievalAgent

agent = RetrievalAgent()
results = agent.retrieve_similar_disputes(
    "payment", 
    top_k=3, 
    supplier_id=1  # Filter by supplier
)
print(results)