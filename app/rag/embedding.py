import os
import sys
import db.env
from pathlib import Path
from typing import List
from openai import OpenAI

API_KEY = os.getenv("OPENAI_API_KEY")
if not API_KEY:
    raise ValueError("Missing OPENAI_API_KEY; embeddings require LLM access.")

MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")  # cheap & good
client = OpenAI(api_key=API_KEY)


def embed_text(text: str) -> List[float]:
    """
    Generate an embedding for the provided text.
    Raises a clear error if the OpenAI API returns no data.
    """
    response = client.embeddings.create(
        model=MODEL,
        input=text,
        timeout=15,
    )
    if not response.data or not response.data[0].embedding:
        raise ValueError("Embedding API returned no data; cannot store vector.")
    return response.data[0].embedding
