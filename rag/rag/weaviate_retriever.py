from langchain.schema.retriever import BaseRetriever
from typing import List, Dict
from weaviate_client import WeaviateClient
from langchain.schema import Document

class WeaviateRetriever(BaseRetriever):
    client: WeaviateClient
    k: int = 3
    neighbors: int = 1

    def _get_relevant_documents(self, query: str) -> List[Document]:
        raw = self.client.query(query, self.k, self.neighbors)
        parts = [p for p in raw.split("\n\n---\n\n") if p.strip()]
        return [Document(page_content=p) for p in parts]
