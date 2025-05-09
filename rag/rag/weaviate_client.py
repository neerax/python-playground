import os
import requests
from functools import lru_cache

WEAVIATE_URL = os.getenv("WEAVIATE_URL")
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")

if not WEAVIATE_URL:
    raise EnvironmentError("WEAVIATE_URL not set in environment variables")

HEADERS = {
    "Content-Type": "application/json",
    **({"X-API-KEY": WEAVIATE_API_KEY} if WEAVIATE_API_KEY else {})
}

@lru_cache(maxsize=1)
def ensure_schema():
    """
    Crea la classe DocumentChunk in Weaviate, se non esiste.
    """
    url = f"{WEAVIATE_URL.rstrip('/')}/v1/schema/classes/DocumentChunk"
    # controlla prima se esiste
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code == 404:
        payload = {
            "class": "DocumentChunk",
            "vectorizer": "text2vec-ollama",
            "moduleConfig": {
                "text2vec-ollama": {
                    "model": "nomic-embed-text",
                    "apiEndpoint": "http://host.docker.internal:11434"
                }
            },
            "properties": [
                {
                    "name": "text",
                    "dataType": ["text"]
                },
                {
                    "name": "chunk_id",
                    "dataType": ["int"]
                },
                {
                    "name": "source",
                    "dataType": ["string"]
                }
            ]
        }

        r = requests.post(f"{WEAVIATE_URL}/v1/schema/classes", json=payload, headers=HEADERS)
        r.raise_for_status()
    elif resp.status_code not in (200, 204):
        resp.raise_for_status()

def index_chunk(text: str, chunk_id: int, source: str, vector: list[float] | None = None):
    """
    Indicizza un singolo chunk in Weaviate.
    """
    obj = {
        "class": "DocumentChunk",
        "properties": {
            "text": text,
            "chunk_id": chunk_id,
            "source": source
        }
    }
    if vector is not None:
        obj["vector"] = vector

    r = requests.post(f"{WEAVIATE_URL}/v1/objects", json=obj, headers=HEADERS)
    r.raise_for_status()

def query_with_neighbors(query: str, k: int = 1, neighbors: int = 1, vector: list[float] | None = None):
    """
    Esegue una GraphQL nearVector (o nearText se vector=None) + recupera chunk adiacenti.
    """
    # 1) ricerca principale
    if vector is not None:
        filter_block = f'nearVector: {{vector: {vector}}}'
    else:
        # usa nearText, se preferisci
        filter_block = f'nearText: {{concepts: ["{query}"]}}'

    graphql = f"""
    {{
      Get {{
        DocumentChunk(
          {filter_block}
          limit: {k}
        ) {{
          chunk_id
          text
        }}
      }}
    }}
    """

    resp = requests.post(f"{WEAVIATE_URL}/v1/graphql", json={"query": graphql}, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()["data"]["Get"]["DocumentChunk"]

    # 2) recupera vicini
    all_texts = []
    for item in data:
        cid = item["chunk_id"]
        for nid in range(cid - neighbors, cid + neighbors + 1):
            if nid < 0:
                continue
            where = {
                "path": ["chunk_id"],
                "operator": "Equal",
                "valueInt": nid
            }
            q = f"""
            {{
              Get {{
                DocumentChunk(where: {where}) {{
                  text
                  chunk_id
                }}
              }}
            }}
            """
            r2 = requests.post(f"{WEAVIATE_URL}/v1/graphql", json={"query": q}, headers=HEADERS)
            r2.raise_for_status()
            hits = r2.json()["data"]["Get"]["DocumentChunk"]
            for h in hits:
                all_texts.append(h["text"])
    return "\n\n---\n\n".join(all_texts)
