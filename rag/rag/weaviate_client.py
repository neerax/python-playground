import os
import requests
from functools import lru_cache
from dotenv import load_dotenv
from graphql_query import Operation, Query, Field, Argument, Variable
from langchain.text_splitter import RecursiveCharacterTextSplitter
import json
from langchain.schema import Document
from typing import List
from langchain.schema.retriever import BaseRetriever



@lru_cache(maxsize=1)
def get_splitter(chunk_size=500, chunk_overlap=100):
    return RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        separators=["\n\n", "\n", ".", "!", "?", " ", ""]
    )

def split_text_with_langchain(text: str, chunk_size: int = 1000, chunk_overlap: int = 200) -> list[str]:
    splitter = get_splitter(chunk_size, chunk_overlap)
    return splitter.split_text(text)

class WeaviateClient:
    def __init__(self, url, weaviate_api_key):
        self.url = url.rstrip('/')+'/'
        self.headers = {
            "Content-Type": "application/json",
            **({"Authorization": "Bearer "+weaviate_api_key} if weaviate_api_key else {})
        }

    def api_build_url(self, name, additional=None, version="v1"):
        url = self.url+version+'/'+name
        if additional:
            url += "/" + additional
        return url

    def api_get(self, name, additional="", version="v1"):
        url = self.api_build_url(name, additional, version)
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        return resp.json()

    def api_post(self, name, body, additional="", version="v1"):
        url = self.api_build_url(name, additional, version)
        resp = requests.post(url, json=body, headers=self.headers)
        resp.raise_for_status()
        return resp.json()

    def api_delete(self, name, additional="", version="v1"):
        url = self.api_build_url(name, additional, version)
        resp = requests.delete(url, headers=self.headers)
        resp.raise_for_status()
        return

    def api_delete_with_json(self, name, additional="", body=None, version="v1"):
        url = self.api_build_url(name, additional, version)
        resp = requests.delete(url, json=body, headers=self.headers)
        resp.raise_for_status()
        return resp.json()

    def get_schema(self):
        return self.api_get("schema")

    def get_class(self, class_name):
        return self.api_get("schema", class_name)

    def create_class(self, definition):
        return self.api_post( "schema", definition)

    def delete_class(self, class_name):
        return self.api_delete("schema", class_name)

    def ingest_text(self, text, source):
        chunks = split_text_with_langchain(text)
        n = len(chunks)
        for i, chunk in enumerate(chunks):
            print("Processing CHUNK", i+1, "of", n)
            self.ingest_chunk(chunk, i, source)
         
    def ingest_chunk(self, text: str, chunk_id: int, source: str):
        obj = {
            "class": "DocumentChunk",
            "properties": {
                "text": text,
                "chunk_id": chunk_id,
                "source": source
            }
        }

        return self.api_post("objects", obj)

    def get_document_chunk(self, source: str):
        op = Operation(
            type="query",
            queries=[
                Query(
                    name="Get",
                    fields=[
                        Field(
                            name="DocumentChunk",
                            fields=["chunk_id",
                                Field(name="_additional", fields=["id"])
                            ]
                        )
                    ]
                )
            ]
        )

        graphql = op.render()

        resp = self.api_post("graphql", {"query": graphql})

        return resp
    
    def delete_document_chunks_by_source(self, source: str):

        payload = {
            "match": {
              "class": "DocumentChunk",
              "where": {
                "path": ["source"],
                "operator": "Equal",
                "valueString": source
              }
            },
            "output": "verbose",
            "dryRun": False
        }

        resp = self.api_delete_with_json("batch", "objects", payload)

        return resp

    def create_query_with_neighbors(self,
        query: str,
        k: int = 1,
        neighbors: int = 1,
        vector: list[float] | None = None
    ):
        # build the nearText / nearVector argument
        if vector is not None:
            filter_arg = Argument(
                name="nearVector",
                value=[Argument(name="vector", value=vector)]
            )
        else:
            filter_arg = Argument(
                name="nearText",
                value=[Argument(name="concepts", value=[query])]
            )

        limit_arg = Argument(name="limit", value=k)

        document_chunk_field = Field(
            name="DocumentChunk",
            arguments=[filter_arg, limit_arg],
            fields=["chunk_id", "text", Field(name="_additional", fields=["certainty", "distance","score"])]
        )

        op = Operation(
            type="query",
            queries=[
                Query(
                    name="Get",
                    fields=[document_chunk_field]
                )
            ]
            )

        return op.render()

    def query(self, text: str, k: int = 1, neighbors: int = 1):
        # ——— Prima query: nearText / nearVector
        initial_gql = self.create_query_with_neighbors(text, k, neighbors)
        initial_resp = self.api_post("graphql", {"query": initial_gql})
        hits = initial_resp.get("data", {}).get("Get", {}).get("DocumentChunk", [])
        if not hits:
            return ""

        # Debug 1: chunk_id principali trovati
        main_ids = [h["chunk_id"] for h in hits]
        print("[DEBUG] Chunk IDs trovati:", main_ids)

        # ——— Calcolo IDs estesi (inclusi vicini)
        extended_ids = {
            n
            for cid in main_ids
            for n in range(cid - neighbors, cid + neighbors + 1)
            if n >= 0
        }
        print("[DEBUG] Chunk IDs estesi:", sorted(extended_ids))

        # ——— Filtra solo i vicini che non erano in main_ids
        neighbor_ids = sorted(extended_ids.difference(main_ids))
        print("[DEBUG] Chunk IDs da recuperare (solo vicini):", neighbor_ids)

        neighbor_chunks = []
        if neighbor_ids:
            # costruiamo un singolo where con questi neighbor_ids
            where_arg = Argument(
                name="where",
                value=[
                    Argument(name="operator", value="Or"),
                    Argument(
                        name="operands",
                        value=[
                            [
                                Argument(name="path", value=["chunk_id"]),
                                Argument(name="operator", value="Equal"),
                                Argument(name="valueInt", value=cid),
                            ]
                            for cid in neighbor_ids
                        ]
                    )
                ]
            )

            doc_field = Field(
                name="DocumentChunk",
                arguments=[where_arg],
                fields=["chunk_id", "text"]
            )
            op = Operation(type="query", queries=[Query(name="Get", fields=[doc_field])])
            gql = op.render()
            resp = self.api_post("graphql", {"query": gql})
            neighbor_chunks = resp.get("data", {}).get("Get", {}).get("DocumentChunk", [])

        # ——— Combiniamo risultati originali + vicini
        all_chunks = hits + neighbor_chunks

        # ——— Ordiniamo per chunk_id
        sorted_chunks = sorted(all_chunks, key=lambda x: x["chunk_id"])

        # ——— Restituiamo testo concatenato
        return "\n\n---\n\n".join(c["text"] for c in sorted_chunks)



class WeaviateRetriever(BaseRetriever):
    client: WeaviateClient
    k: int = 3
    neighbors: int = 1

    def _get_relevant_documents(self, query: str) -> List[Document]:
        raw = self.client.query(query, self.k, self.neighbors)
        parts = [p for p in raw.split("\n\n---\n\n") if p.strip()]
        return [Document(page_content=p) for p in parts]
