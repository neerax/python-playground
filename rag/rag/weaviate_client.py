import os
import requests
from functools import lru_cache
from dotenv import load_dotenv
from graphql_query import Operation, Query, Field, Argument, Variable
from langchain.text_splitter import RecursiveCharacterTextSplitter

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


    def query(self, query: str, k: int = 1):

        graphql = self.create_query_with_neighbors(query, k)

        print(graphql)

        resp = self.api_post("graphql", {"query": graphql} )

        return resp


    def query_with_neighbors(query: str, k: int = 1, neighbors: int = 1, vector: list[float] | None = None):

        graphql = self.create_query_with_neighbors(query, k, neighbors)

        print(graphql)

        resp = requests.post(f"{WEAVIATE_URL}/v1/graphql", json={"query": graphql}, headers=HEADERS)
        resp.raise_for_status()

        data = resp.json()
        return data

        data = resp.json()["data"]["Get"]["DocumentChunk"]

        # # 2) recupera vicini
        # all_texts = []
        # for item in data:
        #     cid = item["chunk_id"]
        #     for nid in range(cid - neighbors, cid + neighbors + 1):
        #         if nid < 0:
        #             continue
        #         where = {
        #             "path": ["chunk_id"],
        #             "operator": "Equal",
        #             "valueInt": nid
        #         }
        #         q = f"""
        #         {{
        #           Get {{
        #             DocumentChunk(where: {where}) {{
        #               text
        #               chunk_id
        #             }}
        #           }}
        #         }}
        #         """
        #         r2 = requests.post(f"{WEAVIATE_URL}/v1/graphql", json={"query": q}, headers=HEADERS)
        #         r2.raise_for_status()
        #         hits = r2.json()["data"]["Get"]["DocumentChunk"]
        #         for h in hits:
        #             all_texts.append(h["text"])
        # return "\n\n---\n\n".join(all_texts)


    

    

    