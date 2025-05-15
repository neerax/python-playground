import requests

from dotenv import load_dotenv
from graphql_query import Operation, Query, Field, Argument, Variable
import json
from langchain.schema import Document
from typing import List, Dict
from langchain.schema.retriever import BaseRetriever
from requests.exceptions import HTTPError
from urllib.parse import urlencode


class WeaviateClient:
    def __init__(self, url, weaviate_api_key):
        self.url = url.rstrip('/')+'/'
        self.headers = {
            "Content-Type": "application/json",
            **({"Authorization": "Bearer "+weaviate_api_key} if weaviate_api_key else {})
        }

    def api_build_url(self, name, additional=None, version="v1", params = None):
        url = self.url+version+'/'+name
        if additional:
            url += "/" + additional
        if params:
            query_string = urlencode(params)
            url += '?' + query_string
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

    def api_put(self, name, body, additional="", version="v1"):
        url = self.api_build_url(name, additional, version)
        resp = requests.put(url, json=body, headers=self.headers)
        resp.raise_for_status()
        return resp.json()

    def api_delete(self, name, additional="", version="v1"):
        url = self.api_build_url(name, additional, version)
        resp = requests.delete(url, headers=self.headers)
        resp.raise_for_status()
        return
    
    def api_patch(self, name, body, additional="", version="v1", params = None):
        url = self.api_build_url(name, additional, version, params)
        resp = requests.patch(url, json=body, headers = self.headers)
        resp.raise_for_status()
        return resp

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
    
    def apply_schema(self, definitions: Dict[str, object]) -> Dict[str, object]:
        
        created_results: Dict[str, object] = {}
        skipped_class: list[str] = []
        failed_class: list[str] = []

        for class_name, definition in definitions.items():
            try:
                self.get_class(class_name)
                skipped_class.append(class_name)
            
            except HTTPError as e:
                if e.response.status_code != 404:
                    raise
                else:
                    try:
                        created = self.create_class(definition)
                        created_results[class_name] = created
                    except HTTPError as e:
                        print(e)
                        failed_class.append(class_name)

        return (created_results, skipped_class, failed_class)    


    

    def ingest(self, class_name, **kwargs):
        payload = {
            "class": class_name,
            "properties": kwargs
        }
        return self.api_post("objects", payload)    

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

    def get_objects(self, class_name: str, fields = []):
        fields.append(Field(name="_additional", fields=["id"]))
        op = Operation(
            type="query",
            queries=[
                Query(
                    name="Get",
                    fields=[
                        Field(
                            name = class_name,
                            fields=fields
                        )
                    ]
                )
            ]
        )
        graphql = op.render()
        resp = self.api_post("graphql", {"query": graphql})
        return resp

    def patch_object(self, class_name, id, body):
        return self.api_patch("objects", body, additional = class_name + '/' +  id)

    def build_graphql_values_from_simple_condition(self, condition : Dict):
        return [
                { "name": "operator", "value": condition["operator"] },
                { "name": "name", "value" : condition["path"] },
                { "name": "valueString", "value" : json.dumps(condition["valueString"])}
        ]
        return argument

    def build_graphql_argument_from_multiple_conditions(self, condition: Dict):
        operands = condition["operands"]
        arguments = []
        for operand in operands:
            arguments.append(self.build_graphql_values_from_simple_condition(operand))
        return arguments

    def build_graphql_values_from_condition(self, condition : Dict):

        if "operands" in condition:
            return [
                Argument(
                    name="operator",
                    value = condition["operator"]
                ),
                Argument(
                    name="operands",
                    value = self.build_graphql_argument_from_multiple_conditions(condition)
                )

            ]
        
        return Argument(
            name = condition["operator"],
            value = self.build_graphql_values_from_simple_condition(condition)
        )

    def build_graphql_where_argument(self, condition: Dict):
        argument = Argument(
            name = "where",
            value = self.build_graphql_values_from_condition(condition)
        )
        return argument

    def super_search(self, class_name: str, variables: Dict, properties: List = [], additional : List = []):

        TYPE_MAP = {
            "where": "GetObjectsDocumentWhereInpObj!",
            "bm25": "GetObjectsDocumentHybridGetBm25InpObj!"
        }

        variable_objs = {
            name: Variable(name=name, type=TYPE_MAP[name])
            for name in variables.keys()
            if name in TYPE_MAP
        }

        variable_definitions = list(variable_objs.values())

        arguments = [
            Argument(name=name, value=variable_objs[name])
            for name in variable_objs
        ]

        fields = properties
        if (additional):
            additional_field = Field(name="_additional", fields=additional)
            fields.append(additional_field)

        query = Query(
            name="Get",
            fields=[
                Field(
                    name=class_name,
                    arguments=arguments,
                    fields=fields
                )
            ]
        )
        operation = Operation(
            queries=[query],
            variables=variable_definitions
        )

        q = operation.render()

        resp = self.api_post("graphql", {
            "query": q,
            "variables": variables
        })

        return resp['data']['Get'][class_name]
                
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

    def ask(self,
            question: str,
            properties: list[str] | None = None,
            limit: int = 1,
            certainty: float | None = None
        ):
        """
        Esegue una query 'ask' su Weaviate utilizzando il modulo qna-openai.
        Restituisce la risposta estratta con campo 'result'.
        """
        args = [
            Argument(name="question", value=question)
        ]
        if properties:
            args.append(Argument(name="properties", value=properties))
        if certainty is not None:
            args.append(Argument(name="certainty", value=certainty))

        ask_arg = Argument(name="ask", value=args)
        limit_arg = Argument(name="limit", value=limit)

        ask_field = Field(
            name="DocumentChunk",
            arguments=[ask_arg, limit_arg],
            fields=[
                Field(
                    name="_additional",
                    fields=[
                        Field(name="answer", fields=[
                            "hasAnswer", "result", "certainty", "startPosition", "endPosition"
                        ])
                    ]
                )
            ]
        )

        op = Operation(
            type="query",
            queries=[Query(name="Get", fields=[ask_field])]
        )
        gql = op.render()
        resp = self.api_post("graphql", {"query": gql})
        return resp.get("data", {}).get("Get", {}).get("DocumentChunk", [])

    def generate(self,
                query: str,
                prompt: str,
                k: int = 1,
                alpha: float | None = None
        ):
        """
        Esegue una query 'generate' (RAG) su Weaviate utilizzando il modulo generative-openai.
        - query: la stringa di ricerca (semantica/ibrida)
        - prompt: template con placeholder {text} o altri campi
        - k: numero di documenti da recuperare
        - alpha: ponderazione per hybrid search (0-1)

        Restituisce i testi generati per ogni risultato.
        """
        # filtro di retrieval (vettoriale/ibrido)
        if alpha is not None:
            filter_arg = Argument(
                name="hybrid",
                value=[
                    Argument(name="query", value=query),
                    Argument(name="alpha", value=alpha)
                ]
            )
        else:
            filter_arg = Argument(
                name="nearText",
                value=[Argument(name="concepts", value=[query])]
            )
        limit_arg = Argument(name="limit", value=k)

        generate_args = [
            Argument(
                name="singleResult",
                value=[Argument(name="prompt", value=prompt)]
            )
        ]
        generate_field = Field(
            name="generate",
            arguments=generate_args,
            fields=["singleResult", "error"]
        )

        rag_field = Field(
            name="DocumentChunk",
            arguments=[filter_arg, limit_arg],
            fields=[
                Field(name="chunk_id"),
                Field(name="text"),
                Field(name="_additional", fields=[generate_field])
            ]
        )

        op = Operation(
            type="query",
            queries=[Query(name="Get", fields=[rag_field])]
        )
        gql = op.render()
        resp = self.api_post("graphql", {"query": gql})
        return resp.get("data", {}).get("Get", {}).get("DocumentChunk", [])

class WeaviateRetriever(BaseRetriever):
    client: WeaviateClient
    k: int = 3
    neighbors: int = 1

    def _get_relevant_documents(self, query: str) -> List[Document]:
        raw = self.client.query(query, self.k, self.neighbors)
        parts = [p for p in raw.split("\n\n---\n\n") if p.strip()]
        return [Document(page_content=p) for p in parts]
