import requests

from graphql_query import Operation, Query, Field as GField, Argument, Variable
from typing import List, Dict
from requests.exceptions import HTTPError
from urllib.parse import urlencode

class WeaviateClient:
    def __init__(self, url, weaviate_api_key):
        self.url = url.rstrip('/')+'/'
        self.headers = {
            "Content-Type": "application/json",
            **({"Authorization": "Bearer " + weaviate_api_key} if weaviate_api_key else {})
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

    def get_objects(self, class_name: str, fields = []):
        fields.append(Field(name="_additional", fields=["id"]))
        op = Operation(
            type="query",
            queries=[
                Query(
                    name="Get",
                    fields=[
                        GField(
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

    def super_search(self, class_name: str, variables: Dict, properties: List = [], additional : List = [], neighbors = 0, neighbors_index_name="chunk_id", key_property_name="source"):

        TYPE_MAP = {
            "where": "GetObjects"+class_name+"WhereInpObj!",
            "bm25": "GetObjects"+class_name+"HybridGetBm25InpObj!",
            "nearText": "GetObjects"+class_name+"NearTextInpObj!",
            #"limit": "Int!" ERRORE GRAVOTTO
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

        if "limit" in variables:
            arguments.append(Argument(name="limit", value=variables["limit"]))

        fields = properties
        if (additional):
            additional_field = GField(name="_additional", fields=additional)
            fields.append(additional_field)

        query = Query(
            name="Get",
            fields=[
                GField(
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

        print("QUERY",q, variables)

        resp = self.api_post("graphql", {
            "query": q,
            "variables": variables
        })

        objects=resp['data']['Get'][class_name]

        if neighbors > 0 and objects:
            # 1) costruisco (chunk_id, source) dei vicini
            neighbor_pairs = set()
            for o in objects:
                cid = o[neighbors_index_name]
                src = o[key_property_name]
                for delta in range(1, neighbors+1):
                    if cid - delta >= 0:
                        neighbor_pairs.add((cid - delta, src))
                    neighbor_pairs.add((cid + delta, src))

            # 2) filtro GraphQL OR-of-ANDs
            where_filter = {
                "operator": "Or",
                "operands": [
                    {
                        "operator": "And",
                        "operands": [
                            {"path": [neighbors_index_name],
                             "operator": "Equal",
                             "valueInt": neigh_cid},
                            {"path": [key_property_name],
                             "operator": "Equal",
                             "valueString": neigh_src}
                        ]
                    }
                    for neigh_cid, neigh_src in neighbor_pairs
                ]
            }

            # 3) richiamo super_search per i soli neighbor, disabilitando la ricorsione
            neighbor_objs = self.super_search(
                class_name,
                variables={
                    "where": where_filter,
                    #"limit": len(neighbor_pairs),
                },
                properties=properties,
                additional=additional,
                neighbors=0,
                neighbors_index_name=neighbors_index_name,
                key_property_name=key_property_name
            )

            # 4) unisco e ordino
            objects.extend(neighbor_objs)

        if objects and neighbors_index_name in objects[0]:
            return sorted(objects, key=lambda x: x[neighbors_index_name])
        
        return objects
                        
    def delete_objects(self, class_name: str, where: Dict):
        payload = {
            "match": {
              "class": class_name,
              "where": where
            },
            "output": "verbose",
            "dryRun": False
        }

        resp = self.api_delete_with_json("batch", "objects", payload)

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


    def nearText(self, class_name: str, text: str, properties : list[str], additional: list[str], k: int = 1, neighbors: int = 1, neighbors_property_name: str = "chunk_id", same_property_name: str = "source"):

        for a in ['certainty', 'distance', 'score']:
            if not a in additional:
                additional.append(a)

        if (neighbors > 0):
            if not neighbors_property_name in properties:
                properties.append(neighbors_property_name)
            if not "id" in additional:
                additional.append("id")

        objects = self.super_search(
            class_name,
            {
                "nearText": {
                    "concepts": [text],
                },
                "limit": k
            },
            properties=properties,
            additional=additional,
            neighbors=neighbors,
            neighbors_index_name=neighbors_property_name,
            key_property_name=same_property_name
        )

        return objects
        

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

        ask_field = GField(
            name="DocumentChunk",
            arguments=[ask_arg, limit_arg],
            fields=[
                GField(
                    name="_additional",
                    fields=[
                        GField(name="answer", fields=[
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
        generate_field = Gield(
            name="generate",
            arguments=generate_args,
            fields=["singleResult", "error"]
        )

        rag_field = Field(
            name="DocumentChunk",
            arguments=[filter_arg, limit_arg],
            fields=[
                Gield(name="chunk_id"),
                Gield(name="text"),
                Gield(name="_additional", fields=[generate_field])
            ]
        )

        op = Operation(
            type="query",
            queries=[Query(name="Get", fields=[rag_field])]
        )
        gql = op.render()
        resp = self.api_post("graphql", {"query": gql})
        return resp.get("data", {}).get("Get", {}).get("DocumentChunk", [])

