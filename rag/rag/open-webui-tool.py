"""
title: Weaviate Search
author: Nicola Ranaldo
description: Tool per eseguire una ricerca semantica di concetti e/o domande su un server Weaviate
requirements: requests, graphql_query, pydantic
version: 0.1.0
"""

import requests
from pydantic import BaseModel, Field
from openwebui_tool import Tool, EventEmitter
from graphql_query import Operation, Query, Field, Argument, Variable

class WeaviateSuperSearchTool(Tool):
    class Valves(BaseModel):
        WEAVIATE_URL: str = Field(..., description="URL base del server Weaviate, es. https://my-weaviate.com")
        WEAVIATE_API_KEY: str = Field(default="", description="API key per l'autenticazione su Weaviate (facoltativa)")

    class UserValves(BaseModel):
        pass

    def __init__(self):
        super().__init__()
        self.valves = self.Valves()
        self.user_valves = self.UserValves()

    def api_build_url(self, path: str, version: str = "v1"):
        base = self.valves.WEAVIATE_URL.rstrip('/') + '/' + version + '/'
        return base + path.lstrip('/')

    def api_post(self, path: str, body: dict, version: str = "v1"):
        url = self.api_build_url(path, version)
        headers = {"Content-Type": "application/json"}
        if self.valves.WEAVIATE_API_KEY:
            headers["Authorization"] = f"Bearer {self.valves.WEAVIATE_API_KEY}"
        resp = requests.post(url, json=body, headers=headers)
        resp.raise_for_status()
        return resp.json()

    def super_search(
        self,
        class_name: str,
        variables: dict,
        properties: list = None,
        additional: list = None
    ):
        """
        Esegue una query GraphQL di tipo 'super search' su Weaviate.
        - class_name: nome della classe Weaviate da interrogare
        - variables: dizionario con filtri ("where"), "bm25" o "nearText", e "limit"
        - properties: lista di campi da restituire
        - additional: lista di campi _additional (es. ["id", "score"])
        """
        TYPE_MAP = {
            "where": f"GetObjects{class_name}WhereInpObj!",
            "bm25": f"GetObjects{class_name}HybridGetBm25InpObj!",
            "nearText": f"GetObjects{class_name}NearTextInpObj!",
        }
        # Definizioni variabili GraphQL
        variable_defs = [
            Variable(name=name, type=TYPE_MAP[name])
            for name in variables if name in TYPE_MAP
        ]
        # Costruiamo gli argomenti della query
        args = []
        for name, val in variables.items():
            if name in TYPE_MAP:
                args.append(Argument(name=name, value=Variable(name=name, type=TYPE_MAP[name])))
            elif name == "limit":
                args.append(Argument(name="limit", value=val))

        # Predisponi campi da restituire
        fields = properties or []
        if additional:
            fields.append(Field(name="_additional", fields=additional))

        # Costruzione della query
        query = Query(
            name="Get",
            fields=[
                Field(
                    name=class_name,
                    arguments=args,
                    fields=fields
                )
            ]
        )
        op = Operation(queries=[query], variables=variable_defs)
        gql = op.render()

        payload = {"query": gql, "variables": variables}
        resp = self.api_post("graphql", payload)
        return resp.get("data", {}).get("Get", {}).get(class_name, [])

    async def run(self, text: str, emitter: EventEmitter):
        """
        params deve contenere:
        - class_name: str
        - variables: dict
        - properties: list[str]
        - additional: list[str]
        """
    
        emitter.emit("🔍 Avvio super_search su Weaviate…")

        additional = ['certainty', 'distance', 'score']
        properties = ["text","source"]
        
        objects = self.super_search(
            'DocumentChunk',
            {
                "nearText": {
                    "concepts": [text],
                },
                "limit": 5
            },
            properties=properties,
            additional=additional
        )

        emitter.emit(f"✅ Trovati {len(objects)} oggetti.")

        return {"results": objects}

        
        
