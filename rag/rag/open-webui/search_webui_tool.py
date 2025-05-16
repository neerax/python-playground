"""
title: Weaviate Search
author: Nicola Ranaldo
description: Tool per eseguire una ricerca semantica di concetti e/o domande su un server Weaviate
requirements: graphql-query, requests, pydantic
version: 0.1.0
"""

import requests
import json
from pydantic import BaseModel, Field, SecretStr
from graphql_query import Operation, Query, Field as GField, Argument, Variable
from typing import Dict, List

class Tools:
    class Valves(BaseModel):
        WEAVIATE_URL: str = Field(
            "https://my-weaviate.com",
            description="URL base del server Weaviate, es. https://my-weaviate.com"
        )
        WEAVIATE_API_KEY: SecretStr = Field(
            SecretStr(""),
            description="API key per l'autenticazione su Weaviate (facoltativa)"
        ),
        WEAVIATE_K: int = Field(
            5,
            description = "Number of documents to return"
        ),
        neighbours: int = Field(
            0,
            description = "Number of neighbours chunks to retreieve first and after"
        )

    class UserValves(BaseModel):
        pass

    def __init__(self):
        self.valves = self.Valves()
        self.user_valves = self.UserValves()

    def api_build_url(self, path: str, version: str = "v1") -> str:
        base = self.valves.WEAVIATE_URL.rstrip("/") + "/" + version + "/"
        return base + path.lstrip("/")

    def api_post(self, path: str, body: dict, version: str = "v1") -> dict:
        url = self.api_build_url(path, version)
        headers = {"Content-Type": "application/json"}
        if self.valves.WEAVIATE_API_KEY:
            headers["Authorization"] = f"Bearer {self.valves.WEAVIATE_API_KEY}"
        resp = requests.post(url, json=body, headers=headers)
        resp.raise_for_status()
        return resp.json()

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

    async def search(self, text: str, __event_emitter__=None) -> dict:
        """
        Riceve in input un testo ed avvia la ricerca sul server Weaviate, restituisce un dict json con frammenti di testo contenenti le informazioni cercate
        """
        emitter = __event_emitter__ or (lambda msg: None)

        await emitter(
            {
                "type": "status",
                "data": {"description": "Avvio ricerca...", "done": False},
            }
        )
        additional = ["certainty", "distance", "score"]
        properties = ["text", "source", "chunk_id"]

        try:
            objects = self.super_search(
                "DocumentChunk",
                {"nearText": {"concepts": [text]}, "limit": self.valves.WEAVIATE_K},
                properties=properties,
                additional=additional,
                neighbors=self.valves.neighbours
            )
            await emitter(
                {
                    "type": "status",
                    "data": {
                        "description": f"Trovati {len(objects)} oggetti",
                        "done": True,
                    },
                }
            )
            pretty = json.dumps({"results": objects}, indent=2, ensure_ascii=False)
            await emitter(
                {"type": "message", "data": {"content": f"```json\n{pretty}\n```"}}
            )
            await emitter(
                {"type": "message", "data": {"content": "Ricerca completata"}}
            )

            return {"results": objects}
        except Exception as e:
            await emitter({"type": "error", "data": {"description": str(e)}})
            raise
