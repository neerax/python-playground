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


class Tools:
    class Valves(BaseModel):
        WEAVIATE_URL: str = Field(
            "https://my-weaviate.com",
            description="URL base del server Weaviate, es. https://my-weaviate.com",
        )
        WEAVIATE_API_KEY: SecretStr = Field(
            SecretStr(""),
            description="API key per l'autenticazione su Weaviate (facoltativa)",
        ),
        WEAVIATE_K: int = Field(
            5,
            description = "Number of documents to return"
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

    def super_search(
        self,
        class_name: str,
        variables: dict,
        properties: list[str] = None,
        additional: list[str] = None,
    ) -> list[dict]:
        TYPE_MAP = {
            "where": f"GetObjects{class_name}WhereInpObj!",
            "bm25": f"GetObjects{class_name}HybridGetBm25InpObj!",
            "nearText": f"GetObjects{class_name}NearTextInpObj!",
        }
        # GraphQL variable definitions
        variable_defs = [
            Variable(name=name, type=TYPE_MAP[name])
            for name in variables
            if name in TYPE_MAP
        ]
        # Build arguments
        args = []
        for name, val in variables.items():
            if name in TYPE_MAP:
                args.append(
                    Argument(name=name, value=Variable(name=name, type=TYPE_MAP[name]))
                )
            elif name == "limit":
                args.append(Argument(name="limit", value=val))

        # Fields to return
        fields = properties or []
        if additional:
            fields.append(GField(name="_additional", fields=additional))

        # Construct query
        query = Query(
            name="Get", fields=[GField(name=class_name, arguments=args, fields=fields)]
        )
        op = Operation(queries=[query], variables=variable_defs)
        gql = op.render()

        payload = {"query": gql, "variables": variables}
        resp = self.api_post("graphql", payload)
        return resp.get("data", {}).get("Get", {}).get(class_name, [])

    async def run(self, text: str, __event_emitter__=None) -> dict:
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
        properties = ["text", "source"]

        try:
            objects = self.super_search(
                "DocumentChunk",
                {"nearText": {"concepts": [text]}, "limit": self.valves.WEAVIATE_K},
                properties=properties,
                additional=additional,
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
