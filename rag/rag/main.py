import requests
import os
from dotenv import load_dotenv

from auth import get_oauth_session
from authlib.integrations.requests_client import OAuth2Session

from functools import lru_cache
from weaviate_client import WeaviateClient #, ensure_schema, index_chunk, create_query_with_neighbors, query_with_neighbors, get_document_chunk, delete_document_chunks_by_source
import json

from types import SimpleNamespace

from utils import toJson

from requests.exceptions import HTTPError



from typing_extensions import Annotated
from typer import Typer, Context, Option, Argument, echo, secho, colors

load_dotenv()

app = Typer()

class PostgrestClient:
    def __init__(self, base_url: str, session: OAuth2Session ):
        self.base_url = base_url
        self.session = session

    def get_postgrest_schema(self) -> str:
        response = self.session.get(self.base_url).json()
        return response

    
def put_tika(path_to_file: str) -> str:

    session = get_oauth_session()

    tika_extract_endpoint = os.getenv("TIKA_EXTRACT_ENDPOINT")
    if not tika_extract_endpoint:
        raise EnvironmentError("TIKA_EXTRACT_ENDPOINT not found in environment variables.")

    headers = {"Accept": "text/plain"}
    try:
        with open(path_to_file, 'rb') as f:
            response = session.put(tika_extract_endpoint, data=f, headers=headers)
            response.raise_for_status()
            response.encoding = "utf-8"
            return response.text
    
    except Exception as e:
        raise

# @app.command()
# def ensure_schemas():
#     ensure_schema()

@app.command()
def get_schema(ctx: Context):
    schema = ctx.obj.weaviate_client.get_schema()
    print(toJson(schema))

@app.command()
def create_schema(ctx: Context):
    payload = {
                "class": "DocumentChunk",
                "vectorizer": "text2vec-ollama",
                "moduleConfig": {
                    "text2vec-ollama": {
                        "model": "nomic-embed-text",
                        "apiEndpoint": "http://ollama:11434"
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

    try:
        created = ctx.obj.weaviate_client.create_class(payload)
        echo(toJson(created))
    except HTTPError as e:
        if (e.response.status_code == 422):
            secho(f"cannot create error, api returns 422 status code, already exists?", err=True, fg=colors.RED)
        else:
            raise
    
@app.command()
def get_class(ctx: Context, class_name: Annotated[str, Argument(...)]):
    try:
        class_schema = ctx.obj.weaviate_client.get_class(class_name)
        print(toJson(class_schema))
    except HTTPError as e:
        if e.response.status_code == 404:
            secho(f"class {class_name} not found",err=True, fg=colors.RED)
        else:
            raise
    
@app.command()
def delete_class(ctx: Context, class_name: Annotated[str, Argument(...)]):
    ctx.obj.weaviate_client.delete_class(class_name)

@app.command()
def ingest_file(ctx: Context, file_path: Annotated[str, Argument(...)]):
    delete_chunks_by_source(ctx, file_path)
    extracted_text = put_tika(file_path)
    ctx.obj.weaviate_client.ingest_text(extracted_text, file_path)
    
@app.command()
def get_document_chunk(ctx: Context, source: str):
    resp = ctx.obj.weaviate_client.get_document_chunk(str)
    echo(toJson(resp))

@app.command()
def delete_chunks_by_source(ctx: Context, source: str):
    resp = ctx.obj.weaviate_client.delete_document_chunks_by_source(source)
    echo(toJson(resp))

@app.command()
def query(ctx: Context, text: str, k: Annotated[int, Argument(...)]):
    resp = ctx.obj.weaviate_client.query(text, k)
    echo(toJson(resp))
    

@app.callback()
def main(
    ctx: Context,
    weaviate_url:     Annotated[str, Option(..., envvar="WEAVIATE_URL", help="URL of the Weaviate instance")],
    weaviate_api_key: Annotated[str, Option(..., envvar="WEAVIATE_API_KEY", help="Weaviate bearer api key")]
):
    ctx.obj = SimpleNamespace()
    ctx.obj.weaviate_client = WeaviateClient(weaviate_url, weaviate_api_key)

    
    

    
    #file_path = "/home/niko/Scaricati/PSN_UserGuide_IaaS_Industry_Standardv3.0.3.pdf"

    #delete_document_chunks_by_source(file_path)

    # q = get_document_chunk(file_path)
    # print(json.dumps(q))
    # return


    #resp = get_oauth_session().get("https://psn-k1-sl.provincia.benevento.it/default/microservice-openldap/list-groups?base_dn=dc=provincia,dc=benevento,dc=it")
    #print(resp.json())

    # session = get_oauth_session()
    # pgc = PostgrestClient( os.getenv("POSTGREST_BASE_URL"), session)
    # print(pgc.get_postgrest_schema())
    
    
    #return
    
    
    # 
    # print(extracted_text)
    # return
    # # splitted_text = split_text_with_langchain(extracted_text)
    
    # 
    
    # n = len(splitted_text)
    # for i, text in enumerate(splitted_text):
    #     print("***** CHUNK",i+1,"of",n)
    #     #index_chunk(text, i, file_path)
    
    #print (json.dumps(query_with_neighbors("utilizzo del token",1,0), indent=2, ensure_ascii=False))
if __name__ == "__main__":
    app()


# {
#   Get {
#     DocumentChunk (where: {
#       path: ["source"],    # Path to the property that should be used
#       operator: Equal,  # operator
#       valueString: "/home/niko/Scaricati/PSN_UserGuide_IaaS_Industry_Standardv3.0.3.pdf"          # value (which is always = to the type of the path property)
#     }){
#       source,
#       chunk_id
#     }
#   }
# }