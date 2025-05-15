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

from chatbot import run_chat
from app import RagApp

load_dotenv()

app = Typer()

class PostgrestClient:
    def __init__(self, base_url: str, session: OAuth2Session ):
        self.base_url = base_url
        self.session = session

    def get_postgrest_schema(self) -> str:
        response = self.session.get(self.base_url).json()
        return response

    

# @app.command()
# def ensure_schemas():
#     ensure_schema()

@app.command()
def get_schema(ctx: Context):
    schema = ctx.obj.app.get_weaviate_client().get_schema()
    print(toJson(schema))

@app.command()
def apply_schema(ctx: Context, file_path: str):
    print("opening", file_path)
    with open(file_path, "r") as f:
        definitions = json.load(f)
        created, skipped, failed = ctx.obj.app.get_weaviate_client().apply_schema(definitions)

    secho("Created:", fg=colors.GREEN)
    secho(toJson(created))
    secho("Skipped:", fg=colors.YELLOW)
    secho(toJson(skipped))
    secho("Failed:", fg=colors.RED)
    secho(toJson(failed))
    
@app.command()
def get_class(ctx: Context, class_name: Annotated[str, Argument(...)]):
    try:
        class_schema = ctx.obj.app.get_weaviate_client().get_class(class_name)
        print(toJson(class_schema))
    except HTTPError as e:
        if e.response.status_code == 404:
            secho(f"class {class_name} not found",err=True, fg=colors.RED)
        else:
            raise
    
@app.command()
def delete_class(ctx: Context, class_name: Annotated[str, Argument(...)]):
    ctx.obj.app.get_weaviate_client().delete_class(class_name)

@app.command()
def ingest(ctx: Context, file_path: str, recursive: Annotated[bool, Option("--recursive", "-r", help="Recursive scan", show_default=True)] = False):
    ctx.obj.app.ingest_path(file_path, recursive)
    

# @app.command()
# def ingest_file(ctx: Context, file_path: Annotated[str, Argument(...)]):
#     delete_chunks_by_source(ctx, file_path)
#     extracted_text = put_tika(file_path)
#     ctx.obj.weaviate_client.ingest_text(extracted_text, file_path)
    
# @app.command()
# def get_document_chunk(ctx: Context, source: str):
#     resp = ctx.obj.weaviate_client.get_document_chunk(str)
#     echo(toJson(resp))

@app.command()
def show_documents(ctx: Context):
    documents = ctx.obj.app.get_documents()
    for document in documents:
        secho(document["_additional"]["id"])
        secho(document["source"])
        secho(document["vectorized"])
#        print(document)

@app.command()
def delete_chunks_by_source(ctx: Context, source: str):
    resp = ctx.obj.weaviate_client.delete_document_chunks_by_source(source)
    echo(toJson(resp))

@app.command()
def query(
    ctx: Context,
    text: Annotated[str, Argument(..., help="The query text")],
    k: Annotated[int, Option("--k", "-k", help="Number of top results", show_default=True)] = 3,
    neighbors: Annotated[int, Option("--neighbors", "-n", help="Number of neighbor chunks to include", show_default=True)] = 1
):
    resp = ctx.obj.app.nearText(text, k, neighbors)
    echo(toJson(resp))

@app.command()
def bm25(ctx: Context, class_name: str, text: str):
    result = ctx.obj.app.bm25(class_name, text)
    print("DOCUMENTI TROVATI", len(result))
    echo(toJson(result))

@app.command()
def chat(
    ctx: Context,
    groq_api_key: Annotated[str, Option("--groq-api-key", "-g",help="API Key per Groq Chat",envvar="GROQ_API_KEY")],
    k: Annotated[int, Option("--k", "-k", help="Top-k chunks", show_default=True)] = 3,
    neighbors: Annotated[int, Option("--neighbors", "-n", help="Vicini da includere", show_default=True)] = 1
):
    """
    Avvia la chat RAG interattiva usando GroqChat + Weaviate.
    """
    client = ctx.obj.weaviate_client
    run_chat(
        client=client,
        groq_api_key=groq_api_key,
        k=k,
        neighbors=neighbors
    )

@app.command()
def patch_object(
    ctx: Context,
    class_name: str,
    id: str,
    body: str
):
    w = ctx.obj.app.get_weaviate_client()
    w.patch_object(class_name, id, json.loads(body))

@app.command()
def get_document_by_path(
    ctx: Context,
    path: str
):
    w = ctx.obj.app.get_document_by_file_path(path)
    print(w)

@app.command()
def get_chunks_by_path(
    ctx: Context,
    path: str
):
    w = ctx.obj.app.get_chunks_by_file_path(path)
    print(w)

@app.command()
def delete_document_by_path(
    ctx: Context,
    path: str
):
    w = ctx.obj.app.delete_documents_by_source(path)
    print(w)


@app.command()
def delete_chunks_by_path(
    ctx: Context,
    path: str
):
    w = ctx.obj.app.delete_chunks_by_source(path)

@app.command()
def chunks_near_text(
    ctx: Context,
    text: str,
    k: int,
    neighbors: int
):
    w = ctx.obj.app.chunks_near_text(text, k, neighbors)
    print(len(w))
    print(w)
    
        

##    print(w)


@app.command()
def test(
    ctx: Context
):

    w = ctx.obj.app.get_weaviate_client()



    #print (ctx.obj.app.get_weaviate_client().build_graphql_where_argument(d).render())
    # print (ctx.obj.app.get_weaviate_client().super_search("Document", {
    #         "where": {
    #             "operator": "Equal",
    #             "path": ["source"],
    #             "valueString": "/home/niko/Sviluppo/python-playground/rag/rag/documenti/PSN_UserGuide_IaaS_Industry_Standardv3.0.3.pdf"
    #         }
    #     }, properties=["size", "source","m_time"],additional=["id"]))
    
    # print (ctx.obj.app.get_weaviate_client().super_search("Document",{
    #         "bm25": {
    #             "query": "Cloud"
    #         },
    #         "where": {
    #             "operator": "Equal",
    #             "path": ["source"],
    #             "valueString": "/home/niko/Sviluppo/python-playground/rag/rag/documenti/PSN_UserGuide_IaaS_Industry_Standardv3.0.3.pdf"
    #         }
    #     }, properties=["size", "source","m_time"],additional=["id","score"]))
    

@app.callback()
def main(
    ctx: Context,
    weaviate_url:     Annotated[str, Option(..., envvar="WEAVIATE_URL", help="URL of the Weaviate instance")],
    weaviate_api_key: Annotated[str, Option(..., envvar="WEAVIATE_API_KEY", help="Weaviate bearer api key")]
):
    ctx.obj = SimpleNamespace()
    ctx.obj.app = RagApp(weaviate_url, weaviate_api_key)
    
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