import os
from typing import Callable
from weaviate_client import WeaviateClient
from auth import get_oauth_session
from datetime import datetime, timezone
from dateutil import parser
from functools import lru_cache
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


def file_func_call(
    path: str,
    func: Callable[[str, int, str], None],
    recursive: bool = False
):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Il percorso '{path}' non esiste.")

    if os.path.isfile(path):
        abs_path = os.path.abspath(path)
        size = os.path.getsize(path)
        mtime = os.path.getmtime(path)
        modified_time = datetime.fromtimestamp(mtime, timezone.utc)
        func(abs_path, size, modified_time)

    elif os.path.isdir(path):
        if not recursive:
            raise ValueError(f"Il percorso '{path}' è una directory ma 'recursive' è False.")
        for root, dirs, files in os.walk(path):
            for file in files:
                full_path = os.path.join(root, file)
                file_func_call(full_path, func, recursive)

    else:
        raise ValueError(f"Il percorso '{path}' non è né file né directory.")
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


class RagApp:
    def __init__(self, url, weaviate_api_key):
        self.url = url
        self.weaviate_api_key = weaviate_api_key
        self.weaviate_client = WeaviateClient(url, weaviate_api_key)

    def get_weaviate_client(self):
        return self.weaviate_client

    def get_document_by_file_path(self, file_path : str):
        result = self.weaviate_client.super_search(
            "Document",
            { "where": 
                {
                    "operator": "Equal",
                    "path" : ["source"],
                    "valueString" : file_path
                }
            },
            properties=["size", "source","m_time", "vectorized"],
            additional=["id"]
        )
        result_length = len (result)
        if result_length > 1:
            raise Exception("document by path returned more then 1 document")
        else:
            if result_length == 1:
                return result[0]

        return None
    
    def get_chunks_by_file_path(self, file_path : str):
        result = self.weaviate_client.super_search(
            "DocumentChunk",
            { "where": 
                {
                    "operator": "Equal",
                    "path" : ["source"],
                    "valueString" : file_path
                }
            },
            properties=["size", "source","m_time"],
            additional=["id"]
        )

        return result


    def get_id_from_object(self, object):
        print("OBJECT****************", object)
        return object["_additional"]["id"]


    def get_document_id_by_file_path(self, file_path: str):
        result = self.get_document_by_file_path(file_path)
        if not result:
            return None
        return self.get_id_from_object(result)

    def ingest_chunks(self, text, source):
        chunks = split_text_with_langchain(text)
        n = len(chunks)
        for i, chunk in enumerate(chunks):
            print("Processing CHUNK", i+1, "of", n)
            self.weaviate_client.ingest("DocumentChunk", text=chunk, source=source, chunk_id=i)

    def delete_objects_by_source(self, class_name: str, source: str):
        self.weaviate_client.delete_objects(
            class_name,
            {
                    "path": ["source"],
                    "operator": "Equal",
                    "valueString": source
            }
        )


    def delete_documents_by_source(self, source: str):
        self.delete_objects_by_source("Document", source)

    def delete_chunks_by_source(self, source: str):
        self.delete_objects_by_source("DocumentChunk", source)
        
    def pipeline(self, file_path, size, m_time):
        extracted_text = put_tika(file_path)
        result = self.weaviate_client.ingest("Document", text=extracted_text, source=file_path, size=size, m_time = m_time.isoformat(), vectorized=False)
        id = result["id"]
        self.ingest_chunks(extracted_text, file_path)
        self.weaviate_client.patch_object("Document", id, {"properties": {"vectorized": True}})

    def ingest_file(self, file_path, size, m_time):
        print("processing file", file_path, size, m_time)
        
        doc = self.get_document_by_file_path(file_path)
        
        if doc is None:
            self.delete_chunks_by_source(file_path) # per sicurezza, potremmo aver cancellato il documento padre e ci potrebbero essere chunks orfani
            self.pipeline(file_path, size, m_time)
            #self.weaviate_client.delete_chunks_by_source(file_path)            
        else:
            # verifichiamo che il file non sia cambiato

            parsed = parser.parse(doc["m_time"])
            if doc["size"] != size or parsed != m_time or not doc["vectorized"]:
                print("*** Document modified, reingesting", file_path)
                self.delete_documents_by_source(file_path)
                self.delete_chunks_by_source(file_path)
                self.pipeline(file_path, size, m_time)
            else:
                print("skipping", file_path)

    def ingest_path(self, path: str, recursive: bool = False):
        return file_func_call(path, self.ingest_file, recursive)
    
    def get_documents(self):
        return self.weaviate_client.get_objects("Document",["text","source","vectorized"])['data']['Get']['Document']
    
    def bm25(self, class_name, text):
        return self.weaviate_client.super_search(
            class_name,
            {
                "bm25": {
                    "query": text
                }
            },
            properties=[
                "size",
                "source",
                "m_time",
                "vectorized"
            ],
            additional=[
                "id",
                "score"
            ]
        )
    
    def chunks_near_text(self, text, k, neighbors):
        return self.weaviate_client.nearText(
            "DocumentChunk",
            text,
            properties=[
                "source",
                "m_time",
                #"text"
            ],
            additional=[
                "id",
                "score"
            ],
            k=k,
            neighbors=neighbors
        )
    
        