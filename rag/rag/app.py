import os
from typing import Callable
from weaviate_client import WeaviateClient
from auth import get_oauth_session
from datetime import datetime, timezone


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
        # timestamp di modifica
        mtime = os.path.getmtime(path)
        # conversione in RFC3339 (UTC, con offset +00:00)
        modified_time = datetime.fromtimestamp(mtime, timezone.utc).isoformat()
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
        result = self.weaviate_client.super_search("Document", { "source" : file_path }, additional=["id"])
        result_length = len (result)
        if result_length > 1:
            raise Exception("document by path returned more then 1 document")
        else:
            if result_length == 1:
                return result[0]

        return None

    def get_document_id_by_file_path(self, file_path: str):
        result = self.get_document_by_file_path(file_path)
        if not result:
            return None
        return result["_additional"]["id"]

    def ingest_file(self, file_path, size, m_time):
        print("processing file", file_path, size, m_time)
        
        id = self.get_document_id_by_file_path(file_path)

        print("ID",id)

        if id is None:
            #self.weaviate_client.delete_chunks_by_source(file_path)
            extracted_text = put_tika(file_path)
            self.weaviate_client.ingest("Document", text=extracted_text, source=file_path, size=size, m_time = m_time, vectorized=False)
            #self.weaviate_client.ingest_text(extracted_text, file_path)
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
    
        