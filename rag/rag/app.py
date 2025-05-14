import os
from typing import Callable
from weaviate_client import WeaviateClient
from auth import get_oauth_session

def file_func_call(path: str, func: Callable[[str], None], recursive: bool = False):

    if not os.path.exists(path):
        raise FileNotFoundError(f"Il percorso '{path}' non esiste.")

    if os.path.isfile(path):
        abs_path = os.path.abspath(path)
        func(abs_path)
    elif os.path.isdir(path):
        if not recursive:
            raise ValueError(f"Il percorso '{path}' è una directory ma 'recursive' è False.")
        else:
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


    def ingest_file(self, file_path):
        print("processing file", file_path)
        #self.weaviate_client.delete_chunks_by_source(file_path)
        extracted_text = put_tika(file_path)
        self.weaviate_client.ingest("Document", text=extracted_text, source=file_path)
        #self.weaviate_client.ingest_text(extracted_text, file_path)

    def ingest_path(self, path: str, recursive: bool = False):
        return file_func_call(path, self.ingest_file, recursive)
    
    def get_documents(self):
        return self.weaviate_client.get_objects("Document",["text","source"])['data']['Get']['Document']
    
        