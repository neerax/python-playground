import requests
import os
from dotenv import load_dotenv
from auth import get_oauth_session
from authlib.integrations.requests_client import OAuth2Session
from langchain.text_splitter import RecursiveCharacterTextSplitter
from functools import lru_cache
from weaviate_client import ensure_schema, index_chunk


load_dotenv()


class PostgrestClient:
    def __init__(self, base_url: str, session: OAuth2Session ):
        self.base_url = base_url
        self.session = session

    def get_postgrest_schema(self) -> str:
        response = self.session.get(self.base_url).json()
        return response

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

    
def put_tika(path_to_file: str) -> str:
    
    tika_extract_endpoint = os.getenv("TIKA_EXTRACT_ENDPOINT")
    if not tika_extract_endpoint:
        raise EnvironmentError("TIKA_EXTRACT_ENDPOINT not found in environment variables.")

    headers = {"Accept": "text/plain"}
    try:
        with open(path_to_file, 'rb') as f:
            response = requests.put(tika_extract_endpoint, data=f, headers=headers)
            response.raise_for_status()
            return response.text
    
    except Exception as e:
        raise

def main():
    
    #resp = get_oauth_session().get("https://psn-k1-sl.provincia.benevento.it/default/microservice-openldap/list-groups?base_dn=dc=provincia,dc=benevento,dc=it")
    #print(resp.json())

    # session = get_oauth_session()
    # pgc = PostgrestClient( os.getenv("POSTGREST_BASE_URL"), session)
    # print(pgc.get_postgrest_schema())
    
    
    #return
    
    file_path = "/home/niko/Scaricati/Avviso 1.2 - Province e Città metropolitane.pdf"
    extracted_text = put_tika(file_path)
    splitted_text = split_text_with_langchain(extracted_text)
    
    ensure_schema()
    
    print("Testo estratto:")
    for i, text in enumerate(splitted_text):
        print("***** CHUNK",i)
        print(text)
    
    
if __name__ == "__main__":
    main()