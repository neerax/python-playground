import requests
import os
from dotenv import load_dotenv
from auth import get_oauth_session
from authlib.integrations.requests_client import OAuth2Session

load_dotenv()


class PostgrestClient:
    def __init__(self, base_url: str, session: OAuth2Session ):
        self.base_url = base_url
        self.session = session

    def get_postgrest_schema(self) -> str:
        response = self.session.get(self.base_url).json()
        return response
    
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

    session = get_oauth_session()
    pgc = PostgrestClient( os.getenv("POSTGREST_BASE_URL"), session)
    print(pgc.get_postgrest_schema())
    
    
    return
    
    file_path = "/home/niko/Scaricati/Avviso 1.2 - Province e Città metropolitane.pdf"
    try:
        extracted_text = put_tika(file_path)
        print("Testo estratto:")
        print(extracted_text)
    except Exception as e:
        print(f"Si è verificato un errore imprevisto: {e}")

if __name__ == "__main__":
    main()