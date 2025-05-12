from functools import lru_cache
import os
import json
import requests
from authlib.jose import jwt
from authlib.integrations.requests_client import OAuth2Session
import time


def provider_discovery() -> dict:
    url = os.getenv("OIDC_ISSUER_URL").rstrip('/') + "/.well-known/openid-configuration"
    return requests.get(url).json()

def _load_token() -> dict | None:
    token_file = os.getenv("TOKEN_FILE", "token.json")
    try:
        with open(token_file, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None

def _save_token(token: dict):
    token_file = os.getenv("TOKEN_FILE", "token.json")
    with open(token_file, "w") as f:
        json.dump(token, f, indent=2)

@lru_cache(maxsize=1)
def get_oauth_session() -> OAuth2Session:
    client_id      = os.getenv("OIDC_CLIENT_ID")
    client_secret  = os.getenv("OIDC_CLIENT_SECRET")
    scope          = os.getenv("OIDC_SCOPE", None)
    token_endpoint = os.getenv("OIDC_TOKEN_ENDPOINT")
    
    session = OAuth2Session(
        client_id=client_id,
        client_secret=client_secret,
        scope=scope,
        token=_load_token(),
        token_endpoint=token_endpoint,
        update_token = _save_token
    )
    
    session.fetch_token(grant_type="client_credentials")

    return session

