"""
Script di test per Tools di openwebui, mock dell'environment e delle chiamate HTTP.
"""
import os
import asyncio
from dotenv import load_dotenv

# Carica variabili da .env
load_dotenv()

from search_webui_tool import Tools

# Imposta env per il tool
os.environ.setdefault("WEAVIATE_URL", "https://mock-weaviate.test")
os.environ.setdefault("WEAVIATE_API_KEY", "test-key")

# Test runner
class CollectorEmitter:
    async def emit(self, message):
        pass
        #print(message)

async def test_run():
    tool = Tools()
    # Iniezione manuale di valves (simulazione Open WebUI)
    tool.valves.WEAVIATE_URL = os.getenv("WEAVIATE_URL")
    tool.valves.WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")
    tool.valves.WEAVIATE_K = 10


    emitter = CollectorEmitter()
    result = await tool.run("piano di migrazione al cloud", __event_emitter__=emitter.emit)

    print("Result:", len(result["results"]))

if __name__ == "__main__":
    asyncio.run(test_run())