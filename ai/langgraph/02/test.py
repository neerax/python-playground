import weaviate
from weaviate.classes.init import Auth
from langchain_weaviate.vectorstores import WeaviateVectorStore
#from langchain.vectorstores import Weaviate as WeaviateVectorStore #client mismatch

#from langchain_community.vectorstores import Weaviate as WeaviateVectorStore



# === CONFIGURAZIONE ===
WEAVIATE_URL = "weaviate.provincia.benevento.it"
WEAVIATE_GRPC_URL = "gweaviate.provincia.benevento.it"


WEAVIATE_API_KEY = "shisi29099tesjlfl4i095u345sjl23y"  # Sostituiscila con la tua API Key
CLASS_NAME = "DocumentChunk"
TEXT_KEY = "text"
QUERY = "Quali sono i sintomi del diabete?"

# === CONNESSIONE A WEAVIATE CON API KEY (SOLO REST) ===
client = weaviate.connect_to_custom(
    http_host=WEAVIATE_URL,
    http_port=443,
    http_secure=True,
    grpc_host=WEAVIATE_GRPC_URL,     # obbligatorio ma non usato
    grpc_port=443,        # porta fittizia, evita conflitti
    grpc_secure=True,      # disabilitato
    auth_credentials=Auth.api_key(WEAVIATE_API_KEY)
)

# Verifica connessione
if not client.is_ready():
    raise RuntimeError("❌ Connessione a Weaviate fallita.")

print("✅ Connesso a Weaviate")

# === INIZIALIZZA IL VECTOR STORE CON langchain-weaviate ===
vectorstore = WeaviateVectorStore(
    client=client,
    index_name=CLASS_NAME,
    text_key=TEXT_KEY,
    embedding=None
)

#etriever = vectorstore.as_retriever(search_type="similarity_score_threshold)

# === ESEGUI LA QUERY ===
#retriever.invoke("come si usa il token?")

vectorstore.similarity_search("come si usa il token?")

# documents = retriever.get_relevant_documents(QUERY)

# # === MOSTRA I RISULTATI ===
# print(f"\n📄 Documenti trovati per la query: '{QUERY}'\n")
# for i, doc in enumerate(documents, 1):
#     print(f"{i}. {doc.page_content}\n")
