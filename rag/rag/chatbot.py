from langchain_groq.chat_models import ChatGroq
from langchain.chains import ConversationalRetrievalChain
from langchain.memory import ConversationBufferMemory
from weaviate_client import WeaviateClient, WeaviateRetriever

def run_chat(
    client: WeaviateClient,
    groq_api_key: str,
    k: int = 3,
    neighbors: int = 1
):
    """
    Avvia una sessione interattiva di chat RAG
    """
    # Prepariamo retriever e modello
    retriever = WeaviateRetriever(client=client, k=k, neighbors=neighbors)
    llm = ChatGroq(api_key=groq_api_key, model="meta-llama/llama-4-scout-17b-16e-instruct")
    
    # Configurazione corretta della memoria specificando l'output_key
    memory = ConversationBufferMemory(
        memory_key="chat_history",
        return_messages=True,
        output_key="answer"  # Specifichiamo esplicitamente quale output memorizzare
    )

    qa = ConversationalRetrievalChain.from_llm(
        llm,
        retriever=retriever,
        memory=memory,
        return_generated_question=True,

        return_source_documents=True  # Questo crea più output ('answer' e 'source_documents')
    )

    print("🚀 Avvio chat (digita EXIT o QUIT per uscire) 🚀")
    print("ℹ️  Per domande senza contesto documentale, inizia con: !norag")

    while True:
        q = input(">> ")
        if q.strip().lower() in ("exit", "quit"):
            print("👋 Chat terminata.")
            break

        if q.strip().lower().startswith("!norag"):
            q_clean = q.strip()[6:].strip()
            # Memoria manuale anche per no-RAG
            memory.chat_memory.add_user_message(q_clean)
            response = llm.invoke(q_clean)
            memory.chat_memory.add_ai_message(response.content)
            print(response.content.strip())
        else:
            resp = qa.invoke({"question": q})
            print(resp.get("answer", "").strip())
            
            # Opzionale: mostra le fonti se presenti
            if "source_documents" in resp and resp["source_documents"]:
                print("\nFonti:")
                for i, doc in enumerate(resp["source_documents"][:2]):  # Limita a 2 fonti per brevità
                    print(f"{i+1}. {doc.page_content[:100]}...")  # Mostra i primi 100 caratteri del contenuto