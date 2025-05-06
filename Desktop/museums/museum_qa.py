import requests
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance
import time
from typing import Optional, List

# Configuration
OLLAMA_API_BASE = "http://192.168.0.58:11434"
EMBED_MODEL = "nomic-embed-text:latest"
TEXT_MODEL = "mistral-small3.1:latest"
COLLECTION_NAME = "chateau-azay-le-ferron"
LOCAL_QDRANT_HOST = "localhost"
LOCAL_QDRANT_PORT = 6333
MAX_RETRIES = 3
RETRY_DELAY = 2

# Initialize Qdrant client
def connect_to_local_qdrant() -> Optional[QdrantClient]:
    print(f"Connecting to Qdrant at {LOCAL_QDRANT_HOST}:{LOCAL_QDRANT_PORT}")
    for attempt in range(MAX_RETRIES):
        try:
            client = QdrantClient(host=LOCAL_QDRANT_HOST, port=LOCAL_QDRANT_PORT)
            if client.collection_exists(COLLECTION_NAME):
                print(f"Collection '{COLLECTION_NAME}' found")
                return client
            print(f"Collection '{COLLECTION_NAME}' does not exist")
            return None
        except Exception as e:
            print(f"Connection attempt {attempt+1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                print(f"ERROR: Cannot connect to Qdrant at {LOCAL_QDRANT_HOST}:{LOCAL_QDRANT_PORT}")
                return None
    return None

# Generate embedding
def generate_ollama_embedding(text: str) -> List[float]:
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                f"{OLLAMA_API_BASE}/api/embeddings",
                json={"model": EMBED_MODEL, "prompt": text},
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            return result.get("embedding", [0.0] * 768)
        except requests.exceptions.RequestException as e:
            print(f"Embedding attempt {attempt+1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                print("Max retries reached for embedding")
                return [0.0] * 768
    return [0.0] * 768

# Query museum info
def query_museum(client: QdrantClient, question: str) -> str:
    question_embedding = generate_ollama_embedding(question)
    
    try:
        search_result = client.query_points(
            collection_name=COLLECTION_NAME,
            query=question_embedding,
            limit=3
        ).points
    except Exception as e:
        print(f"Qdrant query failed: {e}")
        return "Error: Could not retrieve information from Qdrant"

    context = "\n".join(point.payload.get("text", "")[:1000] for point in search_result)
    if not context:
        return "No relevant information found"

    prompt = f"""Based on:
{context}

Answer: {question}
Keep it concise and accurate."""
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                f"{OLLAMA_API_BASE}/api/generate",
                json={
                    "model": TEXT_MODEL,
                    "prompt": prompt,
                    "max_tokens": 150,
                    "stream": False
                },
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            return data.get("response", "No response from model").strip()
        except requests.exceptions.RequestException as e:
            print(f"Generation attempt {attempt+1} failed: {e}")
            print(f"Response text: {response.text if 'response' in locals() else 'No response'}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                print("Max retries reached for text generation")
                return "Error: Could not get response from Mistral API"
    return "Error: Could not get response from Mistral API"

# Test Ollama connection
def test_ollama_connection() -> bool:
    try:
        response = requests.get(f"{OLLAMA_API_BASE}/api/tags", timeout=10)
        response.raise_for_status()
        models = [model["name"] for model in response.json().get("models", [])]
        print(f"Ollama models: {models}")
        if EMBED_MODEL not in models or TEXT_MODEL not in models:
            print(f"Required models ({EMBED_MODEL}, {TEXT_MODEL}) not found")
            return False
        return True
    except requests.exceptions.RequestException as e:
        print(f"Ollama connection failed: {e}")
        return False

# Main execution
if __name__ == "__main__":
    if not test_ollama_connection():
        print("Exiting: Ollama connection or models unavailable")
        exit(1)

    client = connect_to_local_qdrant()
    if not client:
        print("Exiting: Qdrant connection or collection unavailable")
        exit(1)

    questions = [
        "What are the opening hours of the museum ?",
        "What can I see in there ?",
        "How do I contact them ?",
        "Where is it located?",
        "How can I access the place ?",
        "Is there a shop in there ?"
    ]

    for question in questions:
        answer = query_museum(client, question)
        print(f"Q: {question}")
        print(f"A: {answer}\n")