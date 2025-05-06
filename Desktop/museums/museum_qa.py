import requests
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, VectorParams, Distance
import uuid
import json
import numpy as np

# Initialize Qdrant client
qdrant_client = QdrantClient(host="localhost", port=6333)

# Mistral API endpoint (Ollama-based)
MISTRAL_ENDPOINT = "http://192.168.0.58:11434"

# Collection name in Qdrant
COLLECTION_NAME = "museum_info"

# Function to create collection if it doesn't exist
def create_collection():
    try:
        qdrant_client.get_collection(COLLECTION_NAME)
    except:
        qdrant_client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=768, distance=Distance.COSINE)
        )

# Function to get embeddings from Mistral
def get_embedding(text):
    try:
        response = requests.post(
            MISTRAL_ENDPOINT + "/api/embeddings",
            json={"model": "mistral", "prompt": text},
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        if "embedding" not in data:
            print(f"Error: 'embedding' key not found in response: {data}")
            # Fallback: Generate a dummy embedding for testing
            return np.random.rand(768).tolist()
        return data["embedding"]
    except requests.exceptions.RequestException as e:
        print(f"Error calling Mistral API: {e}")
        print(f"Response text: {response.text if 'response' in locals() else 'No response'}")
        # Fallback: Generate a dummy embedding for testing
        return np.random.rand(768).tolist()

# Function to store museum info in Qdrant
def store_museum_info(museum_data):
    create_collection()
    points = []
    for item in museum_data:
        embedding = get_embedding(json.dumps(item))
        point = PointStruct(
            id=str(uuid.uuid4()),
            vector=embedding,
            payload=item
        )
        points.append(point)
    qdrant_client.upsert(
        collection_name=COLLECTION_NAME,
        points=points
    )

# Function to query museum info
def query_museum(question):
    # Get embedding for the question
    question_embedding = get_embedding(question)
    
    # Search in Qdrant
    search_result = qdrant_client.search(
        collection_name=COLLECTION_NAME,
        query_vector=question_embedding,
        limit=3
    )
    
    # Prepare context from search results
    context = ""
    for point in search_result:
        context += json.dumps(point.payload) + "\n"
    
    # Query Mistral with context
    prompt = f"""Based on the following information:
{context}

Answer the question: {question}
Provide a concise and accurate response."""
    
    try:
        response = requests.post(
            MISTRAL_ENDPOINT + "/api/generate",
            json={
                "model": "mistral",
                "prompt": prompt,
                "max_tokens": 200
            },
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        return data.get("response", "No response from model").strip()
    except requests.exceptions.RequestException as e:
        print(f"Error calling Mistral API: {e}")
        return "Error: Could not get response from Mistral API."

# Example usage
if __name__ == "__main__":
    # Sample museum data
    museum_data = [
        {
            "museum": "Louvre Museum",
            "opening_hours": "9:00 AM - 6:00 PM, closed on Tuesdays",
            "things_to_see": "Mona Lisa, Venus de Milo, Winged Victory",
            "location": "75001 Paris, France",
            "contact": "+33 1 40 20 50 50",
            "access": "Metro: Palais-Royal Musée du Louvre",
            "shop": "Gift shop available on-site and online"
        },
        {
            "museum": "Metropolitan Museum of Art",
            "opening_hours": "10:00 AM - 5:00 PM, closed on Wednesdays",
            "things_to_see": "Egyptian Art, American Wing, Arms and Armor",
            "location": "1000 5th Ave, New York, NY 10028, USA",
            "contact": "+1 212-535-7710",
            "access": "Subway: 4, 5, embedding
            "shop": "Multiple gift shops and online store"
        }
    ]
    
    # Store data in Qdrant
    store_museum_info(museum_data)
    
    # Example questions
    questions = [
        "What are the opening hours of the Louvre Museum?",
        "What can I see at the Metropolitan Museum of Art?",
        "How do I contact the Louvre Museum?",
        "Where is the Metropolitan Museum of Art located?"
    ]
    
    # Get answers
    for question in questions:
        answer = query_museum(question)
        print(f"Q: {question}")
        print(f"A: {answer}\n")