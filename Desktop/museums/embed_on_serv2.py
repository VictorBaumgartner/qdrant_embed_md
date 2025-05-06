import os
import re
import uuid
import markdown
import time
import json
import requests
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, VectorParams, Distance
from typing import List, Dict, Tuple, Optional
from pathlib import Path

# Configuration
MARKDOWN_DIR = "./chateau-azay-le-ferron"
OLLAMA_API_BASE = "http://192.168.0.58:11434"  # Ollama API endpoint
OLLAMA_EMBED_MODEL = "nomic-embed-text:latest"  # Updated to match the available model
LOCAL_QDRANT_HOST = "localhost"
LOCAL_QDRANT_PORT = 6333  # Default Qdrant port
COLLECTION_NAME = "chateau-azay-le-ferron"
BATCH_SIZE = 100
GROUP_BY = "url_prefix"
MAX_RETRIES = 3
RETRY_DELAY = 2

def read_markdown_file(file_path: str) -> Tuple[str, Dict]:
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    html = markdown.markdown(content)
    text = re.sub(r'<[^>]+>', '', html)
    metadata = {
        "url": file_path.replace(MARKDOWN_DIR, "https://website.com").replace(".md", ""),
        "title": Path(file_path).stem.replace("-", " ").title(),
        "file_path": file_path
    }
    return text.strip(), metadata

def get_group_key(metadata: Dict) -> str:
    if GROUP_BY == "url_prefix":
        url_parts = metadata["url"].split("/")
        return "/".join(url_parts[3:4]) if len(url_parts) > 3 else "root"
    elif GROUP_BY == "directory":
        return str(Path(metadata["file_path"]).parent.relative_to(MARKDOWN_DIR))
    return "default"

def generate_ollama_embedding(text: str) -> List[float]:
    """Generate embedding for a single text using Ollama API"""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                f"{OLLAMA_API_BASE}/api/embeddings",
                json={"model": OLLAMA_EMBED_MODEL, "prompt": text},
                timeout=60  # Allow up to 60 seconds for the embedding to generate
            )
            response.raise_for_status()
            result = response.json()
            return result["embedding"]
        except Exception as e:
            print(f"Attempt {attempt+1} failed to get embedding: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Max retries reached. Could not get embedding from Ollama server at {OLLAMA_API_BASE}")
                raise
    return []

def generate_embeddings_batch(texts: List[str]) -> List[List[float]]:
    """Generate embeddings for a batch of texts using Ollama API"""
    embeddings = []
    for i, text in enumerate(texts):
        try:
            print(f"Generating embedding for text {i+1}/{len(texts)}...")
            embedding = generate_ollama_embedding(text)
            embeddings.append(embedding)
        except Exception as e:
            print(f"Failed to generate embedding for text {i+1}: {e}")
            # Use a zero vector as a fallback (same size as successful embeddings)
            if embeddings:
                embeddings.append([0.0] * len(embeddings[0]))
            else:
                # If we haven't generated any embeddings yet, we can't determine the size
                raise
    return embeddings

def store_in_qdrant(points: List[PointStruct], client: QdrantClient) -> bool:
    for attempt in range(MAX_RETRIES):
        try:
            if not client.collection_exists(COLLECTION_NAME):
                print(f"Collection '{COLLECTION_NAME}' does not exist. Cannot store points.")
                return False
            client.upsert(collection_name=COLLECTION_NAME, points=points)
            return True
        except Exception as e:
            print(f"Attempt {attempt+1} failed to store points: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print("Max retries reached. Could not store points.")
                return False
    return False

def connect_to_local_qdrant() -> Optional[QdrantClient]:
    print(f"Attempting to connect to local Qdrant at {LOCAL_QDRANT_HOST}:{LOCAL_QDRANT_PORT}")
    for attempt in range(MAX_RETRIES):
        try:
            client = QdrantClient(host=LOCAL_QDRANT_HOST, port=LOCAL_QDRANT_PORT)
            client.get_collections()
            print("Successfully connected to local Qdrant")
            return client
        except Exception as e:
            print(f"Connection attempt {attempt+1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"""
---------------------------------------------------------
ERROR: Cannot connect to local Qdrant server at {LOCAL_QDRANT_HOST}:{LOCAL_QDRANT_PORT}
---------------------------------------------------------
Please make sure that:
1. Qdrant server is running on your local machine
2. The server is accessible at {LOCAL_QDRANT_HOST} on port {LOCAL_QDRANT_PORT}
3. There are no firewall restrictions blocking the connection
---------------------------------------------------------
""")
                return None
    return None

def create_collection_if_not_exists(client: QdrantClient, vector_size: int) -> bool:
    try:
        if not client.collection_exists(COLLECTION_NAME):
            print(f"Creating collection '{COLLECTION_NAME}'")
            client.create_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE)
            )
            print(f"Collection '{COLLECTION_NAME}' created with vector size {vector_size}")
        else:
            print(f"Collection '{COLLECTION_NAME}' already exists")
        return True
    except Exception as e:
        print(f"Failed to create collection: {e}")
        return False

def test_ollama_connection() -> Optional[int]:
    """Test connection to the Ollama API and return the vector size"""
    try:
        print(f"Testing connection to Ollama API at {OLLAMA_API_BASE}...")
        
        # Test Ollama API availability
        response = requests.get(f"{OLLAMA_API_BASE}/api/tags")
        response.raise_for_status()
        tags = response.json()
        print(f"Ollama server is running with models: {[model['name'] for model in tags.get('models', [])]}")
        
        # Test embedding generation with a simple text
        test_text = "This is a test."
        print(f"Testing embedding generation with model '{OLLAMA_EMBED_MODEL}'...")
        embedding = generate_ollama_embedding(test_text)
        
        if embedding:
            vector_size = len(embedding)
            print(f"Successfully generated embedding. Vector size: {vector_size}")
            return vector_size
        else:
            print("Embedding generation returned empty result")
            return None
    except requests.exceptions.ConnectionError as e:
        print(f"Failed to connect to Ollama API at {OLLAMA_API_BASE}: {e}")
        print("Please ensure:")
        print(f"1. Ollama is running on the server at {OLLAMA_API_BASE}")
        print(f"2. The model '{OLLAMA_EMBED_MODEL}' is available (run 'ollama pull {OLLAMA_EMBED_MODEL}' on the server)")
        print("3. The API endpoint is accessible from your machine")
        return None
    except Exception as e:
        print(f"Error testing Ollama connection: {e}")
        return None

def main():
    # Test Ollama API connection
    vector_size = test_ollama_connection()
    if vector_size is None:
        print("Exiting due to Ollama API connection failure.")
        return

    # Connect to local Qdrant
    client = connect_to_local_qdrant()
    if client is None:
        print("Exiting due to local Qdrant connection failure.")
        return

    # Create collection if needed
    if not create_collection_if_not_exists(client, vector_size):
        print("Exiting due to collection creation failure.")
        return

    print(f"Scanning for markdown files in '{MARKDOWN_DIR}'...")
    md_files = list(Path(MARKDOWN_DIR).rglob("*.md"))
    print(f"Found {len(md_files)} markdown files")

    if not md_files:
        print(f"No markdown files found in '{MARKDOWN_DIR}'. Please check the directory path.")
        return

    points = []
    successful_files = 0
    failed_files = 0

    for i, file_path in enumerate(md_files):
        try:
            text, metadata = read_markdown_file(str(file_path))
            if not text:
                print(f"Skipping empty file: {file_path}")
                continue

            group_key = get_group_key(metadata)
            point_id = str(uuid.uuid4())

            point = {
                "id": point_id,
                "text": text,
                "metadata": {
                    **metadata,
                    "group": group_key
                }
            }
            points.append(point)

            if len(points) >= BATCH_SIZE or i == len(md_files) - 1:
                print(f"Processing batch of {len(points)} documents...")
                texts = [p["text"] for p in points]

                print(f"Getting embeddings from Ollama API at {OLLAMA_API_BASE}...")
                embeddings = generate_embeddings_batch(texts)

                qdrant_points = [
                    PointStruct(
                        id=p["id"],
                        vector=embedding,
                        payload={
                            **p["metadata"],
                            "text": p["text"][:1000]
                        }
                    )
                    for p, embedding in zip(points, embeddings)
                ]

                print(f"Storing {len(qdrant_points)} points in local Qdrant...")
                if store_in_qdrant(qdrant_points, client):
                    successful_files += len(points)
                    print(f"Successfully processed batch {i // BATCH_SIZE + 1}: {len(points)} points")
                else:
                    failed_files += len(points)
                    print(f"Failed to process batch {i // BATCH_SIZE + 1}")

                points = []

            if (i + 1) % 10 == 0 or i == len(md_files) - 1:
                progress = (i + 1) / len(md_files) * 100
                print(f"Progress: {progress:.1f}% ({i + 1}/{len(md_files)} files)")

        except Exception as e:
            failed_files += 1
            print(f"Error processing {file_path}: {e}")

    print("\n" + "="*50)
    print("Processing complete!")
    print(f"Successfully processed: {successful_files} files")
    print(f"Failed to process: {failed_files} files")
    print("="*50)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")