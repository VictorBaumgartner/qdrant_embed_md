import os
import re
import uuid
import markdown
import time
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, VectorParams, Distance
from transformers import AutoTokenizer, AutoModel
import torch
from typing import List, Dict, Tuple, Optional
from pathlib import Path

# Configuration
MARKDOWN_DIR = "./abbaye-arthous-landes"
QDRANT_HOST = "192.168.0.58"
QDRANT_PORT = 11434
COLLECTION_NAME = "abbaye-arthous-landes"
BATCH_SIZE = 100
EMBEDDING_MODEL = "nomic-embed-text-v1"
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

def generate_embeddings(texts: List[str], model, tokenizer) -> List[List[float]]:
    encoded_input = tokenizer(texts, padding=True, truncation=True, return_tensors='pt')
    with torch.no_grad():
        model_output = model(**encoded_input)
    embeddings = model_output.last_hidden_state.mean(dim=1)
    return embeddings.numpy().tolist()

def store_in_qdrant(points: List[PointStruct], client: QdrantClient) -> bool:
    for attempt in range(MAX_RETRIES):
        try:
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

def connect_to_qdrant() -> Optional[QdrantClient]:
    print(f"Attempting to connect to Qdrant at {QDRANT_HOST}:{QDRANT_PORT}")
    for attempt in range(MAX_RETRIES):
        try:
            client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
            client.get_collections()
            print("Successfully connected to Qdrant")
            return client
        except Exception as e:
            print(f"Connection attempt {attempt+1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"""
---------------------------------------------------------
ERROR: Cannot connect to Qdrant server at {QDRANT_HOST}:{QDRANT_PORT}
---------------------------------------------------------
Please make sure that:
1. Qdrant server is running
2. The server is accessible at {QDRANT_HOST} on port {QDRANT_PORT}
3. There are no firewall restrictions blocking the connection
---------------------------------------------------------
""")
                return None
    return None

def create_collection_if_not_exists(client: QdrantClient, model, tokenizer) -> bool:
    try:
        if not client.collection_exists(COLLECTION_NAME):
            print(f"Creating collection '{COLLECTION_NAME}'")
            dummy_text = ["This is a test."]
            embedding = generate_embeddings(dummy_text, model, tokenizer)[0]
            vector_size = len(embedding)
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

def main():
    client = connect_to_qdrant()
    if client is None:
        print("Exiting due to connection failure.")
        return

    try:
        print(f"Loading embedding model '{EMBEDDING_MODEL}'...")
        tokenizer = AutoTokenizer.from_pretrained("nomic-ai/nomic-embed-text-v1", trust_remote_code=True)
        model = AutoModel.from_pretrained("nomic-ai/nomic-embed-text-v1", trust_remote_code=True)
        model.eval()
        print("Model loaded successfully")
    except Exception as e:
        print(f"Failed to load embedding model: {e}")
        return

    if not create_collection_if_not_exists(client, model, tokenizer):
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

                print("Generating embeddings...")
                embeddings = generate_embeddings(texts, model, tokenizer)

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

                print(f"Storing {len(qdrant_points)} points in Qdrant...")
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