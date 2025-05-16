import os
import json
import datetime
from typing import List, Dict, Any, Optional
from openai import OpenAI
from pinecone import Pinecone
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize API keys
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "cold-opportunities")

# Initialize clients
openai_client = OpenAI(api_key=OPENAI_API_KEY)
pc = Pinecone(api_key=PINECONE_API_KEY)

class EmailVectorizer:
    
    def __init__(self, embedding_dimension: int = 1536):

        self.embedding_dimension = embedding_dimension
        self.index_name = PINECONE_INDEX_NAME
        
        # Check if index exists, create if it doesn't
        existing_indexes = pc.list_indexes().names()
        
        if self.index_name not in existing_indexes:
            print(f"Index '{self.index_name}' does not exist. Available indexes: {existing_indexes}")
            if not existing_indexes:
                raise ValueError("No indexes available. Please free up space in your Pinecone account.")
            
            # Use the first available index instead
            self.index_name = existing_indexes[0]
            print(f"Using existing index: {self.index_name}")
        else:
            print(f"Using existing Pinecone index: {self.index_name}")
        
        # Connect to the index
        self.index = pc.Index(self.index_name)
    
    def _generate_embedding(self, text: str) -> List[float]:

        # Truncate text if too long (OpenAI has token limits)
        if len(text) > 8000:
            text = text[:8000]
            
        response = openai_client.embeddings.create(
            input=text,
            model="text-embedding-ada-002"
        )
        
        return response.data[0].embedding
    
    def _prepare_email_for_embedding(self, email_thread: Dict[str, Any]) -> str:

        subject = email_thread.get("subject", "")
        
        # Combine all message bodies in the thread
        message_bodies = [msg.get("body", "") for msg in email_thread.get("messages", [])]
        combined_text = f"Subject: {subject}\n\n" + "\n\n".join(message_bodies)
        
        return combined_text
    
    def _check_if_promotional(self, email_thread: Dict[str, Any]) -> bool:

        # Common indicators of promotional emails
        promotional_keywords = [
            "unsubscribe", "opt-out", "view in browser", "view as webpage",
            "privacy policy", "terms of service", "update your preferences",
            "email marketing", "newsletter", "special offer", "discount", 
            "promotion", "limited time", "sale", "subscribe"
        ]
        
        # Check subject and message bodies
        subject = email_thread.get("subject", "").lower()
        
        # If only 1 message in thread, likely promotional
        if email_thread.get("total_number_of_emails_in_thread", 0) <= 1:
            all_text = subject + " " + " ".join([msg.get("body", "").lower() for msg in email_thread.get("messages", [])])
            
            # Check for common promotional indicators
            for keyword in promotional_keywords:
                if keyword in all_text:
                    return True
        
        return False
    
    def process_emails(
        self, 
        email_data_file: str,
        user_id: str = "default_user",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        batch_size: int = 100,
        skip_promotional: bool = True
    ) -> int:

        # Parse date filters if provided
        start_date_obj = None
        end_date_obj = None
        
        if start_date:
            start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        
        if end_date:
            end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d")
            # Set to end of day
            end_date_obj = end_date_obj.replace(hour=23, minute=59, second=59)
        
        print(f"Processing email data from: {email_data_file}")
        
        # Load email data - using utf-8 encoding with error handling
        try:
            with open(email_data_file, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
                # Find the start of the JSON array if there's a header
                json_start = content.find('[')
                if json_start == -1:
                    # Try parsing the whole file as JSON
                    email_data = json.loads(content)
                else:
                    # Parse from the start of the JSON array
                    email_data = json.loads(content[json_start:])
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            # Try alternative approaches
            try:
                print("Attempting to read file as binary and decode...")
                with open(email_data_file, 'rb') as f:
                    content = f.read().decode('utf-8', errors='replace')
                    json_start = content.find('[')
                    if json_start == -1:
                        email_data = json.loads(content)
                    else:
                        email_data = json.loads(content[json_start:])
                print("Successfully loaded JSON using binary mode")
            except Exception as e2:
                print(f"All attempts to read JSON failed: {e2}")
                return 0
        except FileNotFoundError:
            print(f"File not found: {email_data_file}")
            return 0
        except Exception as e:
            print(f"Unexpected error reading file: {e}")
            return 0
        
        print(f"Loaded {len(email_data)} email threads from file")
        
        # Filter by date if needed
        if start_date_obj or end_date_obj:
            filtered_data = []
            for thread in email_data:
                # Check if date_time field exists and has the expected format
                if "date_time" in thread:
                    try:
                        thread_date = datetime.datetime.strptime(thread.get("date_time", ""), "%Y-%m-%d %H:%M:%S %Z")
                        
                        if start_date_obj and thread_date < start_date_obj:
                            continue
                            
                        if end_date_obj and thread_date > end_date_obj:
                            continue
                            
                        filtered_data.append(thread)
                    except ValueError:
                        # If date parsing fails, include the thread anyway
                        filtered_data.append(thread)
                else:
                    # If no date_time field, include the thread anyway
                    filtered_data.append(thread)
            
            email_data = filtered_data
            print(f"Filtered to {len(email_data)} email threads within date range")
        
        # Process in batches
        processed_count = 0
        batch_vectors = []
        
        for i, thread in enumerate(email_data):
            # Skip promotional emails if requested
            if skip_promotional and self._check_if_promotional(thread):
                continue
            
            # Extract date from the thread if available
            thread_date = datetime.datetime.now()
            if "date_time" in thread:
                try:
                    thread_date = datetime.datetime.strptime(thread.get("date_time", ""), "%Y-%m-%d %H:%M:%S %Z")
                except ValueError:
                    pass
            
            # Generate a unique ID for the thread
            thread_id = f"email_{thread_date.strftime('%Y%m%d%H%M%S')}_{i}"
            
            # Prepare email content for embedding
            email_text = self._prepare_email_for_embedding(thread)
            
            # Generate embedding
            embedding = self._generate_embedding(email_text)
            
            # Create metadata for retrieval
            metadata = {
                "subject": thread.get("subject", ""),
                "date_time": thread.get("date_time", ""),
                "thread_length": thread.get("total_number_of_emails_in_thread", 0),
                "id": thread_id,
                # Store a truncated version of the first message for quick preview
                "preview": thread.get("messages", [{}])[0].get("body", "")[:200] + "..." if thread.get("messages") else "",
                # Store the full thread data as JSON string for retrieval
                "thread_data": json.dumps(thread)
            }
            
            # Add to batch
            batch_vectors.append({
                "id": thread_id,
                "values": embedding,
                "metadata": metadata
            })
            
            processed_count += 1
            
            # Upload batch if it reaches the batch size
            if len(batch_vectors) >= batch_size:
                self.index.upsert(vectors=batch_vectors, namespace=user_id)
                print(f"Uploaded batch of {len(batch_vectors)} vectors to Pinecone")
                batch_vectors = []
        
        # Upload any remaining vectors
        if batch_vectors:
            self.index.upsert(vectors=batch_vectors, namespace=user_id)
            print(f"Uploaded final batch of {len(batch_vectors)} vectors to Pinecone")
        
        print(f"Successfully processed and stored {processed_count} email threads")
        return processed_count


def vectorize_emails(
    email_data_file: str,
    user_id: str = "default_user",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> int:

    vectorizer = EmailVectorizer()
    return vectorizer.process_emails(
        email_data_file=email_data_file,
        user_id=user_id,
        start_date=start_date,
        end_date=end_date
    )


if __name__ == "__main__":
    email_data_file = "database/transform_subhraturning@gmail.com.json"
    user_id = "subhraturning"
    vectorize_emails(email_data_file, user_id)
