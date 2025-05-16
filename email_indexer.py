# Updated Mechanism to Upload Email Data to Pinecone
# email_indexer.py

"""
Email Indexer - Pre-processes and uploads emails to Pinecone

This script:
1. Parses raw email JSON data
2. Extracts structured fields and creates vectors
3. Organizes data into Pinecone indexes with namespace-based user separation
4. Creates lookup tables for direct access

Usage:
    python email_indexer.py --data_path emails.json --user_id username
"""

import json
import re
from typing import Dict, List, Any

from openai import OpenAI
from pinecone import Pinecone
from pinecone import ServerlessSpec
from dotenv import load_dotenv
import os
from html2text import html2text

# Load environment variables
load_dotenv()

# Initialize API keys
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")

# Initialize clients
client = OpenAI(api_key=OPENAI_API_KEY)
pc = Pinecone(api_key=PINECONE_API_KEY)

# Define constants for index names
GENERAL_INDEX_NAME = "email-general"
SENDER_INDEX_NAME = "email-sender"
TOPIC_INDEX_NAME = "email-topic"

class EmailProcessor:
    """
    Processes raw email data for Pinecone indexing
    """
    
    def __init__(self, embedding_dimension: int = 1536):
        """
        Initialize the email processor
        
        Args:
            embedding_dimension: Dimension of OpenAI embeddings
        """
        self.embedding_dimension = embedding_dimension
        
        # Ensure Pinecone is initialized
        if not PINECONE_API_KEY:
            raise ValueError("PINECONE_API_KEY not found in environment variables")
    
    def extract_text_from_html(self, html_content: str) -> str:
        """
        Extract plain text from HTML content
        
        Args:
            html_content: HTML string
            
        Returns:
            Plain text extracted from HTML
        """
        if not html_content:
            return ""
        
        # Use html2text to convert HTML to readable text
        text = html2text(html_content)
        
        # Clean up the text
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        return text
    
    def extract_entities_and_info(self, text: str) -> Dict[str, Any]:
        """
        Extract entities and information from text using OpenAI
        
        Args:
            text: Input text
            
        Returns:
            Dictionary with extracted entities and information
        """
        if not text or len(text) < 10:
            return {
                "entities": [],
                "keywords": [],
                "companies": []
            }
        
        # Truncate text to avoid token limits
        truncated_text = text[:4000]
        
        prompt = f"""
        Extract the following information from this email text in JSON format:
        
        1. A list of important entities (people, organizations, products, locations)
        2. A list of up to 10 keywords that best represent the content
        3. A list of company names mentioned
        
        Text:
        {truncated_text}
        
        Return only valid JSON with these keys: "entities", "keywords", "companies"
        """
        
        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo-0125",
                messages=[
                    {"role": "system", "content": "You extract structured information from email text."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            return {
                "entities": result.get("entities", []),
                "keywords": result.get("keywords", []),
                "companies": result.get("companies", [])
            }
        except Exception as e:
            print(f"Error extracting entities: {str(e)}")
            return {
                "entities": [],
                "keywords": [],
                "companies": []
            }
    
    def create_embedding(self, text: str) -> List[float]:
        """
        Create embedding vector for text using OpenAI
        
        Args:
            text: Input text
            
        Returns:
            Embedding vector
        """
        response = client.embeddings.create(
            input=text,
            model="text-embedding-ada-002"
        )
        return response.data[0].embedding
    
    def preprocess_emails(self, raw_email_data: List[Dict]) -> List[Dict]:
        """
        Process raw email data into structured format
        
        Args:
            raw_email_data: List of raw email JSON objects
            
        Returns:
            List of processed email objects
        """
        processed_emails = []
        
        for thread in raw_email_data:
            thread_id = thread.get("thread_id")
            
            for message in thread.get("messages", [])[:2]: # Remove the [:2] after dev
                # Convert HTML body to plain text
                body_text = self.extract_text_from_html(message.get("body", ""))
                body_snippet = body_text[:500]  # Create a snippet
                
                # Extract entities and keywords
                all_text = f"{message.get('subject', '')} {body_snippet}"
                extracted_info = self.extract_entities_and_info(all_text)
                
                # Create normalized email object
                normalized_email = {
                    "message_id": message.get("message_id", ""),
                    "thread_id": thread_id,
                    "timestamp": message.get("timestamp", 0),
                    "datetime": message.get("datetime", ""),
                    "sender": message.get("sender", "").lower(),
                    "sender_domain": message.get("sender", "").split('@')[-1] if '@' in message.get("sender", "") else "",
                    "receiver": message.get("receiver", "").lower(),
                    "subject": message.get("subject", ""),
                    "body_snippet": body_snippet,
                    "entities": extracted_info["entities"],
                    "keywords": extracted_info["keywords"],
                    "companies": extracted_info["companies"],
                    "labels": message.get("labels", []),
                    "references": message.get("references", []),
                    "in_reply_to": message.get("in_reply_to", ""),
                    "has_attachments": "attachment" in body_text.lower(),
                    "full_body": body_text
                }
                
                processed_emails.append(normalized_email)
                
                # Log progress
                print(f"Processed email: {normalized_email['message_id']}")
        
        return processed_emails
    
    def generate_email_vectors(self, processed_emails: List[Dict]) -> List[Dict]:
        """
        Generate vector representations for processed emails
        
        Args:
            processed_emails: List of processed email objects
            
        Returns:
            List of emails with vector representations
        """
        vectorized_emails = []
        
        for email in processed_emails:
            # Create primary search vector
            primary_text = f"Email subject: {email['subject']} from: {email['sender']} content: {email['body_snippet']}"
            primary_vector = self.create_embedding(primary_text)
            
            # Create sender-focused vector
            sender_text = f"Email from {email['sender']} sent by {email['sender']} from {email['sender_domain']}"
            sender_vector = self.create_embedding(sender_text)
            
            # Create topic-focused vector
            topic_keywords = " ".join(email['keywords'])
            topic_text = f"Email about {email['subject']} discussing {topic_keywords}"
            topic_vector = self.create_embedding(topic_text)
            
            # Add vectors to email
            vectorized_email = {
                "id": email["message_id"],
                "vectors": {
                    "primary": primary_vector,
                    "sender": sender_vector,
                    "topic": topic_vector
                },
                "metadata": email
            }
            
            vectorized_emails.append(vectorized_email)
            
            # Log progress
            print(f"Generated vectors for email: {email['message_id']}")
        
        print(f"Generated vectors for {len(vectorized_emails)} emails")
        return vectorized_emails
    
    def ensure_pinecone_indexes_exist(self):
        """
        Create shared Pinecone indexes if they don't exist
        """
        # Check if indexes already exist
        existing_indexes = pc.list_indexes().names()
        
        # Create general index if it doesn't exist
        if GENERAL_INDEX_NAME not in existing_indexes:
            pc.create_index(
                name=GENERAL_INDEX_NAME,
                dimension=self.embedding_dimension,
                metric="cosine",
                spec=ServerlessSpec(
                    cloud="aws",
                    region="us-east-1"
                )
            )
            print(f"Created index: {GENERAL_INDEX_NAME}")
        
        # Create sender index if it doesn't exist
        if SENDER_INDEX_NAME not in existing_indexes:
            pc.create_index(
                name=SENDER_INDEX_NAME,
                dimension=self.embedding_dimension,
                metric="cosine",
                spec=ServerlessSpec(
                    cloud="aws",
                    region="us-east-1"
                )
            )
            print(f"Created index: {SENDER_INDEX_NAME}")
        
        # Create topic index if it doesn't exist
        if TOPIC_INDEX_NAME not in existing_indexes:
            pc.create_index(
                name=TOPIC_INDEX_NAME,
                dimension=self.embedding_dimension,
                metric="cosine",
                spec=ServerlessSpec(
                    cloud="aws",
                    region="us-east-1"
                )
            )
            print(f"Created index: {TOPIC_INDEX_NAME}")

    def upload_to_pinecone(self, vectorized_emails: List[Dict], user_id: str):
        """
        Upload vectorized emails to Pinecone indexes using user-specific namespaces
        
        Args:
            vectorized_emails: List of emails with vector representations
            user_id: User identifier for namespace
        """
        # Generate user-specific namespace
        namespace = f"{user_id}"
        
        # Ensure shared indexes exist
        self.ensure_pinecone_indexes_exist()
        
        # Get indexes
        general_index = pc.Index(GENERAL_INDEX_NAME)
        sender_index = pc.Index(SENDER_INDEX_NAME)
        topic_index = pc.Index(TOPIC_INDEX_NAME)
        
        # Prepare vectors for each index
        general_vectors = []
        sender_vectors = []
        topic_vectors = []
        
        for email in vectorized_emails:
            # Add to general index
            general_vectors.append({
                "id": email["id"],
                "values": email["vectors"]["primary"],
                "metadata": {
                    "message_id": email["metadata"]["message_id"],
                    "thread_id": email["metadata"]["thread_id"],
                    "sender": email["metadata"]["sender"],
                    "subject": email["metadata"]["subject"],
                    "datetime": email["metadata"]["datetime"],
                    "timestamp": email["metadata"]["timestamp"],
                    "body_snippet": email["metadata"]["body_snippet"]
                }
            })
            
            # Add to sender index
            sender_vectors.append({
                "id": email["id"],
                "values": email["vectors"]["sender"],
                "metadata": {
                    "message_id": email["metadata"]["message_id"],
                    "sender": email["metadata"]["sender"],
                    "sender_domain": email["metadata"]["sender_domain"],
                    "subject": email["metadata"]["subject"],
                    "timestamp": email["metadata"]["timestamp"]
                }
            })
            
            # Add to topic index
            topic_vectors.append({
                "id": email["id"],
                "values": email["vectors"]["topic"],
                "metadata": {
                    "message_id": email["metadata"]["message_id"],
                    "subject": email["metadata"]["subject"],
                    "keywords": email["metadata"]["keywords"][:10],  # Limit keywords
                    "companies": email["metadata"]["companies"],
                    "timestamp": email["metadata"]["timestamp"]
                }
            })
        
        # Upload to indexes in batches with user namespace
        batch_size = 100
        
        # Upload to general index
        for i in range(0, len(general_vectors), batch_size):
            batch = general_vectors[i:i+batch_size]
            general_index.upsert(vectors=batch, namespace=namespace)
            print(f"Uploaded batch to general index: {i}-{i+len(batch)}")
        
        # Upload to sender index
        for i in range(0, len(sender_vectors), batch_size):
            batch = sender_vectors[i:i+batch_size]
            sender_index.upsert(vectors=batch, namespace=namespace)
            print(f"Uploaded batch to sender index: {i}-{i+len(batch)}")
        
        # Upload to topic index
        for i in range(0, len(topic_vectors), batch_size):
            batch = topic_vectors[i:i+batch_size]
            topic_index.upsert(vectors=batch, namespace=namespace)
            print(f"Uploaded batch to topic index: {i}-{i+len(batch)}")
        
        print(f"Uploaded {len(general_vectors)} vectors to Pinecone indexes using namespace: {namespace}")
    
    def create_lookup_tables(self, processed_emails: List[Dict], user_id: str):
        """
        Create lookup tables for direct access
        
        Args:
            processed_emails: List of processed email objects
            user_id: User identifier for organizing lookup tables
        """
        # This would typically be done with a database like Redis
        # For this example, we'll save to JSON files
        
        # Message ID lookup
        message_id_lookup = {}
        # Sender lookup
        sender_lookup = {}
        # Subject keyword lookup
        keyword_lookup = {}
        # Company lookup
        company_lookup = {}
        
        for email in processed_emails:
            # Message ID lookup
            message_id_lookup[email["message_id"]] = {
                "thread_id": email["thread_id"],
                "sender": email["sender"],
                "subject": email["subject"],
                "datetime": email["datetime"],
                "timestamp": email["timestamp"]
            }
            
            # Sender lookup
            if email["sender"] not in sender_lookup:
                sender_lookup[email["sender"]] = []
            sender_lookup[email["sender"]].append({
                "message_id": email["message_id"],
                "subject": email["subject"],
                "datetime": email["datetime"],
                "timestamp": email["timestamp"]
            })
            
            # Domain lookup (part of sender lookup)
            domain = email["sender_domain"]
            if domain not in sender_lookup:
                sender_lookup[domain] = []
            sender_lookup[domain].append({
                "message_id": email["message_id"],
                "sender": email["sender"],
                "subject": email["subject"],
                "datetime": email["datetime"],
                "timestamp": email["timestamp"]
            })
            
            # Keyword lookup
            for keyword in email["keywords"]:
                if keyword not in keyword_lookup:
                    keyword_lookup[keyword] = []
                keyword_lookup[keyword].append({
                    "message_id": email["message_id"],
                    "sender": email["sender"],
                    "subject": email["subject"],
                    "datetime": email["datetime"],
                    "timestamp": email["timestamp"]
                })
            
            # Company lookup
            for company in email["companies"]:
                if company not in company_lookup:
                    company_lookup[company] = []
                company_lookup[company].append({
                    "message_id": email["message_id"],
                    "sender": email["sender"],
                    "subject": email["subject"],
                    "datetime": email["datetime"],
                    "timestamp": email["timestamp"]
                })
        
        # Create directories for user-specific lookups
        lookup_dir = f"lookups/user_{user_id}"
        os.makedirs(lookup_dir, exist_ok=True)
        
        # Save lookup tables to files
        with open(f"{lookup_dir}/message_ids.json", "w") as f:
            json.dump(message_id_lookup, f)
        
        with open(f"{lookup_dir}/senders.json", "w") as f:
            json.dump(sender_lookup, f)
        
        with open(f"{lookup_dir}/keywords.json", "w") as f:
            json.dump(keyword_lookup, f)
        
        with open(f"{lookup_dir}/companies.json", "w") as f:
            json.dump(company_lookup, f)
        
        print(f"Created lookup tables in {lookup_dir}/")
    
    def process_and_upload(self, data_path: str, user_id: str):
        """
        Main function to process and upload emails
        
        Args:
            data_path: Path to raw email JSON data
            user_id: User identifier for organizing data
        """
        print(f"Processing email data from {data_path} for user {user_id}...")
        
        # Load raw email data
        with open(data_path, "r", encoding="utf-8") as f:
            raw_email_data = json.load(f)
        
        # Process emails
        print("Extracting structured data from emails...")
        processed_emails = self.preprocess_emails(raw_email_data)
        
        # Generate vectors
        print("Generating vector embeddings...")
        vectorized_emails = self.generate_email_vectors(processed_emails)
        
        # Upload to Pinecone with user namespace
        print(f"Uploading to Pinecone for user {user_id}...")
        self.upload_to_pinecone(vectorized_emails, user_id)
        
        # Create lookup tables
        print("Creating lookup tables...")
        self.create_lookup_tables(processed_emails, user_id)
        
        print("Email processing and uploading complete!")

def index_emails(data_path: str, user_id: str):
    """
    Main function to index emails from raw data
    
    Args:
        data_path: Path to raw email JSON data
        user_id: User identifier for organizing data
    """
    processor = EmailProcessor()
    processor.process_and_upload(data_path, user_id)


if __name__ == "__main__":
    
    data_path= "subhrastien@gmail.com.json"
    user_id = "subhrastien"
    
    index_emails(data_path, user_id)
