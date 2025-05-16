import os
import logging
import time
from typing import Dict, List
import json
from openai import OpenAI
from pinecone import Pinecone
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("message_id_retriever")

# Load environment variables
load_dotenv()

class MessageIDRetriever:
    
    # Define index names as class constants
    GENERAL_INDEX = "email-general"
    SENDER_INDEX = "email-sender"
    TOPIC_INDEX = "email-topic"
    
    def __init__(self, openai_model: str = "gpt-4o") -> None:

        # Initialize API keys
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.pinecone_api_key = os.getenv("PINECONE_API_KEY")
        
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        if not self.pinecone_api_key:
            raise ValueError("PINECONE_API_KEY not found in environment variables")
        
        # Initialize clients
        self.openai_client = OpenAI(api_key=self.openai_api_key)
        self.pc = Pinecone(api_key=self.pinecone_api_key)
        
        # Store model configuration
        self.openai_model = openai_model
        self.embedding_model = "text-embedding-ada-002"  # This produces 1536-dimension vectors
        
        # Initialize index references
        self._init_indexes()
        
        # Cache for recent queries to improve performance
        self.query_cache = {}
        
        logger.info(f"MessageIDRetriever initialized with OpenAI model: {openai_model}")
    
    def _init_indexes(self) -> None:

        try:
            # Get handles to all three specialized indexes
            self.general_index = self.pc.Index(self.GENERAL_INDEX)
            self.sender_index = self.pc.Index(self.SENDER_INDEX)
            self.topic_index = self.pc.Index(self.TOPIC_INDEX)
            
            logger.info("Successfully connected to all Pinecone indexes")
        except Exception as e:
            logger.error(f"Failed to connect to Pinecone indexes: {str(e)}")
            raise
    
    def enhance_query(self, user_input: str) -> str:

        # Check if this query is in the cache
        if user_input in self.query_cache:
            return self.query_cache[user_input]
        
        try:
            prompt = f"""
            I need to find a specific email based on this vague description:
            "{user_input}"
            
            Please reformulate this into a detailed search query that would help find the right email.
            Include likely:
            - Sender information (if mentioned or implied)
            - Subject keywords (if mentioned or implied)
            - Time references (if mentioned or implied)
            - Content keywords (if mentioned or implied)
            
            Avoid adding speculative details that aren't implied in the original query.
            Return only the enhanced search query, nothing else.
            """
            
            response = self.openai_client.chat.completions.create(
                model=self.openai_model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that enhances email search queries."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=150
            )
            
            enhanced_query = response.choices[0].message.content.strip()
            
            # Store in cache
            self.query_cache[user_input] = enhanced_query
            
            logger.info(f"Enhanced query: '{user_input}' -> '{enhanced_query}'")
            return enhanced_query
            
        except Exception as e:
            logger.warning(f"Failed to enhance query, using original: {str(e)}")
            return user_input
    
    def create_embedding(self, text: str) -> List[float]:

        try:
            response = self.openai_client.embeddings.create(
                input=text,
                model=self.embedding_model
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Failed to create embedding: {str(e)}")
            raise
    
    def search_indexes(self, query_embedding: List[float], namespace: str, top_k: int = 5) -> Dict[str, List[Dict]]:

        results = {}
        
        try:
            # Search general index (for overall email context)
            general_results = self.general_index.query(
                vector=query_embedding,
                top_k=top_k,
                namespace=namespace,
                include_metadata=True
            )
            results["general"] = general_results.matches
            
            # Search sender index (for sender-focused queries)
            sender_results = self.sender_index.query(
                vector=query_embedding,
                top_k=top_k,
                namespace=namespace,
                include_metadata=True
            )
            results["sender"] = sender_results.matches
            
            # Search topic index (for subject/topic-focused queries)
            topic_results = self.topic_index.query(
                vector=query_embedding,
                top_k=top_k,
                namespace=namespace,
                include_metadata=True
            )
            results["topic"] = topic_results.matches
            
            logger.info(f"Successfully searched all indexes for namespace '{namespace}'")
            return results
            
        except Exception as e:
            logger.error(f"Failed to search indexes: {str(e)}")
            raise
    
    def score_results(self, results: Dict[str, List[Dict]], query: str) -> List[Dict]:

        candidates = {}
        
        # Process results from each index with different weights
        for index_name, matches in results.items():
            # Apply different weights based on index type
            if index_name == "general":
                weight = 1.0
            elif index_name == "sender":
                weight = 0.8
            else:  # topic
                weight = 0.9
            
            for match in matches:
                message_id = match.id
                score = match.score * weight
                
                if message_id not in candidates:
                    # Initialize new candidate
                    candidates[message_id] = {
                        "message_id": message_id,
                        "score": score,
                        "metadata": match.metadata,
                        "sources": [index_name]
                    }
                else:
                    # Update existing candidate
                    candidates[message_id]["score"] += score
                    candidates[message_id]["sources"].append(index_name)
                    
                    # Keep the most detailed metadata
                    if index_name == "general" and "general" not in candidates[message_id]["sources"]:
                        candidates[message_id]["metadata"] = match.metadata
        
        # Convert dictionary to list and sort by score
        sorted_candidates = sorted(
            candidates.values(),
            key=lambda x: x["score"],
            reverse=True
        )
        
        logger.info(f"Scored {len(sorted_candidates)} unique candidates")
        return sorted_candidates
    
    def validate_candidate(self, candidate: Dict, query: str) -> float:

        try:
            # Extract relevant metadata fields for validation
            metadata = candidate["metadata"]
            
            # Construct a detailed context from available metadata
            context = f"""
            Email Information:
            - Message ID: {candidate['message_id']}
            - Sender: {metadata.get('sender', 'Unknown')}
            - Subject: {metadata.get('subject', 'Unknown')}
            - Date/Time: {metadata.get('datetime', 'Unknown')}
            - Preview: {metadata.get('body_snippet', '')}
            """
            
            if "keywords" in metadata:
                context += f"- Keywords: {', '.join(metadata['keywords'])}\n"
                
            if "companies" in metadata:
                context += f"- Companies: {', '.join(metadata['companies'])}\n"
            
            prompt = f"""
            The user is looking for this email: "{query}"
            
            This is the information about a potential match:
            {context}
            
            On a scale from 0 to 1, how confident are you that this is the email the user is looking for?
            Provide ONLY a single number between 0 and 1, with 1 being completely confident and 0 being not at all confident.
            """
            
            response = self.openai_client.chat.completions.create(
                model=self.openai_model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that validates email matches."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=10
            )
            
            # Extract confidence score
            confidence = float(response.choices[0].message.content.strip())
            return min(max(confidence, 0.0), 1.0)  # Ensure score is between 0 and 1
            
        except Exception as e:
            logger.warning(f"Failed to validate candidate: {str(e)}")
            return 0.5  # Default to neutral confidence
    
    def rerank_results(self, candidates: List[Dict], original_query: str, enhanced_query: str) -> List[Dict]:

        if not candidates:
            return []
        
        # Only validate top candidates to save API calls
        top_candidates = candidates[:3]
        
        for candidate in top_candidates:
            # Validate with the original query (more authentic user intent)
            validation_score = self.validate_candidate(candidate, original_query)
            
            # Adjust the score based on validation
            candidate["validation_score"] = validation_score
            candidate["final_score"] = candidate["score"] * (0.5 + validation_score/2)
        
        # Resort based on final scores
        reranked = sorted(
            top_candidates,
            key=lambda x: x.get("final_score", 0.0),
            reverse=True
        )
        
        # Append the rest of the candidates
        reranked.extend(candidates[3:])
        
        logger.info(f"Reranked candidates, top score: {reranked[0].get('final_score', 0) if reranked else 0}")
        return reranked
    
    def analyze_query_intent(self, query: str) -> Dict[str, float]:

        try:
            prompt = f"""
            Analyze this email search query: "{query}"
            
            Determine the focus of this query based on these categories:
            - sender_focus: How much the query focuses on who sent the email
            - topic_focus: How much the query focuses on the subject or topic
            - recency_focus: How much the query implies recency is important
            - content_focus: How much the query focuses on email content
            
            Return a JSON object with scores from 0.0 to 1.0 for each category.
            Example: {{"sender_focus": 0.8, "topic_focus": 0.3, "recency_focus": 0.1, "content_focus": 0.5}}
            """
            
            response = self.openai_client.chat.completions.create(
                model=self.openai_model,
                messages=[
                    {"role": "system", "content": "You analyze email search queries and return JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            # Parse the JSON response
            intent_analysis = json.loads(response.choices[0].message.content)
            logger.info(f"Query intent analysis: {intent_analysis}")
            return intent_analysis
            
        except Exception as e:
            logger.warning(f"Failed to analyze query intent: {str(e)}")
            # Default to balanced weights
            return {
                "sender_focus": 0.5,
                "topic_focus": 0.5,
                "recency_focus": 0.5,
                "content_focus": 0.5
            }
    
    def apply_time_filter(self, candidates: List[Dict], recency_focus: float) -> List[Dict]:

        if recency_focus < 0.3 or not candidates:
            # Time is not important for this query
            return candidates
        
        try:
            # Apply time decay based on recency focus
            for candidate in candidates:
                metadata = candidate["metadata"]
                
                if "timestamp" in metadata:
                    # Get timestamp (assuming it's in seconds since epoch)
                    timestamp = metadata["timestamp"]
                    
                    if isinstance(timestamp, (int, float)):
                        # Current time in seconds since epoch
                        current_time = time.time()
                        
                        # Calculate days since email
                        days_old = (current_time - timestamp) / (60 * 60 * 24)
                        
                        # Apply exponential decay based on recency focus
                        # Higher recency_focus means faster decay with age
                        time_factor = max(0.3, pow(0.95, days_old * recency_focus * 10))
                        
                        # Adjust score
                        if "final_score" in candidate:
                            candidate["final_score"] *= time_factor
                        else:
                            candidate["score"] *= time_factor
            
            # Resort based on adjusted scores
            return sorted(
                candidates, 
                key=lambda x: x.get("final_score", x["score"]), 
                reverse=True
            )
            
        except Exception as e:
            logger.warning(f"Failed to apply time filter: {str(e)}")
            return candidates
    
    def get_message_id(self, user_input: str, user_namespace: str) -> str:

        try:
            start_time = time.time()
            logger.info(f"Processing request: '{user_input}' for namespace '{user_namespace}'")
            
            # Step 1: Enhance the query for better search results
            enhanced_query = self.enhance_query(user_input)
            
            # Step 2: Analyze query intent to determine search strategy
            intent_analysis = self.analyze_query_intent(user_input)
            
            # Step 3: Create embedding for the enhanced query
            query_embedding = self.create_embedding(enhanced_query)
            
            # Step 4: Search across all indexes
            search_results = self.search_indexes(query_embedding, user_namespace)
            
            # Step 5: Score and combine results
            candidates = self.score_results(search_results, enhanced_query)
            
            if not candidates:
                logger.warning("No candidates found across any indexes")
                return ""
            
            # Step 6: Rerank top results with validation
            reranked_candidates = self.rerank_results(candidates, user_input, enhanced_query)
            
            # Step 7: Apply time-based filtering if necessary
            final_candidates = self.apply_time_filter(
                reranked_candidates, 
                intent_analysis.get("recency_focus", 0.5)
            )
            
            # Step 8: Return the top result if it meets confidence threshold
            if final_candidates and (
                final_candidates[0].get("final_score", 0) > 0.6 or
                final_candidates[0].get("validation_score", 0) > 0.7
            ):
                top_candidate = final_candidates[0]
                message_id = top_candidate["message_id"]
                
                elapsed_time = time.time() - start_time
                logger.info(f"Successfully found message ID: {message_id} (in {elapsed_time:.2f}s)")
                
                # Log detailed information about the match
                logger.info(f"Match details: final_score={top_candidate.get('final_score', 'N/A')}, "
                            f"validation_score={top_candidate.get('validation_score', 'N/A')}, "
                            f"initial_score={top_candidate['score']}")
                
                return message_id
            else:
                # Not confident about any result
                logger.warning("No confident match found")
                return ""
                
        except Exception as e:
            logger.error(f"Error in get_message_id: {str(e)}")
            return ""
    
    def get_message_details(self, message_id: str, user_namespace: str) -> Dict:

        try:
            # Search for the exact message ID in the general index
            # This is a metadata filter operation, not a vector search
            vector = [0.0] * 1536  # Dummy vector, just for the API call
            
            results = self.general_index.query(
                vector=vector,
                top_k=1,
                namespace=user_namespace,
                include_metadata=True,
                filter={"message_id": {"$eq": message_id}}
            )
            
            if results.matches:
                return results.matches[0].metadata
            else:
                logger.warning(f"No details found for message ID: {message_id}")
                return {}
                
        except Exception as e:
            logger.error(f"Error in get_message_details: {str(e)}")
            return {}

    def batch_process_queries(self, queries: List[str], user_namespace: str) -> List[Dict]:

        results = []
        
        for query in queries:
            start_time = time.time()
            message_id = self.get_message_id(query, user_namespace)
            elapsed_time = time.time() - start_time
            
            results.append({
                "query": query,
                "message_id": message_id,
                "found": bool(message_id),
                "time_taken": f"{elapsed_time:.2f}s"
            })
        
        return results

# Example Usage
if __name__ == "__main__":

    # Example user query
    # user_query = "email from Jeremy about Vision Language Models"
    # user_query = "email from Contra about hiring at Plutio."
    user_query = "email from Sheenam about accommodation in Europe"
    user_namespace = "subhrastien"
    
    # Get the message ID
    retriever = MessageIDRetriever()
    message_id = retriever.get_message_id(user_query, user_namespace)
    print(f"message_id: {message_id}")
