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

class ProspectFinder:

    def __init__(self, model: str = "gpt-4o", user_id: str = "default_user"):

        self.model = model
        self.user_id = user_id
        
        # Connect to Pinecone index
        self.index = pc.Index(PINECONE_INDEX_NAME)
    
    def _generate_opportunity_query_embedding(self) -> List[float]:

        query = """
            Find conversations that show signs of business interest or opportunity, 
            but which stopped without a clear resolution. These emails should have:
            - Initial expression of interest in collaboration, product, or service
            - Questions about pricing, capabilities, or specifications
            - Discussion of potential partnership or business relationship
            - No clear conclusion, like a rejection or acceptance
            - Last message was not recently responded to
        """
        
        response = openai_client.embeddings.create(
            input=query,
            model="text-embedding-ada-002"
        )
        
        return response.data[0].embedding
    
    def find_prospects(self, top_k: int = 10, min_thread_length: int = 2) -> List[Dict[str, Any]]:

        # Generate query embedding for semantic search
        query_embedding = self._generate_opportunity_query_embedding()
        
        # Create filter for thread length
        filter_dict = {
            "thread_length": {"$gte": min_thread_length}
        }
        
        # Query Pinecone for similar vectors in the user's namespace
        results = self.index.query(
            vector=query_embedding,
            top_k=top_k * 2,  # Fetch extra results for filtering
            include_metadata=True,
            filter=filter_dict,
            namespace=self.user_id
        )
        
        # Process the matches
        opportunity_candidates = []
        
        for match in results.matches:
            # Parse the thread data from metadata
            thread_data = json.loads(match.metadata["thread_data"])
            
            # Create opportunity object
            opportunity = {
                "id": match.id,
                "subject": match.metadata["subject"],
                "date_time": match.metadata["date_time"],
                "thread_length": match.metadata["thread_length"],
                "preview": match.metadata["preview"],
                "similarity_score": match.score,
                "thread_data": thread_data
            }
            
            opportunity_candidates.append(opportunity)
        
        # Sort by similarity score
        opportunity_candidates.sort(key=lambda x: x["similarity_score"], reverse=True)
        candidates = opportunity_candidates[:top_k]
        
        # Analyze each candidate with GPT
        analyzed_prospects = []
        
        for candidate in candidates:
            analysis = self._analyze_with_gpt(candidate)
            
            # Only include if it's a genuine prospect
            if analysis.get("confidence_score", 0) > 0.4 and not analysis.get("is_promotional", True):
                analysis["original_opportunity"] = candidate
                analyzed_prospects.append(analysis)
        
        # Sort by estimated value
        value_order = {"VERY_HIGH": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1, "": 0}
        analyzed_prospects.sort(
            key=lambda x: (value_order.get(x.get("estimated_value", {}).get("value_level", ""), 0)), 
            reverse=True
        )
        
        return analyzed_prospects
    
    def _analyze_with_gpt(self, opportunity: Dict[str, Any]) -> Dict[str, Any]:

        # Extract thread data
        subject = opportunity["subject"]
        thread_data = opportunity["thread_data"]
        messages = thread_data.get("messages", [])
        
        # Prepare email thread content for GPT
        email_content = f"Subject: {subject}\n\n"
        
        for i, msg in enumerate(messages):
            email_content += f"Message {i+1}:\n{msg.get('body', '')}\n\n"
        
        # Create system prompt
        system_prompt = """
        You are an expert business development and sales AI assistant. Your task is to analyze email conversations
        that might represent missed business opportunities. Your analysis should be thorough, insightful, and
        structured according to the requested format.
        
        Focus on identifying:
        1. The nature of the potential opportunity (lead, partnership, client, etc.)
        2. The estimated value/potential of the opportunity
        3. Signs of interest from the other party
        4. Why the conversation likely went cold
        5. Specific, actionable follow-up suggestions
        
        Be pragmatic, business-focused, and provide concrete insights rather than generic advice.
        Output should be in JSON format as specified in the user's prompt.
        """
        
        # Create user prompt
        user_prompt = f"""
        Analyze the following email thread that appears to be a cold business opportunity - a conversation that had 
        potential value but went cold without proper follow-up or closure.

        === EMAIL THREAD ===
        {email_content}
        ==================

        Provide an analysis in the following JSON format:
        ```json
        {{
            "opportunity_type": "String - One of: LEAD, CLIENT, PARTNERSHIP, RECRUITMENT, INVESTMENT, OTHER",
            "confidence_score": "Float between 0-1 indicating how confident you are this is a genuine opportunity",
            "is_promotional": "Boolean - True if this appears to be a promotional email rather than a genuine opportunity",
            "opportunity_summary": "2-3 sentence summary of the potential opportunity",
            "key_indicators": ["List of phrases or signals in the emails that indicate business potential"],
            "estimated_value": {{
                "value_level": "String - One of: LOW, MEDIUM, HIGH, VERY_HIGH",
                "reasoning": "Explanation of why you assigned this value level"
            }},
            "why_went_cold": "Analysis of why the conversation likely stopped",
            "follow_up_suggestions": ["List of 3-5 specific, tailored follow-up actions"],
            "ideal_follow_up_message": "A suggested follow-up email template personalized to this specific opportunity"
        }}
        ```

        If the email thread doesn't appear to be a genuine business opportunity (e.g., it's just a newsletter, promotional content, or automated message), set the confidence_score to 0.1 or lower and is_promotional to true, but still complete the rest of the analysis as best as possible.
        """
        
        # Call GPT for analysis
        try:
            response = openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                max_tokens=1500,
                response_format={"type": "json_object"}
            )
            
            # Parse the JSON response
            analysis = json.loads(response.choices[0].message.content)
            return analysis
            
        except Exception as e:
            print(f"Error during GPT analysis: {str(e)}")
            return {
                "error": str(e),
                "opportunity_type": "ERROR",
                "confidence_score": 0,
                "is_promotional": True,
                "opportunity_summary": f"Error analyzing this opportunity: {str(e)}",
                "estimated_value": {"value_level": "", "reasoning": ""}
            }
    
    def generate_prospects_report(self, prospects: List[Dict[str, Any]]) -> Dict[str, Any]:

        # Filter out any prospects with errors
        valid_prospects = [p for p in prospects if "error" not in p]
        
        # Opportunity type distribution
        type_distribution = {}
        for prospect in valid_prospects:
            opp_type = prospect.get("opportunity_type", "OTHER")
            type_distribution[opp_type] = type_distribution.get(opp_type, 0) + 1
        
        # Create report structure
        report = {
            "timestamp": datetime.datetime.now().isoformat(),
            "total_prospects": len(valid_prospects),
            "type_distribution": type_distribution,
            "prospects": []
        }
        
        # Add simplified prospect data
        for prospect in valid_prospects:
            original = prospect.get("original_opportunity", {})
            
            report["prospects"].append({
                "subject": original.get("subject", "Unknown"),
                "date_time": original.get("date_time", ""),
                "type": prospect.get("opportunity_type", ""),
                "value": prospect.get("estimated_value", {}).get("value_level", ""),
                "confidence": prospect.get("confidence_score", 0),
                "summary": prospect.get("opportunity_summary", ""),
                "why_went_cold": prospect.get("why_went_cold", ""),
                "follow_up": prospect.get("follow_up_suggestions", []),
                "follow_up_message": prospect.get("ideal_follow_up_message", "")
            })
        
        return report

def visualize(report):

    print("\n=== COLD OPPORTUNITY ANALYSIS REPORT ===")
    print(f"Total Prospects: {report['total_prospects']}")
    
    print("\nTypes:")
    for type_name, count in report['type_distribution'].items():
        print(f"  {type_name}: {count}")
    
    print("\nPROSPECTS:")
    for i, prospect in enumerate(report['prospects'], 1):
        print(f"\n{i}. {prospect['subject']}")
        print(f"   Type: {prospect['type']} | Value: {prospect['value']} | Confidence: {prospect['confidence']:.2f}")
        print(f"   Date: {prospect['date_time']}")
        print(f"   Summary: {prospect['summary'][:100]}...")
        print(f"   Why Cold: {prospect['why_went_cold'][:100]}...")
        print(f"   Follow-up: {prospect['follow_up'][0]}")
        print("   " + "-"*40)

def find_prospects(user_id: str = "default_user", top_k: int = 10) -> Dict[str, Any]:

    finder = ProspectFinder(user_id=user_id)
    prospects = finder.find_prospects(top_k=top_k, min_thread_length=2)
    report = finder.generate_prospects_report(prospects)
    
    # Save report to file
    os.makedirs("database", exist_ok=True)
    with open(f"database/prospects_report_{user_id}.json", "w") as f:
        json.dump(report, f, indent=2)
    
    print(f"Found {len(prospects)} potential prospects for user {user_id}")
    print(f"Report saved to database/prospects_report_{user_id}.json")
    
    return report

if __name__ == '__main__':
    
    user_id = "subhraturning"
    report = find_prospects(user_id)
    visualize(report)
