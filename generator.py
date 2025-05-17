from typing import Dict, Any, Tuple
from gptAnalysis import find_prospects
from vectorizeEmail import vectorize_emails
from dataTransformation import transform_json
from dataExtraction import customEmailDataExtractor
from emailNotification import send_account_ready_notification, send_prospects_report


def onboarding(email_address: str, password: str) -> bool:

    try:
        
        # Step 1: Extract email data
        fetcher = customEmailDataExtractor(email_address, password)
        output_path = fetcher.fetch_email_threads()
        
        # Step 2: Transform data
        email_data_file = transform_json(output_path)
        
        # Step 3: Vectorize emails
        user_id = email_address.split('@')[0]
        vectorize_emails(email_data_file, user_id)
        
        # Step 4: Send account ready notification
        send_account_ready_notification(email_address)
        
        print(f"ONBOARDING COMPLETED FOR : {email_address}")
        return True
        
    except Exception as e:
        print(f"ONBOARDING FAILE FOR : {email_address}\n\nREASON: {str(e)}")
        return False

def get_prospects(email_address: str, top_k: int = 10) -> Tuple[bool, Dict[str, Any]]:

    try:
        
        # Step 1: Find prospects
        user_id = email_address.split('@')[0]
        report_file_path, report = find_prospects(user_id, top_k)       
        
        # Step 2: Send prospects report
        send_prospects_report(email_address, report_file_path)
        
        return True, report
        
    except Exception as e:
        print(f"ERROR GETTING THE PROSPECT FOR : {email_address}\n\nREASON: {str(e)}")
        return False, {"error": str(e)}

if __name__ == "__main__":

    email = "subhraturning@gmail.com"
    password = "your_password"
    
    success = onboarding(email, password)
    print(success)
    
    success, report = get_prospects(email)
    print(success)
    print(report)
