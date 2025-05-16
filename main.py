from dotenv import load_dotenv
from vectorizeEmail import vectorize_emails
from gptAnalysis import find_prospects
from dataExtraction import customEmailDataExtractor
from dataTransformation import transform_json

load_dotenv()

def main():

    email_address = "subhraturning@gmail.com"
    password = "iqgh oiay rzfz qqce"
    user_id = "subhraturning"
    
    fetcher = customEmailDataExtractor(email_address, password)
    output_path = fetcher.fetch_email_threads()
    final_path = transform_json(output_path)

    processed_count = vectorize_emails(email_data_file=final_path, user_id=user_id)
    prospects_report = find_prospects()

    # LOGGING
    print("\n=== RESULTS ===")
    print(f"Found {prospects_report['total_prospects']} potential prospects.")
    print("Prospect types:")
    for opp_type, count in prospects_report['type_distribution'].items():
        print(f"  - {opp_type}: {count}")
    
    print("\nTop prospects:")
    for i, prospect in enumerate(prospects_report['prospects'][:5], 1):
        print(f"{i}. {prospect['subject']} ({prospect['type']}, Value: {prospect['value']})")
        print(f"Summary: {prospect['summary']}")
        print()
    
    print(f"\nFull report saved to output/prospects_report_{user_id}.json")

if __name__ == "__main__":
    main()
