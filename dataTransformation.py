import os
import json

def transform_json(input_path):

    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    transformed = []
    for thread in data:
        
        last_msg = thread['messages'][-1]
        subject = last_msg.get('subject', '')
        date_time = last_msg.get('datetime', '')
        total_count = len(thread.get('messages', []))
        messages = [{'body': msg.get('body', '')} for msg in thread['messages']]

        transformed.append({
            'subject': subject,
            'date_time': date_time,
            'total_number_of_emails_in_thread': total_count,
            'messages': messages
        })

    base_name = os.path.splitext(os.path.basename(input_path))[0]
    output_name = f"transform_{base_name}.json"
    db_folder = 'database'
    os.makedirs(db_folder, exist_ok=True)
    output_path = os.path.join(db_folder, output_name)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(transformed, f, ensure_ascii=False, indent=2)
    
    return output_path

# if __name__ == '__main__':
#     file_path = "database/subhraturning@gmail.com.json"
#     output_path = transform_json(file_path)
#     print(output_path)
