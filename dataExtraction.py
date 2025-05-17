import re
import json
import pytz
import time
import email
import imaplib
import html2text 
from pathlib import Path
from bs4 import BeautifulSoup
from typing import Dict, Optional
from collections import defaultdict
from email.header import decode_header
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime

def decode_header_value(header_value: str) -> str:
    
    try:
        decoded_headers = decode_header(header_value or '')
        decoded_parts = []
        for content, charset in decoded_headers:
            if isinstance(content, bytes):
                decoded_parts.append(content.decode(charset or 'utf-8', errors='replace'))
            else:
                decoded_parts.append(str(content))
        return ' '.join(decoded_parts)
    except Exception as e:
        print(f"Error decoding header: {str(e)}")
        return str(header_value)

def extract_email_address(header: str) -> str:
    
    try:
        match = re.search(r'<([^>]+)>', header)
        if match:
            return match.group(1)
        else:
            match = re.search(r'[\w\.-]+@[\w\.-]+', header)
            if match:
                return match.group(0)
            return header
    except Exception as e:
        print(f"Error extracting email address: {str(e)}")
        return header

def clean_html_content(html_content: str) -> str:

    if not html_content:
        return ""
    
    if '<html' in html_content.lower() or '<body' in html_content.lower() or '<div' in html_content.lower():
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            for script in soup(["script", "style"]):
                script.extract()
            
            text = soup.get_text(separator='\n', strip=True)
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = '\n'.join(chunk for chunk in chunks if chunk)
            
            if len(text) < 50 and len(html_content) > 1000:
                h = html2text.HTML2Text()
                h.ignore_links = False
                h.ignore_images = True
                h.body_width = 0
                text = h.handle(html_content)
                text = re.sub(r'\[(.*?)\]\((.*?)\)', r'\1 (\2)', text)
            
            return text
        
        except Exception as e:
            print(f"Error converting HTML to text: {str(e)}")
            return html_content
    else:
        return html_content

def extract_email_details(email_message) -> Dict:
    
    try:
        subject = decode_header_value(email_message.get("Subject", ""))
        from_header = decode_header_value(email_message.get("From", ""))
        to_header = decode_header_value(email_message.get("To", ""))
        from_email = extract_email_address(from_header)
        to_email = extract_email_address(to_header)
        
        date_str = email_message.get("Date")
        if date_str:
            
            try:
                date = parsedate_to_datetime(date_str)
                date = date.astimezone(pytz.UTC)  # Ensure UTC timezone
            except:
                date = datetime.now(pytz.UTC)
        
        else:
            date = datetime.now(pytz.UTC)
        
        body = ""
        content_type = ""
        
        if email_message.is_multipart():
            html_part = None
            text_part = None
            
            for part in email_message.walk():
                content_type = part.get_content_type()
                
                if content_type == "text/html":
                    html_part = part
                elif content_type == "text/plain":
                    text_part = part
            
            if html_part:
                
                try:
                    payload = html_part.get_payload(decode=True)
                    if payload:
                        html_content = payload.decode('utf-8', errors='replace')
                        body = clean_html_content(html_content)
                except Exception as e:
                    print(f"Error processing HTML part: {str(e)}")
            
            if not body and text_part:
                
                try:
                    payload = text_part.get_payload(decode=True)
                    if payload:
                        body = payload.decode('utf-8', errors='replace')
                except Exception as e:
                    print(f"Error processing text part: {str(e)}")
        else:
            content_type = email_message.get_content_type()
            try:
                payload = email_message.get_payload(decode=True)
                if payload:
                    if content_type == "text/html":
                        html_content = payload.decode('utf-8', errors='replace')
                        body = clean_html_content(html_content)
                    else:
                        body = payload.decode('utf-8', errors='replace')
            except:
                body = email_message.get_payload(decode=False) or ""
        
        labels = []
        flags = getattr(email_message, 'flags', []) or []
        for flag in flags:
            if isinstance(flag, bytes):
                flag = flag.decode('utf-8', errors='replace')
            labels.append(str(flag))
        
        folder = getattr(email_message, 'folder', '')
        if folder:
            if 'sent' in folder.lower():
                labels.append('SENT')
            elif 'inbox' in folder.lower():
                labels.append('INBOX')
        
        message_details = {
            "message_id": email_message.get("Message-ID", ""),
            "datetime": date.strftime("%Y-%m-%d %H:%M:%S UTC"),  
            "timestamp": date.timestamp(),
            "sender": from_email,  
            "receiver": to_email,  
            "subject": subject,
            "body": body,
            "references": [],  
            "in_reply_to": "",  
            "labels": labels
        }
        
        thread_id = getattr(email_message, 'thread_id', None)
        if thread_id:
            message_details["thread_id"] = thread_id
            
        return message_details
    except Exception as e:
        print(f"Error extracting email details: {str(e)}")
        return {}

class customEmailDataExtractor:
    
    def __init__(self, email_address: str, password: str, imap_server: str = "imap.gmail.com"):
        self.email_address = email_address
        self.password = password
        self.imap_server = imap_server

    def fetch_email_threads_complete(self, output_file: Optional[str] = None) -> str:
        return self._fetch_emails(None, output_file)
    
    def fetch_email_threads_by_prev_days(self, num_prev_days: int, output_file: Optional[str] = None) -> str:
        return self._fetch_emails(num_prev_days, output_file)
    
    def fetch_email_threads(self, num_prev_days: Optional[int] = None, output_file: Optional[str] = None) -> str:
        try:
            print("\n===== Starting Email Thread Extraction =====")
            print(f"Email: {self.email_address}")
            print(f"Server: {self.imap_server}")
            print(f"Time range: {'All emails' if num_prev_days is None else f'Last {num_prev_days} days'}")
            start_time = time.time()
            
            if num_prev_days is None:
                result = self.fetch_email_threads_complete(output_file)
            else:
                result = self.fetch_email_threads_by_prev_days(num_prev_days, output_file)
            
            end_time = time.time()
            print(f"\n===== Extraction completed in {end_time - start_time:.2f} seconds =====")
            return result
                
        except Exception as e:
            print(f"\n===== ERROR IN EXTRACTION PROCESS =====")
            print(f"Error fetching email threads: {str(e)}")
            
            print(f"Error type: {type(e).__name__}")
            print(f"Error occurred while fetching {'all threads' if num_prev_days is None else f'threads for past {num_prev_days} days'}")
            print("=======================================")
            
            return ""  
    
    def _fetch_emails(self, num_prev_days: Optional[int], output_file: Optional[str]) -> str:
        email_address = self.email_address
        password = self.password
        imap_server = self.imap_server
        
        if output_file is None:
            output_file = f"{email_address}.json"
            
        try:
            print("\n[1/8] Connecting to IMAP server...")
            connection_start = time.time()
            mail = imaplib.IMAP4_SSL(imap_server)
            print(f"[1/8] Connected to {imap_server} in {time.time() - connection_start:.2f} seconds")
            
            print(f"\n[2/8] Logging in as {email_address}...")
            login_start = time.time()
            mail.login(email_address, password)
            print(f"[2/8] Login successful in {time.time() - login_start:.2f} seconds")
            
            print("\n[3/8] Listing available mail folders...")
            status, folder_list = mail.list()
            if status != 'OK':
                print("Failed to retrieve folder list")
                return ""
                
            folders = []
            for folder_data in folder_list:
                if isinstance(folder_data, bytes):
                    folder_str = folder_data.decode('utf-8')
                    match = re.search(r'"[^"]+"$', folder_str)
                    if match:
                        folder_name = match.group(0).strip('"')
                        folders.append(folder_name)
            
            print(f"[3/8] Found {len(folders)} folders: {', '.join(folders[:5])}{' and more...' if len(folders) > 5 else ''}")
            
            all_emails = []
            
            def fetch_folder_emails(folder_name):

                try:
                    display_folder = folder_name.replace('"', '')
                    print(f"\n[*] Processing folder: {display_folder}")
                    
                    if '[Gmail]' in folder_name:
                        folder_name = folder_name.replace('"', '')
                        folder_name = f'"{folder_name}"'
                    elif ' ' in folder_name and not (folder_name.startswith('"') and folder_name.endswith('"')):
                        folder_name = f'"{folder_name}"'

                    print(f"Selecting folder...")
                    status, _ = mail.select(folder_name, readonly=True)
                    if status != 'OK':
                        print(f"Failed to select folder {folder_name}: {status}")
                        return []
                    
                    if num_prev_days is not None:
                        date_filter = (datetime.now(pytz.UTC) - timedelta(days=num_prev_days))
                        search_criteria = f'SINCE "{date_filter.strftime("%d-%b-%Y")}"'
                        print(f"Searching for emails since {date_filter.strftime('%Y-%m-%d')}...")
                    else:
                        search_criteria = "ALL"
                        print(f"Searching for all emails...")
                    
                    search_start = time.time()
                    status, message_numbers = mail.search(None, search_criteria)
                    if status != 'OK':
                        print(f"Search failed with status: {status}")
                        return []
                    
                    message_ids = message_numbers[0].split()
                    message_count = len(message_ids)
                    print(f"ound {message_count} emails in {time.time() - search_start:.2f} seconds")
                    
                    if message_count == 0:
                        return []
                    
                    folder_emails = []
                    print(f"Processing emails... (0/{message_count})")
                    processing_start = time.time()
                    
                    for i, num in enumerate(message_ids):
                        if i % 10 == 0 or i == message_count - 1:
                            elapsed = time.time() - processing_start
                            emails_per_second = (i + 1) / elapsed if elapsed > 0 else 0
                            remaining = (message_count - i - 1) / emails_per_second if emails_per_second > 0 else 0
                            print(f"Processing emails... ({i+1}/{message_count}) - {emails_per_second:.2f} emails/sec, ~{remaining:.1f} seconds remaining")
                        
                        try:
                            try:
                                if imap_server == "imap.gmail.com":
                                    status, thrid_data = mail.fetch(num, "(X-GM-THRID)")
                                    if status == 'OK' and thrid_data and thrid_data[0]:
                                        thread_id_match = re.search(r'X-GM-THRID\s+([0-9]+)', thrid_data[0].decode('utf-8'))
                                        if thread_id_match:
                                            thread_id = thread_id_match.group(1)
                                        else:
                                            thread_id = None
                            except Exception as e:
                                thread_id = None
                                
                            status, msg_data = mail.fetch(num, "(RFC822)")
                            if status != 'OK' or not msg_data or not msg_data[0]:
                                continue
                            
                            if isinstance(msg_data[0], tuple) and len(msg_data[0]) > 1:
                                email_body = msg_data[0][1]
                                if not isinstance(email_body, bytes):
                                    continue
                                email_message = email.message_from_bytes(email_body)
                            else:
                                continue
                            
                            email_message.folder = folder_name  # Add folder info
                            
                            if thread_id:
                                email_message.thread_id = thread_id
                            
                            status, flag_data = mail.fetch(num, "(FLAGS)")
                            if status == 'OK' and flag_data and flag_data[0]:
                                flags_str = flag_data[0].decode('utf-8') if isinstance(flag_data[0], bytes) else str(flag_data[0])
                                email_message.flags = re.findall(r'\(([^)]*)\)', flags_str)
                            
                            email_details = extract_email_details(email_message)
                            if email_details:
                                folder_emails.append(email_details)
                                
                        except Exception as e:
                            print(f"    Error processing email {num.decode('utf-8') if isinstance(num, bytes) else num}: {str(e)}")
                            continue
                    
                    total_time = time.time() - processing_start
                    print(f"    Completed processing {len(folder_emails)}/{message_count} emails in {total_time:.2f} seconds")
                    return folder_emails
                except Exception as e:
                    print(f"    Error accessing folder {folder_name}: {str(e)}")
                    return []
            
            print("\n[4/8] Fetching emails from Sent folder...")
            sent_folders = ['[Gmail]/Sent Mail', '[Gmail]/Sent', 'Sent', 'Sent Items']
            sent_emails = []
            for folder in sent_folders:
                print(f"    Trying sent folder: {folder}")
                sent_emails = fetch_folder_emails(folder)
                if sent_emails:
                    print(f"    Found {len(sent_emails)} sent emails in {folder}")
                    all_emails.extend(sent_emails)
                    break
                else:
                    print(f"    No emails found in {folder} or folder not accessible")
            
            print("\n[5/8] Fetching emails from Inbox...")
            inbox_emails = fetch_folder_emails('INBOX')
            if inbox_emails:
                print(f"ound {len(inbox_emails)} inbox emails")
                all_emails.extend(inbox_emails)
            else:
                print("No emails found in INBOX or folder not accessible")
            
            print(f"\n[6/8] Logging out from mail server...")
            mail.logout()
            print(f"Successfully logged out")
            
            print(f"\n[7/8] Organizing {len(all_emails)} emails into threads...")
            thread_start = time.time()
            
            thread_map = defaultdict(list)
            gmail_thread_count = 0
            
            for email_data in all_emails:
                if "thread_id" in email_data:
                    thread_map[email_data["thread_id"]].append(email_data)
                    gmail_thread_count += 1
            
            if gmail_thread_count > 0:
                print(f"ound {gmail_thread_count} emails with Gmail thread IDs ({len(thread_map)} unique threads)")
            
            if not thread_map:
                print("No Gmail thread IDs found, reconstructing threads...")
                msg_id_map = {msg["message_id"]: msg for msg in all_emails if msg["message_id"]}
                print(f"    Found {len(msg_id_map)} emails with message IDs")
                
                reply_map = defaultdict(list)
                
                print("Building reply relationships...")
                for email_data in all_emails:
                    msg_id = email_data.get("message_id")
                    if not msg_id:
                        continue
                        
                    in_reply_to = email_data.get("in_reply_to")
                    if in_reply_to and in_reply_to in msg_id_map:
                        reply_map[in_reply_to].append(msg_id)
                    
                    for ref in email_data.get("references", []):
                        if ref in msg_id_map:
                            reply_map[ref].append(msg_id)
                
                if reply_map:
                    print(f"Found {len(reply_map)} reply relationships")
                    roots = set()
                    for msg in all_emails:
                        msg_id = msg.get("message_id")
                        if not msg_id:
                            continue
                            
                        has_parent = False
                        in_reply_to = msg.get("in_reply_to")
                        if in_reply_to and in_reply_to in msg_id_map:
                            has_parent = True
                            
                        if not has_parent:
                            roots.add(msg_id)
                    
                    print(f"Found {len(roots)} root messages")
                    
                    processed_msgs = set()
                    def build_thread(root_id, thread_id):
                        if root_id in processed_msgs:
                            return
                        
                        processed_msgs.add(root_id)
                        if root_id in msg_id_map:
                            thread_map[thread_id].append(msg_id_map[root_id])
                            
                        for reply_id in reply_map.get(root_id, []):
                            build_thread(reply_id, thread_id)
                    
                    print("Building threads from roots...")
                    thread_count = 0
                    for root_id in roots:
                        build_thread(root_id, root_id)
                        thread_count += 1
                        if thread_count % 100 == 0:
                            print(f"Processed {thread_count}/{len(roots)} root messages")
                    
                    print(f"Created {len(thread_map)} threads from root messages")
                
                if not thread_map:
                    print("No reply relationships found, grouping by subject")
                    subject_map = defaultdict(list)
                    for email_data in all_emails:
                        normalized_subject = re.sub(r'^(?:Re|Fwd):\s*', '', email_data["subject"], flags=re.IGNORECASE)
                        subject_map[normalized_subject].append(email_data)
                    
                    print(f"Found {len(subject_map)} unique subjects")
                    for subject, emails in subject_map.items():
                        thread_id = f"thread_{hash(subject) % 10000000:07d}"
                        for email_data in emails:
                            thread_map[thread_id].append(email_data)
            
            print(f"Thread organization completed in {time.time() - thread_start:.2f} seconds")
            
            print("\n[8/8] Finalizing thread data and saving to JSON...")
            save_start = time.time()
            
            threads = []
            print(f"Processing {len(thread_map)} threads...")
            
            thread_count = 0
            for thread_id, thread_messages in thread_map.items():
                thread_count += 1
                if thread_count % 100 == 0:
                    print(f"Processed {thread_count}/{len(thread_map)} threads")
                
                if not thread_messages:
                    continue
                    
                thread_messages.sort(key=lambda x: x["timestamp"])
                
                all_labels = set()
                for msg in thread_messages:
                    all_labels.update(msg.get("labels", []))
                
                thread_data = {
                    "thread_id": thread_id,
                    "total_messages": len(thread_messages),
                    "labels": list(all_labels),
                    "reply_to_message_id": thread_messages[-1]["message_id"] if thread_messages else None,
                    "messages": thread_messages
                }
                
                thread_data["sort_timestamp"] = thread_messages[-1]["timestamp"] if thread_messages else 0
                
                threads.append(thread_data)
            
            print("Sorting threads by most recent message...")
            threads.sort(key=lambda x: x["sort_timestamp"], reverse=True)
            
            for thread in threads:
                if "sort_timestamp" in thread:
                    del thread["sort_timestamp"]
            
            print(f"Preparing to save {len(threads)} threads to file...")
            
            # output_path = Path(output_file)
            # output_path.parent.mkdir(parents=True, exist_ok=True)
            
            original_path = Path(output_file)
            db_folder = original_path.parent / 'database'
            db_folder.mkdir(parents=True, exist_ok=True)
            output_path = db_folder / original_path.name
            
            print(f"Writing JSON file...")
            save_json_start = time.time()
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(threads, f, indent=2, ensure_ascii=False)
            
            print(f"JSON file written in {time.time() - save_json_start:.2f} seconds")
            print(f"Finalization completed in {time.time() - save_start:.2f} seconds")
            print(f"\nSuccessfully saved {len(threads)} threads to {output_path.absolute()}")
            return str(output_path.absolute())
        
        except Exception as e:
            print(f"Error: {str(e)}")
            return ""

# if __name__ == '__main__':

#     email_address = "subhraturning@gmail.com"
#     password = "iqgh oiay rzfz qqce"
#     fetcher = customEmailDataExtractor(email_address, password)
#     output_path = fetcher.fetch_email_threads()
#     print(f"output_path: {output_path}")
