import json
import os

class DataPreprocessor:
    
    def __init__(self, input_path):
        self.input_path = input_path
        self.output_path = os.path.splitext(input_path)[0] + '.txt'
        self.email_data = None
    
    def format_message(self, message, message_number):

        try:
            formatted = []
            formatted.append(f"MESSAGE {message_number}")
            formatted.append(f"ID: {message['message_id']}")
            formatted.append(f"Date: {message['datetime']}")
            formatted.append(f"From: {message['sender']}")
            formatted.append(f"To: {message['receiver']}")
            formatted.append(f"Subject: {message['subject']}")
            formatted.append(f"Labels: {', '.join(message['labels'])}")
            formatted.append(f"References: {message['references']}")
            formatted.append(f"In Reply To: {message['in_reply_to']}")
            formatted.append("")
            formatted.append("BODY:")
            formatted.append(message['body'])
            
            return "\n".join(formatted)
        except KeyError as e:
            raise ValueError(f"Missing required field in message: {str(e)}")
        except Exception as e:
            raise ValueError(f"Error formatting message: {str(e)}")

    def format_thread(self, thread):

        try:
            formatted = []
            
            # Thread separator and metadata
            formatted.append("=" * 58)
            formatted.append(f"THREAD ID: {thread['thread_id']}")
            formatted.append(f"Total Messages: {thread['total_messages']}")
            formatted.append(f"Thread Labels: {', '.join(thread['labels'])}")
            formatted.append(f"Reply To Message ID: {thread['reply_to_message_id']}")
            
            # Add each message in the thread
            for i, message in enumerate(thread['messages'], 1):
                formatted.append("-" * 58)
                formatted.append("")
                formatted.append(self.format_message(message, i))
                formatted.append("")
            
            return "\n".join(formatted)
        except KeyError as e:
            raise ValueError(f"Missing required field in thread: {str(e)}")
        except Exception as e:
            raise ValueError(f"Error formatting thread: {str(e)}")

    def convert(self):

        try:
            # Read JSON data
            with open(self.input_path, 'r', encoding='utf-8') as file:
                self.email_data = json.load(file)
            
            # Format all threads
            formatted_content = []
            for thread in self.email_data:
                formatted_content.append(self.format_thread(thread))
            
            # Add final separator
            formatted_content.append("=" * 58)
            
            # Write to output file
            with open(self.output_path, 'w', encoding='utf-8') as file:
                file.write("\n".join(formatted_content))
            
            return self.output_path
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format: {str(e)}")
        except FileNotFoundError:
            raise FileNotFoundError(f"Input file not found: {self.input_path}")
        except Exception as e:
            raise RuntimeError(f"Error during conversion: {str(e)}")
