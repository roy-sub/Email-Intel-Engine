import json
import os
import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import smtplib
import socket
import time
import logging
from typing import Optional
from dotenv import load_dotenv
from constants import (
    MAIL_HOST, 
    MAIL_PORT, 
    MAIL_USERNAME, 
    MAIL_FROM_ADDRESS, 
    MAIL_FROM_NAME,
    MAX_RETRIES,
    RETRY_DELAY,
    CONNECTION_TIMEOUT
)

# Load environment variables from .env file
load_dotenv()
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")

def send_account_ready_notification(recipient_email: str) -> bool:

    subject = "🚀 Your Cold Opportunity Finder Account is Ready!"
    
    html_content = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                color: #333;
            }}
            .container {{
                max-width: 600px;
                margin: 0 auto;
                padding: 20px;
                border: 1px solid #ddd;
                border-radius: 5px;
            }}
            .header {{
                background-color: #4A56E2;
                color: white;
                padding: 20px;
                text-align: center;
                border-radius: 5px 5px 0 0;
            }}
            .content {{
                padding: 20px;
            }}
            .button {{
                display: inline-block;
                background-color: #4A56E2;
                color: white !important;
                padding: 12px 24px;
                text-decoration: none;
                border-radius: 5px;
                margin-top: 20px;
                font-weight: bold;
            }}
            .footer {{
                text-align: center;
                margin-top: 20px;
                font-size: 12px;
                color: #777;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Your Account is Ready!</h1>
            </div>
            <div class="content">
                <p>Hello {recipient_email.split('@')[0]},</p>
                
                <p>Great news! Your Cold Opportunity Finder account is now fully set up and ready to use. You can now start finding those valuable missed prospects hiding in your email history.</p>
                
                <h3>What You Can Do Now:</h3>
                <ul>
                    <li><strong>Find Prospects</strong> - Discover potential leads and partnerships that went cold</li>
                    <li><strong>Analyze Opportunities</strong> - Get AI-powered insights on your business conversations</li>
                    <li><strong>Follow Up</strong> - Use our suggested templates to re-engage valuable prospects</li>
                </ul>
                
                <p>Simply log in to your dashboard to get started. We've already scanned your emails and identified the most promising opportunities that need your attention.</p>
                
                <div style="text-align: center;">
                    <a href="https://coldopportunities.ai/dashboard" class="button" style="color: white !important; text-decoration: none;">Go to Dashboard</a>
                </div>
                
                <p>If you have any questions, simply reply to this email and we'll be happy to help.</p>
                
                <p>Happy prospecting!</p>
                <p>The Cold Opportunity Finder Team</p>
            </div>
            <div class="footer">
                <p>© 2025 Cold Opportunity Finder. All rights reserved.</p>
                <p>You're receiving this email because you signed up for an account.</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return send_email_notification(recipient_email, html_content, subject)

def send_prospects_report(recipient_email: str, report_path: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> bool:

    # Format date range for email
    date_range = "All Time"
    if start_date and end_date:
        date_range = f"{start_date} to {end_date}"
    elif start_date:
        date_range = f"From {start_date}"
    elif end_date:
        date_range = f"Until {end_date}"
    
    # Create a properly formatted text report from the JSON
    txt_report_path = create_text_report(report_path, start_date, end_date)
    
    # Load report to get statistics
    try:
        with open(report_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)
            
        total_prospects = report_data.get('total_prospects', 0)
        type_distribution = report_data.get('type_distribution', {})
    except:
        total_prospects = "multiple"
        type_distribution = {}
    
    # Create email subject
    subject = f"🔍 Your Cold Opportunity Report ({date_range})"
    
    # Generate distribution text
    distribution_html = ""
    for opp_type, count in type_distribution.items():
        distribution_html += f"<li><strong>{opp_type}</strong>: {count}</li>"
    
    if not distribution_html:
        distribution_html = "<li>Prospect data not available</li>"
    
    # Create HTML content
    html_content = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                color: #333;
            }}
            .container {{
                max-width: 600px;
                margin: 0 auto;
                padding: 20px;
                border: 1px solid #ddd;
                border-radius: 5px;
            }}
            .header {{
                background-color: #4A56E2;
                color: white;
                padding: 20px;
                text-align: center;
                border-radius: 5px 5px 0 0;
            }}
            .content {{
                padding: 20px;
            }}
            .stats {{
                background-color: #f9f9f9;
                padding: 15px;
                border-radius: 5px;
                margin: 20px 0;
            }}
            .button {{
                display: inline-block;
                background-color: #4A56E2;
                color: white !important;
                padding: 12px 24px;
                text-decoration: none;
                border-radius: 5px;
                margin-top: 20px;
                font-weight: bold;
            }}
            .footer {{
                text-align: center;
                margin-top: 20px;
                font-size: 12px;
                color: #777;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Cold Opportunity Report</h1>
                <p>{date_range}</p>
            </div>
            <div class="content">
                <p>Hello {recipient_email.split('@')[0]},</p>
                
                <p>We've analyzed your email data and found <strong>{total_prospects}</strong> potential cold opportunities that might be worth following up on!</p>
                
                <div class="stats">
                    <h3>Opportunity Breakdown:</h3>
                    <ul>
                        {distribution_html}
                    </ul>
                </div>
                
                <p>We've attached a detailed report with information about each opportunity, including:</p>
                <ul>
                    <li>The original conversation details</li>
                    <li>Why each conversation likely went cold</li>
                    <li>Suggested follow-up actions</li>
                    <li>Ready-to-use follow-up email templates</li>
                </ul>
                
                <p>These opportunities represent potential business value that's just waiting to be reclaimed!</p>
                
                <div style="text-align: center;">
                    <a href="https://coldopportunities.ai/dashboard" class="button" style="color: white !important; text-decoration: none;">View Full Report</a>
                </div>
                
                <p>If you have any questions, simply reply to this email and we'll be happy to help.</p>
                
                <p>Happy prospecting!</p>
                <p>The Cold Opportunity Finder Team</p>
            </div>
            <div class="footer">
                <p>© 2025 Cold Opportunity Finder. All rights reserved.</p>
                <p>You're receiving this email because you signed up for an account.</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return send_email_with_attachment(recipient_email, html_content, subject, txt_report_path)

def create_text_report(json_path: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> str:

    try:
        # Load JSON report
        with open(json_path, 'r', encoding='utf-8') as f:
            report = json.load(f)
        
        # Format date range for report title
        date_range = "All Time"
        if start_date and end_date:
            date_range = f"{start_date} to {end_date}"
        elif start_date:
            date_range = f"From {start_date}"
        elif end_date:
            date_range = f"Until {end_date}"
        
        # Create text report
        text_report = f"COLD OPPORTUNITY ANALYSIS REPORT - {date_range}\n"
        text_report += f"Generated: {report.get('timestamp', datetime.datetime.now().isoformat())}\n"
        text_report += f"Total Prospects: {report.get('total_prospects', 0)}\n\n"
        
        # Add type distribution
        text_report += "OPPORTUNITY TYPE DISTRIBUTION:\n"
        for opp_type, count in report.get('type_distribution', {}).items():
            text_report += f"- {opp_type}: {count}\n"
        
        # Add each prospect
        text_report += "\nPROSPECTS:\n"
        
        for i, prospect in enumerate(report.get('prospects', []), 1):
            text_report += f"\n{'='*80}\n"
            
            # Clean the subject by replacing emojis or other special characters
            subject = prospect.get('subject', 'Unknown')
            subject = ''.join(c for c in subject if ord(c) < 65536)  # Remove non-BMP characters
            
            text_report += f"PROSPECT #{i}: {subject}\n"
            text_report += f"{'='*80}\n"
            
            text_report += f"Type: {prospect.get('type', 'Unknown')}\n"
            text_report += f"Value: {prospect.get('value', 'Unknown')}\n"
            text_report += f"Confidence: {prospect.get('confidence', 0):.2f}\n"
            text_report += f"Date: {prospect.get('date_time', 'Unknown')}\n\n"
            
            text_report += "SUMMARY:\n"
            text_report += f"{prospect.get('summary', 'No summary available')}\n\n"
            
            text_report += "WHY IT WENT COLD:\n"
            text_report += f"{prospect.get('why_went_cold', 'Unknown')}\n\n"
            
            text_report += "SUGGESTED FOLLOW-UP ACTIONS:\n"
            for j, suggestion in enumerate(prospect.get('follow_up', []), 1):
                text_report += f"{j}. {suggestion}\n"
            text_report += "\n"
            
            text_report += "SUGGESTED FOLLOW-UP MESSAGE:\n"
            text_report += f"{'-'*40}\n"
            follow_up_msg = prospect.get('follow_up_message', 'No message available')
            # Replace any problematic characters in the follow-up message
            follow_up_msg = ''.join(c for c in follow_up_msg if ord(c) < 65536)
            text_report += f"{follow_up_msg}\n"
            text_report += f"{'-'*40}\n"
        
        # Save as text file with UTF-8 encoding
        txt_path = json_path.replace('.json', '.txt')
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(text_report)
        
        return txt_path
    
    except Exception as e:
        logging.error(f"Error creating text report: {str(e)}")
        
        # Create a simple text file with error message
        txt_path = json_path.replace('.json', '.txt')
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(f"Error generating report: {str(e)}")
        
        return txt_path

def send_email_notification(recipient_email, html_content, subject):

    message = MIMEMultipart('alternative')
    message['Subject'] = subject
    message['From'] = f"{MAIL_FROM_NAME} <{MAIL_FROM_ADDRESS}>"
    message['To'] = recipient_email
    
    html_part = MIMEText(html_content, 'html')
    message.attach(html_part)
    
    return send_email(message, recipient_email)

def send_email_with_attachment(recipient_email, html_content, subject, attachment_path):

    message = MIMEMultipart('mixed')
    message['Subject'] = subject
    message['From'] = f"{MAIL_FROM_NAME} <{MAIL_FROM_ADDRESS}>"
    message['To'] = recipient_email
    
    # Attach HTML content
    html_part = MIMEText(html_content, 'html')
    message.attach(html_part)
    
    # Attach file
    try:
        with open(attachment_path, 'rb') as f:
            attachment = MIMEApplication(f.read(), _subtype="txt")
            attachment.add_header('Content-Disposition', 'attachment', 
                                  filename=os.path.basename(attachment_path))
            message.attach(attachment)
    except Exception as e:
        logging.error(f"Error attaching file: {str(e)}")
    
    return send_email(message, recipient_email)

def send_email(message, recipient_email):

    retry_delay = RETRY_DELAY
    for attempt in range(MAX_RETRIES):
        try:
            socket.gethostbyname(MAIL_HOST)
            
            with smtplib.SMTP(MAIL_HOST, MAIL_PORT, timeout=CONNECTION_TIMEOUT) as server:
                server.set_debuglevel(0)  
                server.ehlo()  
                server.starttls()  
                server.ehlo()  
                server.login(MAIL_USERNAME, MAIL_PASSWORD)
                server.send_message(message)
                
            # LOGGING:
            logging.info(f"Email notification sent successfully to: {recipient_email}")
            
            return True
            
        except (smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError, 
                ConnectionRefusedError, TimeoutError, socket.gaierror, socket.timeout) as e:
            
            logging.warning(f"Connection error: {str(e)}")
            
            if attempt < MAX_RETRIES - 1:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay = retry_delay * 1.5
            else:
                logging.error("Max retries reached. Failed to send email.")
                return False
            
        except Exception as e:
            logging.error(f"Email notification failed to: {recipient_email}\n{str(e)}")
            return False

# if __name__ == "__main__":

#     recipient = "subhrastien@gmail.com"
#     report_path = "database/prospects_report_subhraturning.json"
#     send_account_ready_notification(recipient)
#     send_prospects_report(recipient, report_path)
