import os
import json
import datetime

def add_user(email_address: str, password: str, user_id: str) -> bool:

    try:
        # Create database directory if it doesn't exist
        os.makedirs("database", exist_ok=True)
        
        # Path to users database file
        users_file = "database/users.json"
        
        # Load existing users or create new users dict
        users = {}
        if os.path.exists(users_file):
            try:
                with open(users_file, 'r') as f:
                    users = json.load(f)
            except json.JSONDecodeError:
                # If file exists but is empty or invalid JSON
                users = {}
        
        # Add new user
        users[email_address] = {
            "user_id": user_id,
            "password": password,  # Note: In a real app, you should hash passwords
            "created_at": str(datetime.datetime.now()),
            "status": "active"
        }
        
        # Save updated users dict
        with open(users_file, 'w') as f:
            json.dump(users, f, indent=2)
        
        print(f"User {email_address} added to database")
        return True
        
    except Exception as e:
        print(f"Error adding user to database: {str(e)}")
        return False

def user_login(email_address: str, password: str) -> bool:

    try:
        # Path to users database file
        users_file = "database/users.json"
        
        # Check if users file exists
        if not os.path.exists(users_file):
            print("Users database does not exist")
            return False
        
        # Load users dict
        with open(users_file, 'r') as f:
            users = json.load(f)
        
        # Check if user exists and password matches
        if email_address in users and users[email_address]["password"] == password:
            print(f"Login successful for {email_address}")
            return True
        else:
            print(f"Login failed for {email_address}")
            return False
        
    except Exception as e:
        print(f"Error during login: {str(e)}")
        return False
