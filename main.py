import uvicorn
import threading
from pydantic import BaseModel, EmailStr
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, BackgroundTasks, HTTPException

# Import the wrapper functions directly from generator.py
from generator import onboarding, get_prospects

# Initialize FastAPI app
app = FastAPI(
    title="Cold Opportunity Finder API",
    description="API for finding cold opportunities in email data",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

# Request models
class OnboardingRequest(BaseModel):
    email_address: EmailStr
    password: str

class ProspectsRequest(BaseModel):
    email_address: EmailStr
    top_k: int = 10

# API routes
@app.get("/")
async def root():

    return {
        "message": "Welcome to Cold Opportunity Finder API",
        "version": "1.0.0",
        "endpoints": [
            "/onboarding",
            "/prospects"
        ]
    }

@app.post("/onboarding")
async def onboarding_endpoint(request: OnboardingRequest, background_tasks: BackgroundTasks):

    # Start onboarding in background task
    background_tasks.add_task(
        onboarding, 
        request.email_address, 
        request.password
    )
    
    # Respond immediately
    return {
        "message": "✨ Your AI-powered opportunity discovery journey has begun! ✨\n\nOur system is setting up your account to uncover valuable missed opportunities. This process may take some time depending on the size of your inbox.\n\nOnce complete, you'll receive an email notification with access to your inbox. Get ready to reclaim valuable business relationships!\n\nNo action is needed from you at this time - we'll notify you in your inbox when everything is ready.",
        "status": "processing"
    }

@app.post("/prospects")
async def prospects_endpoint(request: ProspectsRequest):

    try:
        # Since this can take up to 20 minutes, run in a separate thread
        # but wait for completion before responding
        
        email_address = request.email_address
        top_k = request.top_k
        
        # Create a thread for the long-running process
        result = {"success": False, "report": {}}
        
        def run_find_prospects():
            try:
                success, report = get_prospects(email_address, top_k)
                result["success"] = success
                result["report"] = report
            except Exception as e:
                result["error"] = str(e)
        
        # Start thread and wait for it to complete
        thread = threading.Thread(target=run_find_prospects)
        thread.start()
        thread.join()  # This will block until the thread completes
        
        # Check result
        if not result["success"]:
            raise Exception(result.get("error", "Unknown error during prospect finding"))
        
        # Return response
        return {
            "message": f"Found {result['report'].get('total_prospects', 0)} potential opportunities in your email history. A detailed report has been sent to your inbox.",
            "prospects_count": result['report'].get('total_prospects', 0),
            "type_distribution": result['report'].get('type_distribution', {}),
            "report": result['report'],
            "status": "success"
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error finding prospects: {str(e)}"
        )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
