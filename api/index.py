from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from mangum import Mangum
import sys
import os

# Set the working directory to parent folder
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(parent_dir)
sys.path.insert(0, parent_dir)

try:
    from main_app import app
    
    # Add CORS middleware for Vercel deployment
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
except Exception as e:
    # Create a fallback app if main_app fails to import
    app = FastAPI(title="Alteryx Converter - Error")
    
    @app.get("/")
    async def error_handler():
        return JSONResponse({
            "error": "Failed to import main application", 
            "details": str(e),
            "working_directory": os.getcwd(),
            "python_path": sys.path[:3]
        }, status_code=500)

# Export the ASGI handler for Vercel
handler = Mangum(app)