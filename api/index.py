from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
import sys
import os

# Simple FastAPI app for testing
app = FastAPI(title="Alteryx PySpark Converter")

# Add CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def read_root():
    return HTMLResponse("""
    <html>
        <head>
            <title>Alteryx to PySpark Converter</title>
        </head>
        <body>
            <h1>🔄 Alteryx to PySpark Converter</h1>
            <p>Your app is successfully deployed on Vercel!</p>
            <p>This is a simplified version for testing.</p>
        </body>
    </html>
    """)

@app.get("/health")
async def health():
    return {"status": "ok", "message": "App is running successfully"}

# Simple function-based handler for Vercel
def handler(event, context):
    from mangum import Mangum
    asgi_handler = Mangum(app)
    return asgi_handler(event, context)