from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

# Simple FastAPI app for Vercel
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
            <style>
                body { font-family: Arial, sans-serif; margin: 50px; }
                h1 { color: #2196F3; }
                .success { color: #4CAF50; }
            </style>
        </head>
        <body>
            <h1>🔄 Alteryx to PySpark Converter</h1>
            <p class="success">✅ Your app is successfully deployed on Vercel!</p>
            <p>This is a test version to verify deployment works.</p>
            <p><strong>Next steps:</strong> Add full converter functionality</p>
            
            <h2>Available Endpoints:</h2>
            <ul>
                <li><code>/</code> - This page</li>
                <li><code>/health</code> - Health check</li>
                <li><code>/api/status</code> - API status</li>
            </ul>
        </body>
    </html>
    """)

@app.get("/health")
async def health():
    return {"status": "ok", "message": "App is running successfully", "deployment": "vercel"}

@app.get("/api/status")
async def api_status():
    return {
        "service": "Alteryx PySpark Converter",
        "status": "operational", 
        "version": "1.0.0-test",
        "platform": "vercel"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)