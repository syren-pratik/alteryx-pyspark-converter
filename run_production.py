#!/usr/bin/env python3
"""
Production server runner for Alteryx PySpark Converter
"""
import uvicorn
from main_app import app

if __name__ == "__main__":
    # Run on all network interfaces so other computers can access
    uvicorn.run(
        app,
        host="0.0.0.0",  # Listen on all interfaces
        port=8000,
        workers=4,  # Multiple workers for better performance
        log_level="info",
        access_log=True
    )