"""
FastAPI main application for the Alteryx PySpark Converter.

Provides REST API endpoints and web interface for workflow conversion.
"""

from fastapi import FastAPI, File, UploadFile, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn
import tempfile
import os
import json
import time
from pathlib import Path
from typing import Dict, Any, Optional

# Import converter components
from ..core.converter import AlteryxPySparkConverter
from ..core.logger import get_logger
from ...config.settings import Settings


# Initialize FastAPI app
app = FastAPI(
    title=Settings.APP_NAME,
    description=Settings.APP_DESCRIPTION,
    version=Settings.APP_VERSION,
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Initialize components
converter = AlteryxPySparkConverter()
logger = get_logger()

# Setup templates
templates_dir = Settings.TEMPLATES_DIR
if not templates_dir.exists():
    templates_dir.mkdir(parents=True, exist_ok=True)

templates = Jinja2Templates(directory=str(templates_dir))

# Global statistics tracking
app_stats = {
    "total_conversions": 0,
    "successful_conversions": 0,
    "failed_conversions": 0,
    "total_tools_processed": 0,
    "app_start_time": time.time()
}


@app.on_event("startup")
async def startup_event():
    """Initialize application on startup."""
    logger.app_logger.info(f"Starting {Settings.APP_NAME} v{Settings.APP_VERSION}")
    logger.app_logger.info(f"Web server running on {Settings.WEB_HOST}:{Settings.WEB_PORT}")
    
    # Validate settings
    issues = Settings.validate_settings()
    if issues:
        for issue in issues:
            logger.app_logger.warning(f"Configuration issue: {issue}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on application shutdown."""
    logger.app_logger.info("Shutting down Alteryx PySpark Converter")
    
    # Export final analytics report
    try:
        report_file = logger.export_analytics_report()
        logger.app_logger.info(f"Final analytics report saved to: {report_file}")
    except Exception as e:
        logger.app_logger.error(f"Failed to export final analytics report: {e}")


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Serve the main web interface."""
    try:
        # Get converter statistics
        converter_stats = converter.get_converter_statistics()
        supported_tools = converter.get_supported_tools()
        
        context = {
            "request": request,
            "app_name": Settings.APP_NAME,
            "app_version": Settings.APP_VERSION,
            "supported_tools_count": len(supported_tools),
            "total_conversions": app_stats["total_conversions"],
            "success_rate": (app_stats["successful_conversions"] / max(1, app_stats["total_conversions"])) * 100
        }
        
        return templates.TemplateResponse("index.html", context)
        
    except Exception as e:
        logger.log_error(
            component="WebAPI",
            error_type="TemplateRenderError",
            message=f"Failed to render home page: {str(e)}"
        )
        return HTMLResponse("Error loading page", status_code=500)


@app.post("/api/upload")
async def upload_workflow(file: UploadFile = File(...)):
    """Upload and analyze an Alteryx workflow file."""
    
    if not file.filename.endswith(('.yxmd', '.yxwz')):
        raise HTTPException(
            status_code=400,
            detail="Invalid file type. Only .yxmd and .yxwz files are supported."
        )
    
    # Check file size
    if file.size > Settings.MAX_UPLOAD_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"File too large. Maximum size is {Settings.MAX_UPLOAD_SIZE / (1024*1024):.1f}MB"
        )
    
    # Save uploaded file temporarily
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.yxmd') as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name
        
        logger.app_logger.info(f"Uploaded file: {file.filename} ({file.size} bytes)")
        
        # Parse and analyze the workflow
        from ..utils.xml_parser import XMLParser
        xml_parser = XMLParser()
        
        # Validate file
        is_valid, validation_errors = xml_parser.validate_workflow_file(temp_file_path)
        if not is_valid:
            os.unlink(temp_file_path)
            raise HTTPException(
                status_code=400,
                detail=f"Invalid workflow file: {'; '.join(validation_errors)}"
            )
        
        # Parse workflow
        workflow_data = xml_parser.parse_workflow_file(temp_file_path)
        if not workflow_data:
            os.unlink(temp_file_path)
            raise HTTPException(
                status_code=400,
                detail="Failed to parse workflow file"
            )
        
        # Analyze tools and connections
        tools = workflow_data.get('tools', [])
        connections = workflow_data.get('connections', [])
        
        # Calculate support statistics
        supported_count = 0
        unsupported_tools = []
        
        for tool in tools:
            tool_type = tool.get('type', '')
            if converter.registry.is_tool_supported(tool_type):
                supported_count += 1
            else:
                unsupported_tools.append({
                    'id': tool.get('id', ''),
                    'type': tool_type,
                    'name': tool.get('name', tool_type)
                })
        
        support_rate = (supported_count / len(tools) * 100) if tools else 0
        
        # Store file path for conversion
        workflow_data['temp_file_path'] = temp_file_path
        
        # Update app statistics
        app_stats["total_conversions"] += 1
        app_stats["total_tools_processed"] += len(tools)
        
        # Log the analysis
        logger.log_conversion_start(file.filename, len(tools))
        
        response_data = {
            "success": True,
            "workflow_name": workflow_data['workflow_name'],
            "file_name": file.filename,
            "total_tools": len(tools),
            "supported_tools": supported_count,
            "unsupported_tools": len(tools) - supported_count,
            "support_rate": round(support_rate, 2),
            "tools": tools,
            "connections": connections,
            "unsupported_tool_list": unsupported_tools,
            "temp_file_path": temp_file_path
        }
        
        return JSONResponse(response_data)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.log_error(
            component="WebAPI",
            error_type="UploadError",
            message=f"Failed to process uploaded file: {str(e)}"
        )
        
        # Cleanup temp file if it exists
        if 'temp_file_path' in locals():
            try:
                os.unlink(temp_file_path)
            except:
                pass
        
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process uploaded file: {str(e)}"
        )


@app.post("/api/convert")
async def convert_workflow(request: Request):
    """Convert an analyzed workflow to PySpark code."""
    
    try:
        # Get request data
        body = await request.json()
        temp_file_path = body.get('temp_file_path')
        
        if not temp_file_path or not os.path.exists(temp_file_path):
            raise HTTPException(
                status_code=400,
                detail="Invalid or missing workflow file"
            )
        
        # Perform conversion
        start_time = time.time()
        result = converter.convert_workflow_file(temp_file_path)
        conversion_time = time.time() - start_time
        
        # Cleanup temp file
        try:
            os.unlink(temp_file_path)
        except:
            pass
        
        # Update app statistics
        if result['success']:
            app_stats["successful_conversions"] += 1
        else:
            app_stats["failed_conversions"] += 1
        
        # Add performance metrics
        result['conversion_time'] = conversion_time
        result['timestamp'] = time.time()
        
        logger.app_logger.info(
            f"Conversion completed: {result['workflow_name']} "
            f"(Success: {result['success']}, Time: {conversion_time:.2f}s)"
        )
        
        return JSONResponse(result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.log_error(
            component="WebAPI",
            error_type="ConversionError",
            message=f"Failed to convert workflow: {str(e)}"
        )
        
        app_stats["failed_conversions"] += 1
        
        raise HTTPException(
            status_code=500,
            detail=f"Conversion failed: {str(e)}"
        )


@app.get("/api/statistics")
async def get_statistics():
    """Get comprehensive application statistics."""
    
    try:
        # Get converter statistics
        converter_stats = converter.get_converter_statistics()
        
        # Calculate uptime
        uptime_seconds = time.time() - app_stats["app_start_time"]
        uptime_hours = uptime_seconds / 3600
        
        # Combine all statistics
        combined_stats = {
            "app_statistics": {
                **app_stats,
                "uptime_hours": round(uptime_hours, 2),
                "success_rate": (app_stats["successful_conversions"] / max(1, app_stats["total_conversions"])) * 100
            },
            "converter_statistics": converter_stats,
            "environment": Settings.get_environment_info()
        }
        
        return JSONResponse(combined_stats)
        
    except Exception as e:
        logger.log_error(
            component="WebAPI",
            error_type="StatisticsError",
            message=f"Failed to get statistics: {str(e)}"
        )
        
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve statistics"
        )


@app.post("/api/download-code")
async def download_code(request: Request):
    """Download generated PySpark code as a file."""
    
    try:
        body = await request.json()
        code = body.get('code', '')
        filename = body.get('filename', 'converted_workflow.py')
        
        if not code:
            raise HTTPException(
                status_code=400,
                detail="No code provided"
            )
        
        # Create temporary file with the code
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as temp_file:
            temp_file.write(code)
            temp_file_path = temp_file.name
        
        return FileResponse(
            path=temp_file_path,
            filename=filename,
            media_type='application/octet-stream'
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.log_error(
            component="WebAPI",
            error_type="DownloadError",
            message=f"Failed to prepare code download: {str(e)}"
        )
        
        raise HTTPException(
            status_code=500,
            detail="Failed to prepare download"
        )


@app.get("/api/export-analytics")
async def export_analytics():
    """Export comprehensive analytics report."""
    
    try:
        report_file = logger.export_analytics_report()
        
        return FileResponse(
            path=report_file,
            filename=f"analytics_report_{int(time.time())}.json",
            media_type='application/json'
        )
        
    except Exception as e:
        logger.log_error(
            component="WebAPI",
            error_type="AnalyticsExportError",
            message=f"Failed to export analytics: {str(e)}"
        )
        
        raise HTTPException(
            status_code=500,
            detail="Failed to export analytics report"
        )


@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring."""
    
    return {
        "status": "healthy",
        "app_name": Settings.APP_NAME,
        "version": Settings.APP_VERSION,
        "timestamp": time.time(),
        "uptime_seconds": time.time() - app_stats["app_start_time"]
    }


def run_server():
    """Run the FastAPI server."""
    uvicorn.run(
        "alteryx_pyspark_converter.src.api.main:app",
        host=Settings.WEB_HOST,
        port=Settings.WEB_PORT,
        reload=Settings.WEB_RELOAD,
        log_level=Settings.LOG_LEVEL.lower()
    )


if __name__ == "__main__":
    run_server()