#!/usr/bin/env python3
"""
Simplified main application for the Alteryx PySpark Converter.
"""

from fastapi import FastAPI, File, UploadFile, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates
import uvicorn
import tempfile
import os
import json
import time
from pathlib import Path
from typing import Dict, Any, Optional

# Import converter for workflow analysis
from simple_converter_backup import SimpleAlteryxConverter
# from enhanced_converter import EnhancedAlteryxConverter  # TODO: Fix syntax errors


def format_code_for_platform(code: str, platform: str, workflow_name: str) -> str:
    """Format code for different platforms."""
    
    if platform == 'databricks':
        # Databricks notebook format with magic commands
        lines = code.split('\n')
        formatted_lines = [
            f"# Databricks notebook source",
            f"",
            f"# MAGIC %md",
            f"# MAGIC # {workflow_name}",
            f"# MAGIC Generated Alteryx workflow conversion",
            f"# MAGIC ",
            f"# MAGIC **Generated on:** {time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"",
            f"# COMMAND ----------"
        ]
        
        # Group lines into cells
        current_cell = []
        for line in lines:
            if line.startswith('# Step ') or line.startswith('# Import'):
                if current_cell:
                    formatted_lines.extend(current_cell)
                    formatted_lines.append("")
                    formatted_lines.append("# COMMAND ----------")
                current_cell = [line]
            else:
                current_cell.append(line)
        
        # Add the last cell
        if current_cell:
            formatted_lines.extend(current_cell)
        
        return '\n'.join(formatted_lines)
    
    elif platform == 'jupyter':
        # Jupyter notebook JSON format
        lines = code.split('\n')
        cells = []
        current_cell = []
        
        for line in lines:
            if line.startswith('# Step ') or line.startswith('# Import'):
                if current_cell:
                    cells.append({
                        "cell_type": "code",
                        "source": current_cell,
                        "metadata": {},
                        "outputs": []
                    })
                current_cell = [line + "\n"]
            else:
                current_cell.append(line + "\n")
        
        # Add the last cell
        if current_cell:
            cells.append({
                "cell_type": "code", 
                "source": current_cell,
                "metadata": {},
                "outputs": []
            })
        
        # Create notebook structure
        notebook = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "source": [
                        f"# {workflow_name}\n",
                        f"\n",
                        f"Converted Alteryx workflow to PySpark\n",
                        f"\n",
                        f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    ],
                    "metadata": {}
                }
            ] + cells,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python", 
                    "name": "python3"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 4
        }
        
        return json.dumps(notebook, indent=2)
    
    else:
        # Default PySpark script format
        header = f'''#!/usr/bin/env python3
"""
{workflow_name} - Converted from Alteryx

Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}
Platform: PySpark
"""

'''
        return header + code

# Initialize FastAPI app
app = FastAPI(
    title="Alteryx PySpark Converter",
    description="Enterprise-grade converter for Alteryx workflows to PySpark code",
    version="2.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Initialize components - use simple converter for core functionality
converter = SimpleAlteryxConverter()
# enhanced_converter = EnhancedAlteryxConverter()  # TODO: Enable after fixing syntax

# Setup templates with correct path
templates_dir = Path(__file__).parent / "templates"
templates_dir.mkdir(exist_ok=True)
templates = Jinja2Templates(directory=str(templates_dir))

# Global statistics tracking
app_stats = {
    "total_conversions": 0,
    "successful_conversions": 0,
    "failed_conversions": 0,
    "total_tools_processed": 0,
    "app_start_time": time.time()
}


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Serve the main web interface."""
    try:
        context = {
            "request": request,
            "app_name": "Alteryx PySpark Converter",
            "app_version": "2.0.0",
            "supported_tools_count": 15,
            "total_conversions": app_stats["total_conversions"],
            "success_rate": (app_stats["successful_conversions"] / max(1, app_stats["total_conversions"])) * 100
        }
        
        return templates.TemplateResponse("main_template.html", context)
        
    except Exception as e:
        print(f"Template error: {e}")
        return HTMLResponse(f"<h1>Alteryx PySpark Converter</h1><p>Upload your .yxmd file to convert to PySpark</p><p>Error: {e}</p>", status_code=200)


@app.post("/api/upload")
async def upload_workflow(file: UploadFile = File(...)):
    """Upload and analyze an Alteryx workflow file."""
    
    if not file.filename.endswith(('.yxmd', '.yxwz')):
        raise HTTPException(
            status_code=400,
            detail="Invalid file type. Only .yxmd and .yxwz files are supported."
        )
    
    # Save uploaded file temporarily
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.yxmd') as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name
        
        print(f"Uploaded file: {file.filename} ({file.size} bytes)")
        
        # Read original XML content for display
        try:
            with open(temp_file_path, 'r', encoding='utf-8') as f:
                original_xml = f.read()
        except:
            original_xml = "Unable to read XML content"
        
        # Parse and analyze the workflow using simple converter
        try:
            result = converter.convert_workflow(temp_file_path)
        except Exception as conversion_error:
            print(f"Conversion error details: {conversion_error}")
            raise HTTPException(status_code=400, detail=f"Failed to convert workflow: {str(conversion_error)}")
        
        if result and result.get('success', False):
            tools = result.get('tools', [])
            
            # Calculate support statistics
            supported_count = 0
            unsupported_tools = []
            
            for tool in tools:
                tool_type = tool.get('type', '')
                if converter._is_tool_supported(tool_type):
                    supported_count += 1
                else:
                    unsupported_tools.append({
                        'id': tool.get('id', ''),
                        'type': tool_type,
                        'name': tool.get('name', tool_type)
                    })
            
            support_rate = (supported_count / len(tools) * 100) if tools else 0
            
            # Update app statistics
            app_stats["total_conversions"] += 1
            app_stats["total_tools_processed"] += len(tools)
            
            response_data = {
                "success": True,
                "workflow_name": Path(file.filename).stem,
                "file_name": file.filename,
                "total_tools": len(tools),
                "supported_tools": supported_count,
                "unsupported_tools": len(tools) - supported_count,
                "support_rate": round(support_rate, 2),
                "tools": tools,
                "connections": result.get('connections', []),
                "unsupported_tool_list": unsupported_tools,
                "workflow_steps": result.get('workflow_steps', []),
                "temp_file_path": temp_file_path,
                "original_filename": Path(file.filename).stem,
                "original_xml": original_xml
            }
            
            return JSONResponse(response_data)
        else:
            raise HTTPException(status_code=400, detail="Failed to parse workflow file")
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Upload error: {e}")
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
        output_format = body.get('output_format', 'pyspark')
        original_filename = body.get('original_filename', 'workflow')
        
        if not temp_file_path or not os.path.exists(temp_file_path):
            raise HTTPException(
                status_code=400,
                detail="Invalid or missing workflow file"
            )
        
        # Perform conversion with enhanced workflow analysis
        start_time = time.time()
        result = converter.convert_workflow(temp_file_path)
        conversion_time = time.time() - start_time
        
        # Format code based on output format
        if result and result.get('success', False):
            original_code = result.get('code', '')
            # Use original filename instead of temp file name
            workflow_name = original_filename or result.get('workflow_name', 'workflow')
            result['code'] = format_code_for_platform(original_code, output_format, workflow_name)
            
            # Update filename based on output format
            if output_format == 'jupyter':
                result['suggested_filename'] = f"{workflow_name}.ipynb"
            elif output_format == 'databricks':
                result['suggested_filename'] = f"{workflow_name}_databricks.py"
            else:
                result['suggested_filename'] = f"{workflow_name}.py"
        
        # Cleanup temp file
        try:
            os.unlink(temp_file_path)
        except:
            pass
        
        # Update app statistics
        if result and result.get('success', False):
            app_stats["successful_conversions"] += 1
        else:
            app_stats["failed_conversions"] += 1
        
        # Add performance metrics
        if result:
            result['conversion_time'] = conversion_time
            result['timestamp'] = time.time()
        
        print(f"Conversion completed: Success: {result.get('success', False)}, Time: {conversion_time:.2f}s")
        
        return JSONResponse(result or {"success": False, "error": "Conversion failed"})
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Conversion error: {e}")
        app_stats["failed_conversions"] += 1
        
        raise HTTPException(
            status_code=500,
            detail=f"Conversion failed: {str(e)}"
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
        
        # Determine file extension based on filename
        file_extension = '.py'
        if filename.endswith('.ipynb'):
            file_extension = '.ipynb'
        elif filename.endswith('.sql'):
            file_extension = '.sql'
        
        # Create temporary file with the code
        with tempfile.NamedTemporaryFile(mode='w', suffix=file_extension, delete=False) as temp_file:
            temp_file.write(code)
            temp_file_path = temp_file.name
        
        # Determine the correct media type based on file extension
        if filename.endswith('.ipynb'):
            media_type = 'application/json'
        elif filename.endswith('.py'):
            media_type = 'text/x-python'
        elif filename.endswith('.sql'):
            media_type = 'text/sql'
        else:
            media_type = 'application/octet-stream'
        
        return FileResponse(
            path=temp_file_path,
            filename=filename,
            media_type=media_type
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Download error: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to prepare download"
        )


@app.get("/api/tool-reference")
async def get_tool_reference():
    """Get tool reference data for the knowledge base."""
    
    # Read the tool knowledge base CSV
    import csv
    from pathlib import Path
    
    csv_path = Path(__file__).parent / "tool_knowledge_base.csv"
    tools = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                tools.append({
                    "name": row['Tool Name'],
                    "description": row['Description'],
                    "functions": row['PySpark Functions'].replace(' ', '\n'),
                    "usage": row['Usage Count'],
                    "complexity": row['Complexity'],
                    "example": row['Example Use Case']
                })
    except Exception as e:
        print(f"Error reading tool knowledge base: {e}")
        return []
    
    return tools


@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring."""
    
    return {
        "status": "healthy",
        "app_name": "Alteryx PySpark Converter",
        "version": "2.0.0",
        "timestamp": time.time(),
        "uptime_seconds": time.time() - app_stats["app_start_time"]
    }


if __name__ == "__main__":
    print("Starting Alteryx PySpark Converter...")
    uvicorn.run("main_app:app", host="localhost", port=8000, reload=True)