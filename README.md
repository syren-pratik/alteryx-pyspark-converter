# 🔄 Alteryx to PySpark Converter

A comprehensive web-based tool that converts Alteryx workflows (.yxmd files) to production-ready PySpark code.

## ✨ Features

- **🎯 Smart Conversion**: Converts 20+ Alteryx tools to PySpark equivalents
- **🔧 Expression Parsing**: Advanced formula and expression conversion  
- **📊 Visual Interface**: Professional web UI with drag-and-drop upload
- **📱 Multiple Formats**: Export to Python, Jupyter Notebook, or Databricks
- **🔍 Tool Knowledge Base**: Interactive reference with 34 supported tools

## 🚀 Quick Start

### Option 1: GitHub Codespaces (Recommended)
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/syren-pratik/alteryx-pyspark-converter)

1. Click the badge above or go to your repository
2. Click **Code** → **Codespaces** → **Create codespace on main**
3. Wait for setup to complete (2-3 minutes)
4. Run: `python main_app.py`
5. Open the forwarded port when prompted

### Option 2: Local Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Run application
python main_app.py

# Open browser to http://localhost:8000
```

## 🛠️ Supported Tools

✅ Input Data, Output Data, Filter, Formula, Join, Union, Select, Sort, Unique, Browse, and 10+ more!

## 📖 Example Conversion

**Alteryx Formula:** `[Amount] * 1.1`  
**PySpark Output:** `df.withColumn("Amount", col("Amount") * lit(1.1))`

