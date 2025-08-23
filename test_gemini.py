#!/usr/bin/env python3
"""Test Google Gemini API integration"""

import requests
import json

# Test server endpoint
BASE_URL = "http://localhost:8000"

# Sample test data
sample_xml = """<?xml version="1.0"?>
<AlteryxDocument>
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput">
        <Configuration>
          <File>customers.csv</File>
        </Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Filter.Filter">
        <Configuration>
          <Expression>Age > 25</Expression>
        </Configuration>
      </GuiSettings>
    </Node>
  </Nodes>
</AlteryxDocument>"""

sample_pyspark = """
# Step 1: Input
df_1 = spark.read.csv("customers.csv", header=True, inferSchema=True)

# Step 2: Filter
df_2 = df_1.filter(df_1["Age"] > 25)

# Final output
df_2.show()
"""

print("Testing Google Gemini API Integration...")
print("=" * 60)

try:
    # Test Gemini analysis
    response = requests.post(
        f"{BASE_URL}/api/analyze",
        json={
            "xml_content": sample_xml,
            "pyspark_code": sample_pyspark,
            "workflow_info": {
                "total_tools": 2,
                "tools": [
                    {"type": "DbFileInput"},
                    {"type": "Filter"}
                ]
            },
            "model_provider": "gemini",
            "gemini_api_key": "AIzaSyAaCNYpfIslFxLsL9kO-6mRmVVqNentwBE"
        },
        timeout=30
    )
    
    if response.status_code == 200:
        result = response.json()
        print("✅ Gemini Analysis Successful!")
        print(f"✅ Analysis Source: {result.get('source', 'unknown')}")
        print(f"✅ Quality Score: {result.get('score', 'N/A')}/10")
        
        # Check for AI features
        if result.get('workflow_description'):
            print(f"✅ Workflow Description: {result['workflow_description'][:100]}...")
        
        if result.get('issues'):
            print(f"✅ Issues Found: {len(result['issues'])}")
            
        if result.get('optimizations'):
            print(f"✅ Optimizations: {len(result['optimizations'])}")
            
        if result.get('report'):
            print("\n📝 Analysis Report Preview:")
            print("-" * 40)
            print(result['report'][:500])
            print("-" * 40)
    else:
        print(f"❌ Error: Status code {response.status_code}")
        print(f"   Response: {response.text}")
        
except requests.exceptions.Timeout:
    print("❌ Request timed out")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "=" * 60)
print("Gemini test complete!")