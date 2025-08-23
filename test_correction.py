#!/usr/bin/env python3
"""Test the new code correction feature"""

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
          <Expression>Age > 25 AND Status = 'Active'</Expression>
        </Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileOutput.DbFileOutput">
        <Configuration>
          <File>filtered_customers.csv</File>
        </Configuration>
      </GuiSettings>
    </Node>
  </Nodes>
</AlteryxDocument>"""

# Intentionally flawed PySpark code for correction
sample_pyspark = """
df_1 = spark.read.csv("customers.csv")
df_2 = df_1.filter(df_1["Age"] > 25)
df_2.show()
"""

print("Testing AI Code Correction Feature...")
print("=" * 60)

try:
    # Test code correction
    response = requests.post(
        f"{BASE_URL}/api/correct",
        json={
            "xml_content": sample_xml,
            "pyspark_code": sample_pyspark,
            "workflow_info": {
                "total_tools": 3,
                "tools": [
                    {"type": "DbFileInput"},
                    {"type": "Filter"},
                    {"type": "DbFileOutput"}
                ]
            },
            "model_provider": "gemini",
            "gemini_api_key": "AIzaSyAaCNYpfIslFxLsL9kO-6mRmVVqNentwBE"
        },
        timeout=45
    )
    
    if response.status_code == 200:
        result = response.json()
        
        if result.get('success'):
            print("✅ Code Correction Successful!")
            print(f"✅ Accuracy Score: {result.get('accuracy_score', 0)}%")
            print(f"✅ Model Provider: {result.get('model_provider', 'unknown')}")
            
            # Show corrections
            corrections = result.get('corrections', [])
            if corrections:
                print(f"\n📝 Corrections Applied ({len(corrections)}):")
                for corr in corrections[:3]:
                    print(f"   Line {corr.get('line', '?')}: {corr.get('original', '')[:30]}...")
                    print(f"   → {corr.get('corrected', '')[:50]}...")
            
            # Show improvements
            improvements = result.get('improvements', [])
            if improvements:
                print(f"\n✨ Improvements ({len(improvements)}):")
                for imp in improvements[:3]:
                    print(f"   • {imp}")
            
            # Compare code lengths
            original_lines = result.get('original_code', '').count('\n')
            corrected_lines = result.get('corrected_code', '').count('\n')
            print(f"\n📊 Code Stats:")
            print(f"   Original: {original_lines} lines")
            print(f"   Corrected: {corrected_lines} lines")
            
            # Show snippet of corrected code
            corrected = result.get('corrected_code', '')
            if corrected:
                print("\n🔧 Corrected Code Preview:")
                print("-" * 40)
                print('\n'.join(corrected.split('\n')[:10]))
                print("-" * 40)
        else:
            print("❌ Correction failed")
            print(f"   Error: {result.get('detail', 'Unknown error')}")
    else:
        print(f"❌ Error: Status code {response.status_code}")
        print(f"   Response: {response.text[:200]}")
        
except requests.exceptions.Timeout:
    print("❌ Request timed out")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "=" * 60)
print("Correction test complete!")