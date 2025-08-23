#!/usr/bin/env python3
"""Test web upload functionality with AI analysis"""

import requests
import json

# Test server endpoint
BASE_URL = "http://localhost:8000"

# Sample Alteryx XML
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
    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileOutput.DbFileOutput">
        <Configuration>
          <File>filtered_customers.csv</File>
        </Configuration>
      </GuiSettings>
    </Node>
  </Nodes>
  <Connections>
    <Connection>
      <Origin ToolID="1" Connection="Output"/>
      <Destination ToolID="2" Connection="Input"/>
    </Connection>
    <Connection>
      <Origin ToolID="2" Connection="True"/>
      <Destination ToolID="3" Connection="Input"/>
    </Connection>
  </Connections>
</AlteryxDocument>"""

# Test the conversion endpoint directly
print("Testing /api/convert endpoint with AI analysis...")
print("=" * 60)

try:
    response = requests.post(
        f"{BASE_URL}/api/convert",
        json={
            "xml_content": sample_xml,
            "use_ai": True,
            "use_ollama": True
        },
        timeout=60
    )
    
    if response.status_code == 200:
        result = response.json()
        print("✅ Conversion successful!")
        print(f"✅ PySpark code generated: {len(result.get('pyspark_code', ''))} characters")
        
        if 'ai_analysis' in result:
            analysis = result['ai_analysis']
            print(f"✅ AI Analysis included")
            print(f"   - Source: {analysis.get('source', 'unknown')}")
            print(f"   - Score: {analysis.get('score', 'N/A')}/10")
            print(f"   - Issues found: {len(analysis.get('issues', []))}")
            print(f"   - Optimizations: {len(analysis.get('optimizations', []))}")
            
            # Print generated code snippet
            print("\n📝 Generated PySpark Code (first 500 chars):")
            print("-" * 40)
            print(result['pyspark_code'][:500])
            print("-" * 40)
            
            # Print AI recommendations if available
            if 'corrections' in analysis and analysis['corrections']:
                print("\n🔍 AI Corrections:")
                for corr in analysis['corrections'][:3]:
                    print(f"   • {corr}")
                    
        else:
            print("⚠️ No AI analysis included in response")
    else:
        print(f"❌ Error: Status code {response.status_code}")
        print(f"   Response: {response.text}")
        
except requests.exceptions.Timeout:
    print("❌ Request timed out - AI analysis may be taking too long")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "=" * 60)
print("Web upload test complete!")