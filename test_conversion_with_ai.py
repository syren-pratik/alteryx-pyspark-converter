#!/usr/bin/env python3
"""Test conversion with automatic AI correction"""

import requests
import json
import tempfile
import os

# Create a sample YXMD file
sample_yxmd = """<?xml version="1.0"?>
<AlteryxDocument>
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput">
        <Configuration>
          <File>sales_data.csv</File>
        </Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Filter.Filter">
        <Configuration>
          <Expression>Amount > 1000 AND Region = 'North'</Expression>
        </Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxSummaryPluginsGui.CrossTab.CrossTab">
        <Configuration>
          <GroupFields>
            <Field field="Product"/>
          </GroupFields>
          <HeaderField field="Region"/>
          <DataField field="Amount"/>
          <Methods>
            <Method>Sum</Method>
          </Methods>
        </Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="4">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileOutput.DbFileOutput">
        <Configuration>
          <File>output.csv</File>
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
    <Connection>
      <Origin ToolID="3" Connection="Output"/>
      <Destination ToolID="4" Connection="Input"/>
    </Connection>
  </Connections>
</AlteryxDocument>"""

# Save to temp file
with tempfile.NamedTemporaryFile(mode='w', suffix='.yxmd', delete=False) as f:
    f.write(sample_yxmd)
    temp_path = f.name

print("Testing Conversion with AI Optimization...")
print("=" * 60)

try:
    # First upload the file
    print("1. Uploading workflow file...")
    with open(temp_path, 'rb') as f:
        upload_response = requests.post(
            "http://localhost:8000/api/upload",
            files={'file': ('test.yxmd', f, 'text/xml')}
        )
    
    if upload_response.status_code == 200:
        upload_result = upload_response.json()
        print(f"   ✓ Upload successful: {upload_result.get('filename')}")
        
        # Now convert with AI optimization
        print("\n2. Converting with AI optimization...")
        convert_response = requests.post(
            "http://localhost:8000/api/convert",
            json={'temp_file_path': upload_result.get('temp_file_path')}
        )
        
        if convert_response.status_code == 200:
            result = convert_response.json()
            
            print("   ✓ Conversion successful!")
            
            # Check if AI optimization was applied
            if result.get('stats'):
                stats = result['stats']
                print(f"\n3. AI Optimization Results:")
                print(f"   • Accuracy Score: {stats.get('accuracy_score', 0)}%")
                print(f"   • AI Optimized: {stats.get('ai_optimized', False)}")
                print(f"   • Lines Generated: {stats.get('lines_generated', 0)}")
            
            # Show improvements if any
            if result.get('improvements'):
                print(f"\n4. Improvements Applied:")
                for imp in result['improvements'][:5]:
                    print(f"   ✓ {imp}")
            
            # Check for CrossTab pivot implementation
            code = result.get('pyspark_code', '')
            print(f"\n5. Code Quality Checks:")
            print(f"   • Uses pivot() for CrossTab: {'pivot(' in code}")
            print(f"   • Has proper imports: {'from pyspark.sql' in code}")
            print(f"   • Clean variable names: {'df_' not in code or 'sales' in code.lower()}")
            
            # Show a snippet of the code
            print(f"\n6. Generated Code Preview:")
            print("-" * 40)
            lines = code.split('\n')[:15]
            for line in lines:
                if line.strip():
                    print(f"   {line}")
            print("-" * 40)
            
            # Check logs
            if result.get('logs'):
                print(f"\n7. Conversion Logs:")
                for log_line in result['logs'].split('\n')[:10]:
                    if log_line.strip():
                        print(f"   {log_line}")
            
        else:
            print(f"   ✗ Conversion failed: {convert_response.status_code}")
            print(f"   Error: {convert_response.json()}")
    else:
        print(f"   ✗ Upload failed: {upload_response.status_code}")
        
finally:
    # Clean up temp file
    os.unlink(temp_path)

print("\n" + "=" * 60)
print("Test complete!")