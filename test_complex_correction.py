#!/usr/bin/env python3
"""Test complex workflow correction with common Alteryx conversion issues"""

import requests
import json

BASE_URL = "http://localhost:8000"

# Complex Alteryx XML with CrossTab, Multi-Row Formula, etc.
complex_xml = """<?xml version="1.0"?>
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
          <Expression>Amount > 1000 AND (Region = 'North' OR Region = 'South')</Expression>
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
      <GuiSettings Plugin="AlteryxBasePluginsGui.MultiRowFormula.MultiRowFormula">
        <Configuration>
          <UpdateField>RunningTotal</UpdateField>
          <Expression>[Row-1:RunningTotal] + [Amount]</Expression>
        </Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="5">
      <GuiSettings Plugin="AlteryxBasePluginsGui.TextToColumns.TextToColumns">
        <Configuration>
          <Field>Description</Field>
          <Delimeters>,</Delimeters>
        </Configuration>
      </GuiSettings>
    </Node>
  </Nodes>
</AlteryxDocument>"""

# Flawed PySpark code with typical auto-generation issues
flawed_code = """
# Step 1: Input
df_1 = spark.read.csv("sales_data.csv")

# Step 2: Filter  
df_2_T = df_1.filter(df_1["Amount"] > 1000)
df_2_F = df_1.filter(~(df_1["Amount"] > 1000))

# Step 3: CrossTab (WRONG - using groupBy instead of pivot)
df_3 = df_2_T.groupBy("Product").agg(concat_ws(",", collect_list("Region")))

# Step 4: Multi-Row Formula (WRONG - not using Window functions)
df_4 = df_3.withColumn("RunningTotal", col("Amount"))

# Step 5: Text to Columns (WRONG - not using explode)
df_5 = df_4.withColumn("Description_split", split(col("Description"), ","))

# Output
df_5.show()
"""

print("Testing Complex Workflow Correction...")
print("=" * 60)

try:
    response = requests.post(
        f"{BASE_URL}/api/correct",
        json={
            "xml_content": complex_xml,
            "pyspark_code": flawed_code,
            "workflow_info": {
                "total_tools": 5,
                "tools": [
                    {"type": "Input"},
                    {"type": "Filter"},
                    {"type": "CrossTab"},
                    {"type": "MultiRowFormula"},
                    {"type": "TextToColumns"}
                ]
            },
            "model_provider": "gemini",
            "gemini_api_key": "AIzaSyAaCNYpfIslFxLsL9kO-6mRmVVqNentwBE"
        },
        timeout=60
    )
    
    if response.status_code == 200:
        result = response.json()
        
        if result.get('success'):
            print("✅ Correction Successful!")
            print(f"✅ Accuracy: {result.get('accuracy_score', 0)}%")
            
            # Show improvements
            improvements = result.get('improvements', [])
            if improvements:
                print(f"\n🔧 Key Fixes Applied ({len(improvements)}):")
                for imp in improvements[:5]:
                    print(f"   • {imp}")
            
            # Check for specific fixes
            corrected_code = result.get('corrected_code', '')
            
            print("\n📊 Verification of Fixes:")
            print(f"   ✓ Uses .pivot(): {'pivot(' in corrected_code}")
            print(f"   ✓ Uses Window functions: {'Window' in corrected_code}")
            print(f"   ✓ Uses explode(): {'explode(' in corrected_code}")
            print(f"   ✓ Better variable names: {'df_' not in corrected_code or 'sales' in corrected_code}")
            print(f"   ✓ Has proper imports: {'from pyspark.sql' in corrected_code}")
            
            # Show a snippet of corrected code
            print("\n📝 Corrected Code Preview:")
            print("-" * 40)
            lines = corrected_code.split('\n')
            for line in lines[:20]:
                if line.strip():
                    print(line)
            print("-" * 40)
            
        else:
            print("❌ Correction failed")
    else:
        print(f"❌ Error: {response.status_code}")
        
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "=" * 60)
print("Complex correction test complete!")