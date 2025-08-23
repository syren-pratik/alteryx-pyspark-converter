#!/usr/bin/env python3
import requests
import json

# Test CodeLlama:7b with a complex workflow
test_data = {
    "xml_content": """<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2022.3">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput">
        <Configuration>
          <File>customers.csv</File>
          <FormatSpecificOptions>
            <HeaderRow>True</HeaderRow>
            <IgnoreErrors>False</IgnoreErrors>
            <AllowShareWrite>False</AllowShareWrite>
          </FormatSpecificOptions>
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
      <GuiSettings Plugin="AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect">
        <Configuration>
          <SelectFields>
            <Field field="CustomerID" selected="True"/>
            <Field field="Name" selected="True"/>
            <Field field="Age" selected="True"/>
            <Field field="Region" selected="True"/>
            <Field field="Sales" selected="True"/>
            <Field field="Status" selected="False"/>
          </SelectFields>
        </Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="4">
      <GuiSettings Plugin="AlteryxSummaryPluginsGui.Summarize.Summarize">
        <Configuration>
          <GroupByFields>
            <Field field="Region"/>
          </GroupByFields>
          <SummarizeFields>
            <Field field="Sales" action="Sum" rename="Total_Sales"/>
            <Field field="CustomerID" action="Count" rename="Customer_Count"/>
            <Field field="Age" action="Avg" rename="Avg_Age"/>
          </SummarizeFields>
        </Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="5">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Sort.Sort">
        <Configuration>
          <SortInfo>
            <Field field="Total_Sales" order="Desc"/>
          </SortInfo>
        </Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="6">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileOutput.DbFileOutput">
        <Configuration>
          <File>regional_summary.csv</File>
          <CreateNewFile>True</CreateNewFile>
        </Configuration>
      </GuiSettings>
    </Node>
  </Nodes>
</AlteryxDocument>""",
    "pyspark_code": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark
spark = SparkSession.builder.appName("CustomerAnalysis").getOrCreate()

# Step 1: Read input data
df_1 = spark.read.csv("customers.csv", header=True, inferSchema=True)

# Step 2: Filter records
df_2 = df_1.filter((F.col("Age") > 25) & (F.col("Status") == "Active"))

# Step 3: Select columns
df_3 = df_2.select("CustomerID", "Name", "Age", "Region", "Sales")

# Step 4: Summarize data  
df_4 = df_3.groupBy("Region").agg(
    F.sum("Sales").alias("Total_Sales"),
    F.count("CustomerID").alias("Customer_Count"),
    F.avg("Age").alias("Avg_Age")
)

# Step 5: Sort results
df_5 = df_4.orderBy(F.col("Total_Sales").desc())

# Step 6: Write output
df_5.write.mode("overwrite").csv("regional_summary.csv", header=True)

# Stop Spark session
spark.stop()""",
    "workflow_info": {
        "total_tools": 6,
        "tools": [
            {"type": "Input Data", "id": "1"},
            {"type": "Filter", "id": "2"},
            {"type": "Select", "id": "3"},
            {"type": "Summarize", "id": "4"},
            {"type": "Sort", "id": "5"},
            {"type": "Output Data", "id": "6"}
        ]
    },
    "use_ai": True,
    "use_ollama": True,  # Force Ollama to use CodeLlama
    "api_key": None
}

print("Testing CodeLlama:7b Analysis...")
print("="*60)

try:
    response = requests.post("http://localhost:8000/api/analyze", json=test_data, timeout=120)
    
    if response.status_code == 200:
        result = response.json()
        
        # Show analysis details
        analysis = result.get('analysis', {})
        print(f"✅ Analysis Source: {analysis.get('source', 'unknown')}")
        print(f"✅ Quality Score: {result.get('score', 0)}/10")
        print(f"✅ Model Used: {analysis.get('model', 'unknown')}")
        
        print("\n" + "="*60)
        print("FULL ANALYSIS REPORT")
        print("="*60)
        print(result.get('report', 'No report generated'))
        
        # Show if the analysis is more detailed than before
        if analysis.get('issues'):
            print("\n🔍 Issues Found:", len(analysis.get('issues', [])))
        if analysis.get('optimizations'):
            print("⚡ Optimizations:", len(analysis.get('optimizations', [])))
        if analysis.get('missing_features'):
            print("❌ Missing Features:", len(analysis.get('missing_features', [])))
            
    else:
        print(f"❌ Error {response.status_code}: {response.text}")
        
except Exception as e:
    print(f"❌ Request failed: {e}")