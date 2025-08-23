import requests
import json
import os

# Test OpenAI analysis
sample_data = {
    "xml_content": """<?xml version="1.0"?>
<AlteryxDocument>
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput">
        <Configuration><File>customers.csv</File></Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Filter.Filter">
        <Configuration><Expression>Age > 25 AND Status = 'Active'</Expression></Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Summarize.Summarize">
        <Configuration>
          <GroupByFields>
            <Field field="Region"/>
          </GroupByFields>
          <SummarizeFields>
            <Field field="Sales" action="Sum"/>
            <Field field="CustomerID" action="Count"/>
          </SummarizeFields>
        </Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="4">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileOutput.DbFileOutput">
        <Configuration><File>summary_report.csv</File></Configuration>
      </GuiSettings>
    </Node>
  </Nodes>
</AlteryxDocument>""",
    "pyspark_code": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark
spark = SparkSession.builder.appName("CustomerAnalysis").getOrCreate()

# Step 1: Read input data
df = spark.read.csv("customers.csv", header=True, inferSchema=True)

# Step 2: Filter records
df = df.filter((F.col("Age") > 25) & (F.col("Status") == "Active"))

# Step 3: Summarize data
df = df.groupBy("Region").agg(
    F.sum("Sales").alias("Total_Sales"),
    F.count("CustomerID").alias("Customer_Count")
)

# Step 4: Write output
df.write.mode("overwrite").csv("summary_report.csv", header=True)""",
    "workflow_info": {
        "total_tools": 4,
        "tools": [
            {"type": "Input Data", "id": "1"},
            {"type": "Filter", "id": "2"},
            {"type": "Summarize", "id": "3"},
            {"type": "Output Data", "id": "4"}
        ]
    },
    "use_ai": True,
    "use_ollama": False,
    "api_key": os.getenv("OPENAI_API_KEY", "")
}

print("Testing OpenAI Analysis...")
print("Using API Key:", "sk-...asA" if sample_data["api_key"] else "No key")

response = requests.post("http://localhost:8000/api/analyze", json=sample_data, timeout=30)

if response.status_code == 200:
    result = response.json()
    print("\n✅ SUCCESS!")
    print("="*60)
    print("ANALYSIS REPORT")
    print("="*60)
    print(f"Score: {result.get('score')}/10")
    print(f"Source: {result.get('analysis', {}).get('source', 'unknown')}")
    
    print("\nReport Preview:")
    report = result.get('report', '')
    print(report[:2000] if len(report) > 2000 else report)
else:
    print(f"\n❌ Error {response.status_code}:")
    print(response.json())
