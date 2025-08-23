import requests
import json

# Test with real Alteryx XML and corresponding PySpark code
sample_data = {
    "xml_content": """<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2022.3">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput">
        <Position x="50" y="50"/>
        <Annotation>
          <Name>Input Data</Name>
        </Annotation>
        <Configuration>
          <File>sales_data.csv</File>
        </Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect">
        <Position x="150" y="50"/>
        <Annotation>
          <Name>Select Columns</Name>
        </Annotation>
        <Configuration>
          <SelectFields>
            <Field field="CustomerID" selected="True"/>
            <Field field="Sales" selected="True"/>
            <Field field="Date" selected="True"/>
          </SelectFields>
        </Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxBasePluginsGui.Filter.Filter">
        <Position x="250" y="50"/>
        <Configuration>
          <Expression>Sales > 1000</Expression>
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
      <Origin ToolID="2" Connection="Output"/>
      <Destination ToolID="3" Connection="Input"/>
    </Connection>
  </Connections>
</AlteryxDocument>""",
    "pyspark_code": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("AlteryxWorkflow").getOrCreate()

# Step 1: Input Data
df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)

# Step 2: Select Columns
df = df.select("CustomerID", "Sales", "Date")

# Step 3: Filter
df = df.filter(F.col("Sales") > 1000)

# Show results
df.show()""",
    "workflow_info": {
        "total_tools": 3,
        "tools": [
            {"type": "Input Data", "id": "1"},
            {"type": "Select", "id": "2"},
            {"type": "Filter", "id": "3"}
        ]
    },
    "use_ai": False,
    "use_ollama": True
}

print("Sending full XML and code for comparison analysis...")
response = requests.post("http://localhost:8000/api/analyze", json=sample_data, timeout=90)
print(f"Status: {response.status_code}")

if response.status_code == 200:
    result = response.json()
    print("\n" + "="*60)
    print("ANALYSIS RESULTS")
    print("="*60)
    print(f"Score: {result.get('score', 'N/A')}/10")
    print(f"\nReport Preview:")
    print(result.get('report', 'No report')[:1000])
    
    if result.get('recommendations'):
        print(f"\nTop Recommendations: {len(result['recommendations'])}")
        for rec in result['recommendations'][:3]:
            print(f"  - {rec}")
else:
    print(json.dumps(response.json(), indent=2))
