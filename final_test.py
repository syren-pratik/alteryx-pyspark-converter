import requests
import json

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
        <Configuration><Expression>Age > 25</Expression></Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="3">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileOutput.DbFileOutput">
        <Configuration><File>filtered_customers.csv</File></Configuration>
      </GuiSettings>
    </Node>
  </Nodes>
</AlteryxDocument>""",
    "pyspark_code": """from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CustomerFilter").getOrCreate()

# Read input
df = spark.read.csv("customers.csv", header=True)

# Filter records
df = df.filter(df.Age > 25)

# Write output
df.write.csv("filtered_customers.csv")""",
    "workflow_info": {
        "total_tools": 3,
        "tools": [{"type": "Input"}, {"type": "Filter"}, {"type": "Output"}]
    },
    "use_ollama": True
}

response = requests.post("http://localhost:8000/api/analyze", json=sample_data, timeout=90)
if response.status_code == 200:
    result = response.json()
    print(result.get('report', 'No report'))
else:
    print(f"Error: {response.status_code}")
