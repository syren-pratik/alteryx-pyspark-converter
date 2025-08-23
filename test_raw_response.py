import requests
import json

# Same test data
sample_data = {
    "xml_content": """<?xml version="1.0"?>
<AlteryxDocument yxmdVer="2022.3">
  <Nodes>
    <Node ToolID="1">
      <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput">
        <Configuration><File>sales_data.csv</File></Configuration>
      </GuiSettings>
    </Node>
    <Node ToolID="2">
      <GuiSettings Plugin="AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect">
        <Configuration>
          <SelectFields>
            <Field field="CustomerID" selected="True"/>
            <Field field="Sales" selected="True"/>
          </SelectFields>
        </Configuration>
      </GuiSettings>
    </Node>
  </Nodes>
</AlteryxDocument>""",
    "pyspark_code": """from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test").getOrCreate()
df = spark.read.csv("sales_data.csv")
df = df.select("CustomerID", "Sales")
df.show()""",
    "workflow_info": {"total_tools": 2, "tools": [{"type": "Input"}, {"type": "Select"}]},
    "use_ollama": True
}

response = requests.post("http://localhost:8000/api/analyze", json=sample_data, timeout=90)
result = response.json()

print("RAW ANALYSIS OBJECT:")
print(json.dumps(result.get('analysis', {}), indent=2))
print("\n" + "="*60)
print("\nFULL REPORT:")
print(result.get('report', 'No report'))
