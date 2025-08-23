import requests
import json

# Test AI analysis with sample code
sample_data = {
    "xml_content": "<AlteryxDocument><Nodes></Nodes></AlteryxDocument>",
    "pyspark_code": """
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test").getOrCreate()
df = spark.read.csv("input.csv")
df = df.select("col1", "col2")
df = df.select("col1")
df.write.csv("output.csv")
""",
    "workflow_info": {
        "total_tools": 5,
        "tools": [{"type": "Input"}, {"type": "Select"}]
    },
    "use_ai": False,
    "use_ollama": True
}

response = requests.post("http://localhost:8000/api/analyze", json=sample_data)
print(f"Status: {response.status_code}")
print(json.dumps(response.json(), indent=2))
