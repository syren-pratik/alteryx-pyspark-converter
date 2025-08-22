#!/bin/bash
# AWS EC2 Deployment Script

echo "🚀 Deploying to AWS EC2..."

# Variables
APP_NAME="alteryx-converter"
REGION="us-east-1"
INSTANCE_TYPE="t3.medium"

# Create requirements.txt if not exists
cat > requirements.txt << EOF
fastapi==0.104.1
uvicorn[standard]==0.24.0
pyspark==3.5.0
python-multipart==0.0.6
jinja2==3.1.2
xmltodict==0.13.0
EOF

# Create systemd service file
cat > alteryx-converter.service << EOF
[Unit]
Description=Alteryx PySpark Converter
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/alteryx-converter
Environment="PATH=/home/ubuntu/.local/bin:/usr/bin"
ExecStart=/usr/bin/python3 -m uvicorn main_app:app --host 0.0.0.0 --port 8000 --workers 4
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

echo "📦 Creating deployment package..."
tar -czf alteryx-converter.tar.gz \
    *.py \
    templates/ \
    *.csv \
    requirements.txt \
    alteryx-converter.service

echo "📤 Upload alteryx-converter.tar.gz to your EC2 instance"
echo ""
echo "Then SSH to your instance and run:"
echo "  tar -xzf alteryx-converter.tar.gz"
echo "  sudo apt update && sudo apt install -y python3-pip"
echo "  pip3 install -r requirements.txt"
echo "  sudo cp alteryx-converter.service /etc/systemd/system/"
echo "  sudo systemctl enable alteryx-converter"
echo "  sudo systemctl start alteryx-converter"
echo ""
echo "✅ Your app will be available at http://<EC2-IP>:8000"