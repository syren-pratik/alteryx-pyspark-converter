#!/bin/bash

echo "🚀 Setting up Alteryx PySpark Converter..."

# Install Python dependencies
echo "📦 Installing Python packages..."
pip install -r requirements.txt

# Create necessary directories
mkdir -p logs temp

echo "✅ Setup complete!"
echo ""
echo "🌟 To start the application, run:"
echo "   python main_app.py"
echo ""
echo "🌐 The app will be available at: http://localhost:8000"
echo "   (Codespaces will automatically forward the port)"