#!/bin/bash

echo "🦙 Setting up Ollama for AI Code Analysis"
echo "=========================================="
echo ""

# Check if Ollama is installed
if ! command -v ollama &> /dev/null; then
    echo "❌ Ollama is not installed."
    echo ""
    echo "📦 To install Ollama:"
    echo ""
    
    # Detect OS
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "For macOS:"
        echo "  brew install ollama"
        echo "  OR"
        echo "  Download from: https://ollama.ai/download"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo "For Linux:"
        echo "  curl -fsSL https://ollama.ai/install.sh | sh"
    else
        echo "Visit: https://ollama.ai/download"
    fi
    
    echo ""
    echo "After installation, run this script again."
    exit 1
fi

echo "✅ Ollama is installed!"
echo ""

# Start Ollama service
echo "🚀 Starting Ollama service..."
ollama serve &
OLLAMA_PID=$!
sleep 3

# Check if Ollama is running
if ! curl -s http://localhost:11434/api/tags > /dev/null 2>&1; then
    echo "⚠️  Ollama service might already be running or failed to start."
    echo "   Try: ollama serve"
else
    echo "✅ Ollama service is running!"
fi

echo ""
echo "📦 Pulling recommended models for code analysis..."
echo ""

# Pull recommended models
models=("llama2" "codellama" "mistral")
for model in "${models[@]}"; do
    echo "Downloading $model..."
    ollama pull $model
done

echo ""
echo "✅ Setup complete!"
echo ""
echo "Available models:"
ollama list

echo ""
echo "🎉 You can now use Ollama for FREE AI code analysis!"
echo "   Just click the '🦙 Ollama (Free)' button in the AI Analysis tab."
echo ""
echo "To stop Ollama service: kill $OLLAMA_PID"