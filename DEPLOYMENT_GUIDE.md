# 🚀 Deployment Guide for Alteryx PySpark Converter

## Quick Start Options

### 1️⃣ **Local Network (5 minutes)**
Share with your team on local network:
```bash
# Install dependencies
pip install -r requirements.txt

# Run for network access
python3 run_production.py

# Access from any computer on network:
# http://YOUR-IP:8000
```

### 2️⃣ **Docker (10 minutes)**
Most reliable and portable option:
```bash
# Build and run with Docker Compose
docker-compose up -d

# Or manually:
docker build -t alteryx-converter .
docker run -p 8000:8000 alteryx-converter
```

### 3️⃣ **Cloud Deployment Options**

## ☁️ AWS EC2 Deployment
```bash
# 1. Launch EC2 instance (Ubuntu 22.04, t3.medium recommended)
# 2. Configure Security Group: Allow inbound TCP 8000 (or 80 with nginx)
# 3. SSH to instance and run:

# Install dependencies
sudo apt update
sudo apt install -y python3-pip git nginx

# Clone your code
git clone <your-repo-url>
cd alteryx_latest_code

# Install Python packages
pip3 install -r requirements.txt

# Setup as system service
sudo cp alteryx-converter.service /etc/systemd/system/
sudo systemctl enable alteryx-converter
sudo systemctl start alteryx-converter

# Setup Nginx (optional but recommended)
sudo cp nginx.conf /etc/nginx/sites-available/alteryx-converter
sudo ln -s /etc/nginx/sites-available/alteryx-converter /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

## 🔷 Azure App Service
```bash
# 1. Create Azure Web App (Python 3.9)
az webapp create --resource-group myRG --plan myPlan --name alteryx-converter --runtime "PYTHON:3.9"

# 2. Deploy using Azure CLI
az webapp up --name alteryx-converter --resource-group myRG --runtime "PYTHON:3.9"

# 3. Configure app settings
az webapp config appsettings set --name alteryx-converter --resource-group myRG --settings WEBSITES_PORT=8000
```

## 🟣 Heroku (Easiest)
```bash
# 1. Install Heroku CLI
# 2. Create Heroku app
heroku create alteryx-converter

# 3. Create Procfile
echo "web: uvicorn main_app:app --host 0.0.0.0 --port \$PORT" > Procfile

# 4. Deploy
git add .
git commit -m "Deploy to Heroku"
git push heroku main

# 5. Open app
heroku open
```

## 🟢 Google Cloud Run
```bash
# 1. Build container
gcloud builds submit --tag gcr.io/PROJECT_ID/alteryx-converter

# 2. Deploy to Cloud Run
gcloud run deploy alteryx-converter \
  --image gcr.io/PROJECT_ID/alteryx-converter \
  --platform managed \
  --allow-unauthenticated \
  --port 8000 \
  --memory 1Gi
```

## 🎯 Digital Ocean App Platform
```yaml
# app.yaml
name: alteryx-converter
services:
- name: web
  github:
    repo: your-username/your-repo
    branch: main
  run_command: uvicorn main_app:app --host 0.0.0.0 --port 8000
  http_port: 8000
  instance_count: 1
  instance_size_slug: basic-xs
```

## 🔒 Production Checklist

### Security
- [ ] Enable HTTPS with SSL certificate (Let's Encrypt)
- [ ] Set up authentication (if needed)
- [ ] Configure CORS properly
- [ ] Limit file upload sizes
- [ ] Add rate limiting

### Performance
- [ ] Configure multiple workers (4-8 recommended)
- [ ] Set up CDN for static files
- [ ] Enable response caching
- [ ] Configure database (if using)

### Monitoring
- [ ] Set up logging (CloudWatch, Azure Monitor, etc.)
- [ ] Configure health checks
- [ ] Set up alerts
- [ ] Monitor resource usage

### Backup
- [ ] Regular backup of uploaded files
- [ ] Database backups (if applicable)
- [ ] Configuration backup

## 📊 Recommended Specifications

| Environment | CPU | RAM | Storage | Users |
|------------|-----|-----|---------|-------|
| Development | 2 cores | 4GB | 20GB | 1-5 |
| Small Team | 4 cores | 8GB | 50GB | 5-20 |
| Department | 8 cores | 16GB | 100GB | 20-100 |
| Enterprise | 16+ cores | 32GB+ | 500GB+ | 100+ |

## 🔧 Environment Variables

Create `.env` file for production:
```env
APP_ENV=production
APP_HOST=0.0.0.0
APP_PORT=8000
MAX_UPLOAD_SIZE=100  # MB
WORKERS=4
LOG_LEVEL=info
ALLOWED_ORIGINS=*  # Configure for your domain
```

## 🚨 Troubleshooting

### Port Already in Use
```bash
# Find process using port 8000
lsof -i :8000
# Kill process
kill -9 <PID>
```

### Memory Issues with Large Files
```bash
# Increase Python memory limit
export PYTHON_MEMORY_LIMIT=4096
# Or use Docker with memory limits
docker run -m 4g alteryx-converter
```

### Slow Performance
- Increase number of workers
- Use production WSGI server (Gunicorn)
- Enable caching
- Use CDN for static files

## 📝 Maintenance

### Updates
```bash
# Pull latest code
git pull origin main

# Update dependencies
pip install -r requirements.txt --upgrade

# Restart service
sudo systemctl restart alteryx-converter
```

### Logs
```bash
# View application logs
sudo journalctl -u alteryx-converter -f

# Docker logs
docker logs alteryx-converter -f
```

## 🎉 Success Metrics

Monitor these KPIs:
- Response time < 2s for uploads
- Conversion success rate > 95%
- Uptime > 99.9%
- Memory usage < 80%
- CPU usage < 70%

## 📞 Support

For issues or questions:
1. Check logs first
2. Review error messages
3. Verify dependencies
4. Check network/firewall settings

---

**Ready to deploy! Choose the option that best fits your infrastructure.** 🚀