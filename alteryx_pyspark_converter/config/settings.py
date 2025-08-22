"""
Configuration settings for the Alteryx PySpark Converter.

Centralized configuration management for the entire application.
"""

import os
from pathlib import Path
from typing import Dict, Any, List


class Settings:
    """Application settings and configuration."""
    
    # Application Info
    APP_NAME = "Alteryx PySpark Converter"
    APP_VERSION = "2.0.0"
    APP_DESCRIPTION = "Enterprise-grade converter for Alteryx workflows to PySpark code"
    
    # Paths
    BASE_DIR = Path(__file__).parent.parent.parent
    LOG_DIR = BASE_DIR / "logs"
    CONFIG_DIR = BASE_DIR / "config"
    TEMPLATES_DIR = BASE_DIR / "templates"
    
    # Logging Configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", "10485760"))  # 10MB
    LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))
    LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "30"))
    
    # Web Server Configuration
    WEB_HOST = os.getenv("WEB_HOST", "localhost")
    WEB_PORT = int(os.getenv("WEB_PORT", "8000"))
    WEB_DEBUG = os.getenv("WEB_DEBUG", "False").lower() == "true"
    WEB_RELOAD = os.getenv("WEB_RELOAD", "True").lower() == "true"
    
    # File Upload Configuration
    MAX_UPLOAD_SIZE = int(os.getenv("MAX_UPLOAD_SIZE", "52428800"))  # 50MB
    ALLOWED_EXTENSIONS = [".yxmd", ".yxwz"]
    TEMP_DIR = BASE_DIR / "temp"
    
    # Conversion Configuration
    DEFAULT_SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "AlteryxWorkflow")
    ENABLE_BROADCAST_HINTS = os.getenv("ENABLE_BROADCAST_HINTS", "True").lower() == "true"
    ENABLE_CACHING_HINTS = os.getenv("ENABLE_CACHING_HINTS", "True").lower() == "true"
    OUTPUT_CODE_STYLE = os.getenv("OUTPUT_CODE_STYLE", "formatted")  # formatted, compact, commented
    
    # Analytics Configuration
    ENABLE_ANALYTICS = os.getenv("ENABLE_ANALYTICS", "True").lower() == "true"
    ENABLE_PERFORMANCE_TRACKING = os.getenv("ENABLE_PERFORMANCE_TRACKING", "True").lower() == "true"
    ANALYTICS_EXPORT_FORMAT = os.getenv("ANALYTICS_EXPORT_FORMAT", "json")  # json, csv, excel
    
    # Connector Configuration
    AUTO_DISCOVER_CONNECTORS = os.getenv("AUTO_DISCOVER_CONNECTORS", "True").lower() == "true"
    CONNECTOR_TIMEOUT = int(os.getenv("CONNECTOR_TIMEOUT", "30"))  # seconds
    
    # Error Handling Configuration
    STRICT_MODE = os.getenv("STRICT_MODE", "False").lower() == "true"
    CONTINUE_ON_ERROR = os.getenv("CONTINUE_ON_ERROR", "True").lower() == "true"
    MAX_ERRORS_PER_WORKFLOW = int(os.getenv("MAX_ERRORS_PER_WORKFLOW", "10"))
    
    # PySpark Code Generation Settings
    PYSPARK_CONFIG = {
        "spark.app.name": DEFAULT_SPARK_APP_NAME,
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }
    
    # Tool Priority Configuration (for dependency resolution)
    TOOL_PRIORITIES = {
        "AlteryxBasePluginsGui.DbFileInput.DbFileInput": 1,  # Input tools first
        "AlteryxBasePluginsGui.TextInput.TextInput": 1,
        "AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect": 2,
        "AlteryxBasePluginsGui.Filter.Filter": 3,
        "AlteryxBasePluginsGui.Formula.Formula": 4,
        "AlteryxBasePluginsGui.Join.Join": 5,
        "AlteryxBasePluginsGui.Union.Union": 5,
        "AlteryxBasePluginsGui.Sort.Sort": 6,
        "AlteryxBasePluginsGui.Unique.Unique": 7,
        "AlteryxBasePluginsGui.Summarize.Summarize": 8,
        "AlteryxBasePluginsGui.DbFileOutput.DbFileOutput": 10,  # Output tools last
        "AlteryxBasePluginsGui.BrowseV2.BrowseV2": 10
    }
    
    # UI Configuration
    UI_THEME = os.getenv("UI_THEME", "dark")  # dark, light, auto
    UI_ANIMATION_SPEED = int(os.getenv("UI_ANIMATION_SPEED", "1000"))  # milliseconds
    UI_MAX_WORKFLOW_NODES = int(os.getenv("UI_MAX_WORKFLOW_NODES", "100"))
    
    # Performance Configuration
    ENABLE_PARALLEL_PROCESSING = os.getenv("ENABLE_PARALLEL_PROCESSING", "True").lower() == "true"
    MAX_WORKER_THREADS = int(os.getenv("MAX_WORKER_THREADS", "4"))
    CONVERSION_TIMEOUT = int(os.getenv("CONVERSION_TIMEOUT", "300"))  # seconds
    
    # Security Configuration
    ENABLE_FILE_VALIDATION = os.getenv("ENABLE_FILE_VALIDATION", "True").lower() == "true"
    SCAN_UPLOADED_FILES = os.getenv("SCAN_UPLOADED_FILES", "True").lower() == "true"
    ALLOWED_FILE_PATTERNS = ["*.yxmd", "*.yxwz"]
    
    @classmethod
    def ensure_directories(cls):
        """Ensure all required directories exist."""
        directories = [
            cls.LOG_DIR,
            cls.TEMP_DIR,
            cls.CONFIG_DIR
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_pyspark_session_code(cls) -> str:
        """Generate PySpark session initialization code."""
        config_lines = []
        for key, value in cls.PYSPARK_CONFIG.items():
            config_lines.append(f'    .config("{key}", "{value}")')
        
        return f'''# Initialize Spark Session
spark = SparkSession.builder \\
    .appName("{cls.DEFAULT_SPARK_APP_NAME}") \\
{''.join(config_lines)} \\
    .getOrCreate()

# Set log level to reduce verbose output
spark.sparkContext.setLogLevel("WARN")
'''
    
    @classmethod
    def get_environment_info(cls) -> Dict[str, Any]:
        """Get current environment configuration."""
        return {
            "app_name": cls.APP_NAME,
            "app_version": cls.APP_VERSION,
            "python_version": os.sys.version,
            "log_level": cls.LOG_LEVEL,
            "web_host": cls.WEB_HOST,
            "web_port": cls.WEB_PORT,
            "max_upload_size": cls.MAX_UPLOAD_SIZE,
            "analytics_enabled": cls.ENABLE_ANALYTICS,
            "strict_mode": cls.STRICT_MODE,
            "ui_theme": cls.UI_THEME
        }
    
    @classmethod
    def validate_settings(cls) -> List[str]:
        """Validate current settings and return any issues."""
        issues = []
        
        # Check directory permissions
        if not cls.LOG_DIR.exists():
            try:
                cls.LOG_DIR.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                issues.append(f"Cannot create log directory: {e}")
        
        # Check port availability
        if cls.WEB_PORT < 1024 and os.geteuid() != 0:
            issues.append(f"Port {cls.WEB_PORT} requires root privileges")
        
        # Check upload size limits
        if cls.MAX_UPLOAD_SIZE < 1024:
            issues.append("Upload size limit too small (minimum 1KB)")
        
        # Check worker thread limits
        if cls.MAX_WORKER_THREADS > 16:
            issues.append("Too many worker threads may degrade performance")
        
        return issues


# Initialize directories on import
Settings.ensure_directories()