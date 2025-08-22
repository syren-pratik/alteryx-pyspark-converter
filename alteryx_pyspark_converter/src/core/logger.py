"""
Comprehensive logging system for Alteryx PySpark Converter.

Tracks errors, tool usage statistics, performance metrics, and analytics.
"""

import logging
import json
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from collections import defaultdict, Counter
import threading
from pathlib import Path


class Logger:
    """Enterprise-grade logging system with analytics and metrics tracking."""
    
    def __init__(self, log_dir: str = "logs"):
        """Initialize the logging system."""
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # Thread-safe counters for analytics
        self._lock = threading.Lock()
        self._tool_usage_stats = defaultdict(int)
        self._error_stats = defaultdict(int)
        self._conversion_stats = {
            'total_conversions': 0,
            'successful_conversions': 0,
            'failed_conversions': 0,
            'total_tools_processed': 0,
            'supported_tools': 0,
            'unsupported_tools': 0
        }
        
        # Setup loggers
        self._setup_loggers()
        
    def _setup_loggers(self):
        """Configure different loggers for different purposes."""
        
        # Main application logger
        self.app_logger = self._create_logger(
            name='alteryx_converter',
            filename='application.log',
            level=logging.INFO,
            format_str='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Error tracking logger
        self.error_logger = self._create_logger(
            name='error_tracker',
            filename='errors.log',
            level=logging.ERROR,
            format_str='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        
        # Tool usage analytics logger
        self.analytics_logger = self._create_logger(
            name='analytics',
            filename='analytics.log',
            level=logging.INFO,
            format_str='%(asctime)s - ANALYTICS - %(message)s'
        )
        
        # Performance metrics logger
        self.performance_logger = self._create_logger(
            name='performance',
            filename='performance.log',
            level=logging.INFO,
            format_str='%(asctime)s - PERFORMANCE - %(message)s'
        )
        
        # Unsupported tools logger
        self.unsupported_logger = self._create_logger(
            name='unsupported_tools',
            filename='unsupported_tools.log',
            level=logging.WARNING,
            format_str='%(asctime)s - UNSUPPORTED - %(message)s'
        )
        
    def _create_logger(self, name: str, filename: str, level: int, format_str: str) -> logging.Logger:
        """Create a configured logger instance."""
        logger = logging.getLogger(name)
        logger.setLevel(level)
        
        # Avoid duplicate handlers
        if logger.handlers:
            return logger
            
        # File handler
        file_handler = logging.FileHandler(self.log_dir / filename)
        file_handler.setLevel(level)
        
        # Console handler for important logs
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.WARNING)
        
        # Formatter
        formatter = logging.Formatter(format_str)
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def log_conversion_start(self, workflow_file: str, total_tools: int):
        """Log the start of a workflow conversion."""
        self.app_logger.info(f"Starting conversion of workflow: {workflow_file}")
        self.app_logger.info(f"Total tools in workflow: {total_tools}")
        
        with self._lock:
            self._conversion_stats['total_conversions'] += 1
            self._conversion_stats['total_tools_processed'] += total_tools
    
    def log_conversion_success(self, workflow_file: str, supported_tools: int, unsupported_tools: int):
        """Log successful workflow conversion."""
        self.app_logger.info(f"Successfully converted workflow: {workflow_file}")
        self.app_logger.info(f"Supported tools: {supported_tools}, Unsupported tools: {unsupported_tools}")
        
        with self._lock:
            self._conversion_stats['successful_conversions'] += 1
            self._conversion_stats['supported_tools'] += supported_tools
            self._conversion_stats['unsupported_tools'] += unsupported_tools
            
        # Log analytics
        support_rate = (supported_tools / (supported_tools + unsupported_tools)) * 100 if (supported_tools + unsupported_tools) > 0 else 0
        self.analytics_logger.info(f"Conversion completed: {workflow_file}, Support rate: {support_rate:.2f}%")
    
    def log_conversion_failure(self, workflow_file: str, error: str):
        """Log failed workflow conversion."""
        self.error_logger.error(f"Failed to convert workflow: {workflow_file}, Error: {error}")
        
        with self._lock:
            self._conversion_stats['failed_conversions'] += 1
            self._error_stats[error] += 1
    
    def log_tool_usage(self, tool_type: str, tool_id: str, supported: bool):
        """Log usage of a specific tool."""
        with self._lock:
            self._tool_usage_stats[tool_type] += 1
            
        if supported:
            self.app_logger.debug(f"Processing supported tool: {tool_type} (ID: {tool_id})")
        else:
            self.unsupported_logger.warning(f"Encountered unsupported tool: {tool_type} (ID: {tool_id})")
            
        # Log to analytics
        self.analytics_logger.info(f"Tool used: {tool_type}, Supported: {supported}")
    
    def log_error(self, component: str, error_type: str, message: str, details: Optional[Dict] = None):
        """Log an error with detailed information."""
        error_info = {
            'component': component,
            'error_type': error_type,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'details': details or {}
        }
        
        self.error_logger.error(json.dumps(error_info))
        
        with self._lock:
            self._error_stats[f"{component}::{error_type}"] += 1
    
    def log_performance_metric(self, operation: str, duration_ms: float, details: Optional[Dict] = None):
        """Log performance metrics."""
        metric_info = {
            'operation': operation,
            'duration_ms': duration_ms,
            'timestamp': datetime.now().isoformat(),
            'details': details or {}
        }
        
        self.performance_logger.info(json.dumps(metric_info))
    
    def log_connector_registration(self, connector_name: str, tool_types: List[str]):
        """Log connector registration."""
        self.app_logger.info(f"Registered connector: {connector_name} for tools: {', '.join(tool_types)}")
    
    def get_tool_usage_statistics(self) -> Dict[str, Any]:
        """Get comprehensive tool usage statistics."""
        with self._lock:
            stats = dict(self._tool_usage_stats)
            
        # Calculate most and least used tools
        if stats:
            most_used = max(stats.items(), key=lambda x: x[1])
            least_used = min(stats.items(), key=lambda x: x[1])
        else:
            most_used = least_used = ("None", 0)
            
        return {
            'tool_usage_counts': stats,
            'most_used_tool': {'name': most_used[0], 'count': most_used[1]},
            'least_used_tool': {'name': least_used[0], 'count': least_used[1]},
            'total_tools_used': sum(stats.values()),
            'unique_tools_used': len(stats),
            'conversion_stats': dict(self._conversion_stats),
            'error_stats': dict(self._error_stats)
        }
    
    def export_analytics_report(self, output_file: Optional[str] = None) -> str:
        """Export comprehensive analytics report."""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.log_dir / f"analytics_report_{timestamp}.json"
        
        report = {
            'report_generated': datetime.now().isoformat(),
            'statistics': self.get_tool_usage_statistics(),
            'top_10_most_used_tools': self._get_top_tools(10),
            'top_10_errors': self._get_top_errors(10),
            'conversion_summary': self._get_conversion_summary()
        }
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
            
        self.app_logger.info(f"Analytics report exported to: {output_file}")
        return str(output_file)
    
    def _get_top_tools(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top N most used tools."""
        with self._lock:
            return [
                {'tool': tool, 'usage_count': count}
                for tool, count in Counter(self._tool_usage_stats).most_common(limit)
            ]
    
    def _get_top_errors(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top N most common errors."""
        with self._lock:
            return [
                {'error': error, 'occurrence_count': count}
                for error, count in Counter(self._error_stats).most_common(limit)
            ]
    
    def _get_conversion_summary(self) -> Dict[str, Any]:
        """Get conversion summary statistics."""
        with self._lock:
            stats = self._conversion_stats.copy()
            
        # Calculate success rate
        total = stats['total_conversions']
        success_rate = (stats['successful_conversions'] / total * 100) if total > 0 else 0
        
        # Calculate tool support rate
        total_tools = stats['supported_tools'] + stats['unsupported_tools']
        support_rate = (stats['supported_tools'] / total_tools * 100) if total_tools > 0 else 0
        
        return {
            **stats,
            'success_rate_percentage': round(success_rate, 2),
            'tool_support_rate_percentage': round(support_rate, 2),
            'average_tools_per_workflow': round(stats['total_tools_processed'] / total, 2) if total > 0 else 0
        }
    
    def cleanup_old_logs(self, days_to_keep: int = 30):
        """Clean up log files older than specified days."""
        cutoff_date = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)
        
        for log_file in self.log_dir.glob("*.log"):
            if log_file.stat().st_mtime < cutoff_date:
                log_file.unlink()
                self.app_logger.info(f"Cleaned up old log file: {log_file}")


# Global logger instance
logger_instance = None

def get_logger() -> Logger:
    """Get the global logger instance."""
    global logger_instance
    if logger_instance is None:
        logger_instance = Logger()
    return logger_instance