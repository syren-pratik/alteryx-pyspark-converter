#!/usr/bin/env python3
"""
Main application runner for the Alteryx PySpark Converter.

This script provides the entry point for running the converter application.
"""

import sys
import os
import argparse
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from alteryx_pyspark_converter.config.settings import Settings
from alteryx_pyspark_converter.src.core.logger import get_logger
from alteryx_pyspark_converter.src.api.main import run_server


def main():
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(
        description="Alteryx PySpark Converter - Enterprise-grade workflow conversion"
    )
    
    parser.add_argument(
        "--host",
        default=Settings.WEB_HOST,
        help=f"Host to bind the server (default: {Settings.WEB_HOST})"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=Settings.WEB_PORT,
        help=f"Port to bind the server (default: {Settings.WEB_PORT})"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=Settings.LOG_LEVEL,
        help=f"Log level (default: {Settings.LOG_LEVEL})"
    )
    
    parser.add_argument(
        "--analytics-only",
        action="store_true",
        help="Export analytics report and exit"
    )
    
    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Validate configuration and exit"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version=f"{Settings.APP_NAME} v{Settings.APP_VERSION}"
    )
    
    args = parser.parse_args()
    
    # Override settings with command line arguments
    Settings.WEB_HOST = args.host
    Settings.WEB_PORT = args.port
    Settings.LOG_LEVEL = args.log_level
    
    # Initialize logger
    logger = get_logger()
    
    if args.validate_config:
        validate_configuration(logger)
        return
    
    if args.analytics_only:
        export_analytics_and_exit(logger)
        return
    
    # Print startup banner
    print_banner()
    
    # Validate configuration
    issues = Settings.validate_settings()
    if issues:
        logger.app_logger.warning("Configuration issues detected:")
        for issue in issues:
            logger.app_logger.warning(f"  - {issue}")
        print()
    
    # Start the server
    try:
        logger.app_logger.info(f"Starting {Settings.APP_NAME} v{Settings.APP_VERSION}")
        logger.app_logger.info(f"Server will be available at: http://{Settings.WEB_HOST}:{Settings.WEB_PORT}")
        logger.app_logger.info("Press Ctrl+C to stop the server")
        
        run_server()
        
    except KeyboardInterrupt:
        logger.app_logger.info("Server stopped by user")
    except Exception as e:
        logger.app_logger.error(f"Failed to start server: {e}")
        sys.exit(1)


def print_banner():
    """Print application banner."""
    banner = f"""
╔══════════════════════════════════════════════════════════════════════╗
║                    {Settings.APP_NAME}                     ║
║                           Version {Settings.APP_VERSION}                           ║
║                                                                      ║
║  Enterprise-grade converter for Alteryx workflows to PySpark code   ║
║                                                                      ║
║  Features:                                                           ║
║  • Modular connector architecture                                    ║
║  • Comprehensive logging and analytics                              ║
║  • Real-time workflow visualization                                  ║
║  • Professional web interface                                        ║
║  • Error tracking and performance metrics                           ║
╚══════════════════════════════════════════════════════════════════════╝
"""
    print(banner)


def validate_configuration(logger):
    """Validate application configuration."""
    print("Validating configuration...")
    
    issues = Settings.validate_settings()
    
    if not issues:
        logger.app_logger.info("✓ Configuration validation passed")
        print("✓ All configuration checks passed")
    else:
        logger.app_logger.error("✗ Configuration validation failed")
        print("✗ Configuration issues found:")
        for issue in issues:
            print(f"  - {issue}")
        sys.exit(1)


def export_analytics_and_exit(logger):
    """Export analytics report and exit."""
    print("Exporting analytics report...")
    
    try:
        report_file = logger.export_analytics_report()
        print(f"✓ Analytics report exported to: {report_file}")
        logger.app_logger.info(f"Analytics report exported to: {report_file}")
    except Exception as e:
        print(f"✗ Failed to export analytics report: {e}")
        logger.app_logger.error(f"Failed to export analytics report: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()