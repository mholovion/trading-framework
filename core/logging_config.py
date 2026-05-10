#!/usr/bin/env python3
"""
Logging Configuration for Microservices
=======================================

Centralized logging configuration for all microservices with separate log files.
"""

import logging
import logging.handlers
import os
from pathlib import Path
from typing import Optional


class ServiceLogger:
    """
    Centralized logger configuration for microservices
    """
    
    @staticmethod
    def setup_logger(service_name: str, 
                    log_level: str = "INFO",
                    log_dir: Optional[str] = None) -> logging.Logger:
        """
        Setup logger for a specific service
        
        Args:
            service_name: Name of the service (e.g., 'orchestrator', 'database')
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
            log_dir: Directory for log files (optional)
        
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(service_name)
        
        # Clear any existing handlers
        logger.handlers.clear()
        logger.propagate = False
        
        # Set log level
        level = getattr(logging, log_level.upper(), logging.INFO)
        logger.setLevel(level)
        
        # Create formatter
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler (for Docker logs)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler (if log directory specified)
        if log_dir:
            log_path = Path(log_dir) / f"{service_name}.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Rotating file handler (10MB, keep 5 files)
            file_handler = logging.handlers.RotatingFileHandler(
                filename=log_path,
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5,
                encoding='utf-8'
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger
    
    @staticmethod
    def get_log_config() -> dict:
        """
        Get logging configuration from environment variables
        
        Returns:
            Dictionary with log configuration
        """
        return {
            'level': os.getenv('LOG_LEVEL', 'INFO'),
            'base_dir': os.getenv('LOG_DIR', '/app/logs'),
            'console_only': os.getenv('LOG_CONSOLE_ONLY', 'false').lower() == 'true'
        }


def setup_service_logging(service_name: str) -> logging.Logger:
    """
    Convenience function to setup logging for a service
    
    Args:
        service_name: Name of the service
        
    Returns:
        Configured logger
    """
    config = ServiceLogger.get_log_config()
    
    log_dir = None if config['console_only'] else config['base_dir']
    
    return ServiceLogger.setup_logger(
        service_name=service_name,
        log_level=config['level'],
        log_dir=log_dir
    )


# Predefined loggers for each service
def get_orchestrator_logger() -> logging.Logger:
    """Get logger for orchestrator service"""
    return setup_service_logging('orchestrator')


def get_database_logger() -> logging.Logger:
    """Get logger for database service"""
    return setup_service_logging('database')


def get_historical_logger() -> logging.Logger:
    """Get logger for historical data service"""
    return setup_service_logging('historical')


def get_realtime_logger() -> logging.Logger:
    """Get logger for realtime data service"""
    return setup_service_logging('realtime')


def get_indicators_logger() -> logging.Logger:
    """Get logger for indicators service"""
    return setup_service_logging('indicators')


def get_strategies_logger() -> logging.Logger:
    """Get logger for strategies service"""
    return setup_service_logging('strategies')

def get_strategies_gap_logger() -> logging.Logger:
    """Get logger for strategies gap service"""
    return setup_service_logging('strategies_gap')

def get_api_logger() -> logging.Logger:
    """Get logger for API service"""
    return setup_service_logging('api')