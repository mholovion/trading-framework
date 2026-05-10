# ===== core/logging.py - Component-based logging version =====
import logging
import logging.handlers
import os
from typing import Dict, Any
from core.exceptions import ConfigurationError

class LogManager:
    """Centralized logging management with component-based separation"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config['logging']
        self.component_loggers = {}
        self._setup_base_logging()
    
    def _setup_base_logging(self):
        """Setup base logging configuration"""
        level_map = {
            'ERROR': logging.ERROR,
            'WARN': logging.WARNING,
            'INFO': logging.INFO,
            'DEBUG': logging.DEBUG
        }
        
        self.log_level = level_map.get(self.config['level'])
        if self.log_level is None:
            raise ConfigurationError(f"Invalid log level: {self.config['level']}")
        
        # Create logs directory
        log_dir = self.config.get('component_logs_dir', 'logs')
        if not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir, exist_ok=True)
                print(f"Created logs directory: {log_dir}")
            except OSError as e:
                print(f"Warning: Could not create logs directory: {e}")
        
        self.log_dir = log_dir
        
        # Create main formatter
        self.formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Setup console handler for all components
        self.console_handler = logging.StreamHandler()
        self.console_handler.setFormatter(self.formatter)
        
    def _setup_component_logger(self, component_name: str) -> logging.Logger:
        """Setup logger for specific component with its own log file"""
        logger = logging.getLogger(component_name)
        logger.setLevel(self.log_level)
        
        # Clear existing handlers to avoid duplicates
        logger.handlers.clear()
        
        # Add console handler
        logger.addHandler(self.console_handler)
        
        # Setup component-specific file handler
        component_log_file = os.path.join(self.log_dir, f"{component_name}.log")
        
        try:
            file_handler = logging.handlers.RotatingFileHandler(
                component_log_file,
                maxBytes=self.config['max_file_size_mb'] * 1024 * 1024,
                backupCount=self.config['backup_count']
            )
            file_handler.setFormatter(self.formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            print(f"Warning: Could not setup file logging for {component_name}: {e}")
        
        # Prevent propagation to root logger to avoid duplicate messages
        logger.propagate = False
        
        return logger
    
    def get_logger(self, name: str) -> logging.Logger:
        """Get component-specific logger instance"""
        # Map specific logger names to component categories
        component_mapping = {
            'modules.aggregation.manager': 'aggregation',
            'modules.indicators.manager': 'indicators', 
            'core.smart_gap_manager': 'gap_manager',
            'modules.connections.manager': 'connections',
            'core.database': 'database',
            'modules.strategies.manager': 'strategies',
            'core.events': 'events',
            'modules.api.server': 'api',
            'main': 'main'
        }
        
        # Extract component name
        component_name = component_mapping.get(name, name.split('.')[-1] if '.' in name else name)
        
        # Create component logger if not exists
        if component_name not in self.component_loggers:
            self.component_loggers[component_name] = self._setup_component_logger(component_name)
        
        return self.component_loggers[component_name]