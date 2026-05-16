#!/usr/bin/env python3
"""
Configuration Validator
========================

Universal configuration validation system that eliminates hardcoded values
and provides strict validation with detailed error reporting.
"""

import logging
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
from pathlib import Path

@dataclass
class ValidationError:
    """Configuration validation error"""
    field: str
    message: str
    value: Any = None

class ConfigValidationException(Exception):
    """Exception raised when configuration validation fails"""
    
    def __init__(self, errors: List[ValidationError]):
        self.errors = errors
        error_messages = [f"{err.field}: {err.message}" for err in errors]
        super().__init__(f"Configuration validation failed:\n" + "\n".join(error_messages))

class ConfigValidator:
    """
    Universal configuration validator
    """
    
    def __init__(self):
        self.logger = logging.getLogger('ConfigValidator')
        
    def validate_main_config(self, config: Dict[str, Any]) -> List[ValidationError]:
        """Validate main configuration"""
        errors = []
        
        # Database configuration
        if 'database' not in config:
            errors.append(ValidationError('database', 'Database configuration is required'))
        else:
            db_config = config['database']
            required_db_fields = ['host', 'port', 'name', 'user', 'password']
            
            for field in required_db_fields:
                if field not in db_config:
                    errors.append(ValidationError(f'database.{field}', f'Field {field} is required'))
                elif not db_config[field]:
                    errors.append(ValidationError(f'database.{field}', f'Field {field} cannot be empty'))
        
        # Validate logging configuration if present
        if 'logging' in config:
            log_config = config['logging']
            if 'level' in log_config:
                valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
                if log_config['level'] not in valid_levels:
                    errors.append(ValidationError('logging.level', f'Invalid log level. Must be one of: {valid_levels}'))
        
        return errors
        
    def validate_connections_config(self, config: Dict[str, Any]) -> List[ValidationError]:
        """Validate connections configuration"""
        errors = []
        
        if 'connections' not in config:
            errors.append(ValidationError('connections', 'Connections configuration is required'))
            return errors
            
        connections = config['connections']
        
        if not connections:
            errors.append(ValidationError('connections', 'At least one connection must be configured'))
            return errors
            
        for conn_name, conn_config in connections.items():
            if not isinstance(conn_config, dict):
                errors.append(ValidationError(f'connections.{conn_name}', 'Connection configuration must be an object'))
                continue
                
            # Required fields for each connection
            required_fields = ['exchange', 'symbol', 'timeframe', 'historical']
            historical_required_fields = ['start_date', 'batch_size', 'rate_limit_ms']
            for field in required_fields:
                if field not in conn_config:
                    errors.append(ValidationError(f'connections.{conn_name}.{field}', f'Field {field} is required'))
                elif not conn_config[field]:
                    errors.append(ValidationError(f'connections.{conn_name}.{field}', f'Field {field} cannot be empty'))
                elif field == 'historical' and not isinstance(conn_config[field], dict):
                    errors.append(ValidationError(f'connections.{conn_name}.{field}', f'Field {field} must be an object'))
                elif field == 'historical':
                    for hist_field in historical_required_fields:
                        if hist_field not in conn_config[field]:
                            errors.append(ValidationError(f'connections.{conn_name}.{field}.{hist_field}', f'Field {hist_field} is required'))
                        elif not conn_config[field][hist_field]:
                            errors.append(ValidationError(f'connections.{conn_name}.{field}.{hist_field}', f'Field {hist_field} cannot be empty'))

            # Validate exchange
            if 'exchange' in conn_config:
                # We should have exchange plugins to validate against
                pass  # For now, accept any exchange
                
            # Validate timeframe format
            if 'timeframe' in conn_config:
                timeframe = conn_config['timeframe']
                valid_timeframes = ['1m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
                if timeframe not in valid_timeframes:
                    errors.append(ValidationError(
                        f'connections.{conn_name}.timeframe', 
                        f'Invalid timeframe. Must be one of: {valid_timeframes}'
                    ))
        
        return errors
        
    def validate_indicators_config(self, config: Dict[str, Any], connections_config: Dict[str, Any]) -> List[ValidationError]:
        """Validate indicators configuration"""
        errors = []
        
        if 'indicators' not in config:
            errors.append(ValidationError('indicators', 'Indicators configuration is required'))
            return errors
            
        indicators = config['indicators']

        if not indicators:
            return errors  # empty is valid in on-demand mode
            
        # Get available connections
        available_connections = set(connections_config.get('connections', {}).keys())
        
        for indicator_name, indicator_config in indicators.items():
            if not isinstance(indicator_config, dict):
                errors.append(ValidationError(f'indicators.{indicator_name}', 'Indicator configuration must be an object'))
                continue
                
            # Required fields
            required_fields = ['connection', 'plugin']
            
            for field in required_fields:
                if field not in indicator_config:
                    errors.append(ValidationError(f'indicators.{indicator_name}.{field}', f'Field {field} is required'))
                elif not indicator_config[field]:
                    errors.append(ValidationError(f'indicators.{indicator_name}.{field}', f'Field {field} cannot be empty'))
            
            # Validate connection reference
            if 'connection' in indicator_config:
                connection = indicator_config['connection']
                if connection not in available_connections:
                    errors.append(ValidationError(
                        f'indicators.{indicator_name}.connection', 
                        f'Unknown connection "{connection}". Available connections: {list(available_connections)}'
                    ))
            
            # Validate parameters if present
            if 'parameters' in indicator_config:
                params = indicator_config['parameters']
                if not isinstance(params, dict):
                    errors.append(ValidationError(f'indicators.{indicator_name}.parameters', 'Parameters must be an object'))
        
        return errors
        
    def validate_strategies_config(self, config: Dict[str, Any], connections_config: Dict[str, Any], indicators_config: Dict[str, Any]) -> List[ValidationError]:
        """Validate strategies configuration"""
        errors = []
        
        if not config:
            return errors  # empty is valid in on-demand mode
            
        # Get available connections and indicators
        available_connections = set(connections_config.get('connections', {}).keys())
        available_indicators = set(indicators_config.get('indicators', {}).keys())
        
        if 'strategies' not in config:
            errors.append(ValidationError('strategies', 'Strategies configuration is required'))
            return errors
            
        strategies = config['strategies']

        for strategy_name, strategy_config in strategies.items():
            if not isinstance(strategy_config, dict):
                errors.append(ValidationError(f'strategies.{strategy_name}', 'Strategy configuration must be an object'))
                continue
                
            # Required fields
            required_fields = ['connection', 'plugin']
            
            for field in required_fields:
                if field not in strategy_config:
                    errors.append(ValidationError(f'strategies.{strategy_name}.{field}', f'Field {field} is required'))
                elif not strategy_config[field]:
                    errors.append(ValidationError(f'strategies.{strategy_name}.{field}', f'Field {field} cannot be empty'))
            
            # Validate connection reference
            if 'connection' in strategy_config:
                connection = strategy_config['connection']
                if connection not in available_connections:
                    errors.append(ValidationError(
                        f'strategies.{strategy_name}.connection', 
                        f'Unknown connection "{connection}". Available connections: {list(available_connections)}'
                    ))
            
            # Validate parameters if present
            if 'parameters' in strategy_config:
                params = strategy_config['parameters']
                if not isinstance(params, dict):
                    errors.append(ValidationError(f'strategies.{strategy_name}.parameters', 'Parameters must be an object'))
                else:
                    # Validate that all required parameters are present (no defaults allowed)
                    param_errors = self._validate_strategy_parameters(strategy_name, strategy_config['plugin'], params)
                    errors.extend(param_errors)
            
            # Validate required indicators if specified
            if 'required_indicators' in strategy_config:
                required_indicators = strategy_config['required_indicators']
                if not isinstance(required_indicators, list):
                    errors.append(ValidationError(f'strategies.{strategy_name}.required_indicators', 'Required indicators must be a list'))
                else:
                    for indicator in required_indicators:
                        if indicator not in available_indicators:
                            errors.append(ValidationError(
                                f'strategies.{strategy_name}.required_indicators', 
                                f'Unknown indicator "{indicator}". Available indicators: {list(available_indicators)}'
                            ))
        
        return errors
        
    def _validate_strategy_parameters(self, strategy_name: str, plugin: str, parameters: Dict[str, Any]) -> List[ValidationError]:
        """Validate strategy-specific parameters"""
        errors = []
        
        # Define required parameters for each strategy plugin
        strategy_requirements = {
            'ema_deviation_strategy': {
                'required_params': ['ema_indicator', 'min_dev_pct', 'max_dev_pct'],
                'param_types': {
                    'ema_indicator': str,
                    'min_dev_pct': (int, float),
                    'max_dev_pct': (int, float),
                },
                'param_ranges': {
                    'min_dev_pct': (0, 10),
                    'max_dev_pct': (0, 10),
                }
            },
            'rsi_multi_timeframe_strategy': {
                'required_params': [
                    'rsi_4h_upper', 'rsi_4h_lower',
                    'rsi_1d_upper', 'rsi_1d_lower', 
                    'rsi_1w_upper', 'rsi_1w_lower',
                    'extreme_sum_upper', 'extreme_sum_lower',
                    'cooldown_hours'
                ],
                'param_types': {
                    'rsi_4h_upper': (int, float), 'rsi_4h_lower': (int, float),
                    'rsi_1d_upper': (int, float), 'rsi_1d_lower': (int, float),
                    'rsi_1w_upper': (int, float), 'rsi_1w_lower': (int, float),
                    'extreme_sum_upper': (int, float), 'extreme_sum_lower': (int, float),
                    'cooldown_hours': (int, float)
                },
                'param_ranges': {
                    'rsi_4h_upper': (50, 100), 'rsi_4h_lower': (0, 50),
                    'rsi_1d_upper': (50, 100), 'rsi_1d_lower': (0, 50),
                    'rsi_1w_upper': (50, 100), 'rsi_1w_lower': (0, 50),
                    'extreme_sum_upper': (100, 300), 'extreme_sum_lower': (0, 100),
                    'cooldown_hours': (0, 168)  # Max 1 week
                }
            }
        }
        
        if plugin not in strategy_requirements:
            errors.append(ValidationError(f'strategies.{strategy_name}.plugin', f'Unknown strategy plugin: {plugin}'))
            return errors
            
        requirements = strategy_requirements[plugin]
        
        # Check required parameters
        for param in requirements['required_params']:
            if param not in parameters:
                errors.append(ValidationError(
                    f'strategies.{strategy_name}.parameters.{param}', 
                    f'Required parameter {param} is missing'
                ))
            else:
                value = parameters[param]
                
                # Check type
                expected_type = requirements['param_types'].get(param)
                if expected_type and not isinstance(value, expected_type):
                    errors.append(ValidationError(
                        f'strategies.{strategy_name}.parameters.{param}', 
                        f'Parameter {param} must be of type {expected_type.__name__ if not isinstance(expected_type, tuple) else "/".join(t.__name__ for t in expected_type)}'
                    ))
                
                # Check range
                if param in requirements['param_ranges']:
                    min_val, max_val = requirements['param_ranges'][param]
                    if not (min_val <= value <= max_val):
                        errors.append(ValidationError(
                            f'strategies.{strategy_name}.parameters.{param}', 
                            f'Parameter {param} must be between {min_val} and {max_val}'
                        ))
        
        return errors
        
    def validate_all_configs(self, configs: Dict[str, Dict[str, Any]]) -> List[ValidationError]:
        """Validate all configuration files together"""
        all_errors = []
        
        # Validate main config
        if 'main' in configs:
            all_errors.extend(self.validate_main_config(configs['main']))
        else:
            all_errors.append(ValidationError('main', 'Main configuration is required'))
            
        # Validate connections config
        if 'connections' in configs:
            all_errors.extend(self.validate_connections_config(configs['connections']))
        else:
            all_errors.append(ValidationError('connections', 'Connections configuration is required'))
            
        # Validate indicators config (requires connections)
        if 'indicators' in configs and 'connections' in configs:
            all_errors.extend(self.validate_indicators_config(configs['indicators'], configs['connections']))
        elif 'indicators' in configs:
            all_errors.append(ValidationError('indicators', 'Cannot validate indicators without connections configuration'))
            
        # Validate strategies config (requires connections and indicators)
        if 'strategies' in configs and 'connections' in configs and 'indicators' in configs:
            all_errors.extend(self.validate_strategies_config(configs['strategies'], configs['connections'], configs['indicators']))
        elif 'strategies' in configs:
            all_errors.append(ValidationError('strategies', 'Cannot validate strategies without connections and indicators configuration'))
            
        return all_errors
        
    def validate_and_raise(self, configs: Dict[str, Dict[str, Any]]):
        """Validate configurations and raise exception if errors found"""
        errors = self.validate_all_configs(configs)
        
        if errors:
            # Log all errors
            self.logger.error("Configuration validation failed:")
            for error in errors:
                self.logger.error(f" {error.field}: {error.message}")
                
            raise ConfigValidationException(errors)
            
        self.logger.info("Configuration validation passed")
        
    def get_connection_mapping(self, connections_config: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        """Get mapping from connection names to exchange/symbol/timeframe"""
        mapping = {}
        
        for conn_name, conn_config in connections_config.get('connections', {}).items():
            if conn_config.get('enabled', True):
                mapping[conn_name] = {
                    'exchange': conn_config['exchange'],
                    'symbol': conn_config['symbol'], 
                    'timeframe': conn_config['timeframe']
                }
                
        return mapping