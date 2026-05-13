#!/usr/bin/env python3
"""
Universal Configuration Manager
===============================

Universal configuration manager that eliminates hardcoded values
and provides strict validation without defaults.
"""

import json
import os
import logging
import argparse
import sys
from typing import Dict, Any, Optional, List
from pathlib import Path
from .config_validator import ConfigValidator, ConfigValidationException

class UniversalConfigManager:
    """
    Universal configuration manager with strict validation
    """
    
    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize config manager
        
        Args:
            config_dir: Configuration directory path. If None, will be determined from CLI args
        """
        if config_dir is None:
            config_dir = self._get_config_dir_from_args()
            
        if not os.path.exists(config_dir):
            raise FileNotFoundError(f"Configuration directory not found: {config_dir}")
            
        self.config_dir = Path(config_dir)
        self.logger = logging.getLogger('UniversalConfigManager')
        self.validator = ConfigValidator()
        self._configs: Dict[str, Dict[str, Any]] = {}
        self._connection_mapping: Optional[Dict[str, Dict[str, str]]] = None
        
    def _get_config_dir_from_args(self) -> str:
        """Get config directory from env var or command line arguments."""
        # Env var takes priority (set by launcher.py when spawning workers)
        env_override = os.environ.get('CONFIG_DIR')
        if env_override:
            return env_override

        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument(
            '--config-dir', '--config_dir',
            default=os.path.join(os.getcwd(), 'config'),
            help='Configuration directory path',
        )
        known_args, _ = parser.parse_known_args()
        return known_args.config_dir
        
    def load_config(self, config_name: str) -> Dict[str, Any]:
        """Load configuration file"""
        config_path = self.config_dir / f"{config_name}.json"
        
        if not config_path.exists():
            # Try .yaml extension as fallback
            config_path = self.config_dir / f"{config_name}.yaml"
            if not config_path.exists():
                raise FileNotFoundError(f"Configuration file not found: {config_name}.json or {config_name}.yaml in {self.config_dir}")
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                if config_path.suffix == '.json':
                    config = json.load(f)
                else:
                    import yaml
                    config = yaml.safe_load(f)
                    
        except (json.JSONDecodeError, yaml.YAMLError) as e:
            raise ValueError(f"Invalid configuration file {config_path}: {e}")
        
        if config is None:
            raise ValueError(f"Empty configuration file: {config_path}")
        
        self._configs[config_name] = config
        self.logger.info(f"Loaded configuration: {config_name}")
        
        return config
        
    def load_all_configs(self, symbol_filter: str = None) -> Dict[str, Dict[str, Any]]:
        """Load all required configuration files and expand templates."""
        required_configs = ['main', 'connections', 'indicators', 'strategies', 'aggregation']

        self.logger.info("Loading all configurations...")

        for config_name in required_configs:
            try:
                self.load_config(config_name)
            except FileNotFoundError as e:
                self.logger.error(f"Missing required configuration: {config_name}")
                raise e

        # Expand templates into full per-symbol configs before validation
        self.expand_from_templates(symbol_filter=symbol_filter)

        # Validate all configurations together (on expanded format)
        self.validator.validate_and_raise(self._configs)

        # Build connection mapping
        self._build_connection_mapping()

        self.logger.info("All configurations loaded and validated")
        return self._configs

    def expand_from_templates(self, symbol_filter: str = None) -> None:
        """Expand indicator/strategy templates into per-symbol configs.

        Reads indicator_templates and strategy_templates from loaded configs,
        iterates enabled connections, and generates the expanded format that
        all services already understand. Optionally filters to a single symbol.
        """
        connections_cfg = self._configs.get('connections', {}).get('connections', {})
        indicator_templates = self._configs.get('indicators', {}).get('indicator_templates', {})
        strategy_templates = self._configs.get('strategies', {}).get('strategy_templates', {})
        agg_rules = (
            self._configs.get('aggregation', {})
            .get('aggregation', {})
            .get('timeframe_rules', [])
        )

        expanded_indicators: Dict[str, Any] = {}
        expanded_strategies: Dict[str, Any] = {}
        expanded_agg_mapping: Dict[str, str] = {}

        for conn_name, conn in connections_cfg.items():
            if not conn.get('enabled'):
                continue

            symbol = conn['symbol']
            if symbol_filter and symbol != symbol_filter:
                continue

            exchange = conn['exchange']
            prefix = symbol.lower().split('_')[0]  # SOL_USDT → sol

            # Expand indicator templates
            for tmpl_id in conn.get('indicators', []):
                if tmpl_id not in indicator_templates:
                    raise ValueError(
                        f"Unknown indicator template '{tmpl_id}' in connection '{conn_name}'. "
                        f"Available templates: {list(indicator_templates.keys())}"
                    )
                indicator_id = f"{prefix}_{tmpl_id}"
                expanded_indicators[indicator_id] = {
                    **indicator_templates[tmpl_id],
                    'connection': conn_name,
                    'enabled': True,
                }

            # Expand strategy templates
            for strat_name, overrides in (conn.get('strategies') or {}).items():
                if strat_name not in strategy_templates:
                    raise ValueError(
                        f"Unknown strategy template '{strat_name}' in connection '{conn_name}'. "
                        f"Available templates: {list(strategy_templates.keys())}"
                    )
                tmpl = strategy_templates[strat_name]
                strategy_id = f"{prefix}_{strat_name}"
                requires = tmpl.get('requires', [])
                required_indicators = [f"{prefix}_{ind}" for ind in requires]
                params = {**tmpl.get('parameters', {}), **(overrides or {})}

                # Auto-generate named indicator refs for ema_deviation
                if strat_name == 'ema_deviation' and len(requires) >= 2:
                    params['ema_indicator'] = f"{prefix}_{requires[0]}"
                    params['trend_indicator'] = f"{prefix}_{requires[1]}"

                expanded_strategies[strategy_id] = {
                    'enabled': True,
                    'connection': conn_name,
                    'plugin': tmpl['plugin'],
                    'required_indicators': required_indicators,
                    'parameters': params,
                }

            # Expand aggregation source_mapping from timeframe rules
            for rule in agg_rules:
                source = rule['source']
                for target in rule.get('targets', []):
                    expanded_agg_mapping[f"{exchange}:{symbol}:{target}"] = source

        # Replace raw template configs with the expanded format services already expect
        self._configs['indicators'] = {'indicators': expanded_indicators}
        self._configs['strategies'] = {'strategies': expanded_strategies}
        if 'aggregation' not in self._configs:
            self._configs['aggregation'] = {'aggregation': {}}
        self._configs['aggregation'].setdefault('aggregation', {})['source_mapping'] = expanded_agg_mapping

        self.logger.info(
            f"Templates expanded: {len(expanded_indicators)} indicators, "
            f"{len(expanded_strategies)} strategies, "
            f"{len(expanded_agg_mapping)} aggregation mappings"
            + (f" (symbol={symbol_filter})" if symbol_filter else "")
        )
        
    def get_config(self, config_name: str) -> Dict[str, Any]:
        """Get loaded configuration"""
        if config_name not in self._configs:
            raise ValueError(f"Configuration '{config_name}' not loaded. Call load_config() or load_all_configs() first.")
        return self._configs[config_name]
        
    def _build_connection_mapping(self):
        """Build connection name to exchange/symbol mapping"""
        if 'connections' not in self._configs:
            return
            
        self._connection_mapping = self.validator.get_connection_mapping(self._configs['connections'])
        self.logger.info(f"Built connection mapping for {len(self._connection_mapping)} connections")
        
    def get_connection_mapping(self) -> Dict[str, Dict[str, str]]:
        """Get connection name to exchange/symbol/timeframe mapping"""
        if self._connection_mapping is None:
            raise ValueError("Connection mapping not built. Call load_all_configs() first.")
        return self._connection_mapping
        
    def resolve_connection(self, connection_name: str) -> Dict[str, str]:
        """Resolve connection name to exchange/symbol/timeframe"""
        mapping = self.get_connection_mapping()
        
        if connection_name not in mapping:
            raise ValueError(f"Unknown connection: {connection_name}. Available connections: {list(mapping.keys())}")
            
        return mapping[connection_name]
        
    def get_strategy_config(self, strategy_name: str) -> Dict[str, Any]:
        """Get strategy configuration with validation"""
        strategies_config = self.get_config('strategies')
        
        if strategy_name not in strategies_config:
            available_strategies = list(strategies_config.keys())
            raise ValueError(f"Unknown strategy: {strategy_name}. Available strategies: {available_strategies}")
            
        strategy_config = strategies_config[strategy_name]
        
        if not strategy_config.get('enabled', True):
            raise ValueError(f"Strategy {strategy_name} is disabled")
            
        return strategy_config
        
    def get_strategy_parameters(self, strategy_name: str) -> Dict[str, Any]:
        """Get strategy parameters with validation"""
        strategy_config = self.get_strategy_config(strategy_name)
        
        if 'parameters' not in strategy_config:
            raise ValueError(f"Strategy {strategy_name} has no parameters defined")
            
        return strategy_config['parameters']
        
    def get_required_parameter(self, strategy_name: str, param_name: str) -> Any:
        """Get required parameter value (no defaults)"""
        parameters = self.get_strategy_parameters(strategy_name)
        
        if param_name not in parameters:
            raise ValueError(f"Required parameter '{param_name}' not found in strategy '{strategy_name}' configuration")
            
        value = parameters[param_name]
        
        if value is None:
            raise ValueError(f"Parameter '{param_name}' in strategy '{strategy_name}' cannot be null")
            
        return value
        
    def get_indicator_config(self, indicator_name: str) -> Dict[str, Any]:
        """Get indicator configuration with validation"""
        indicators_config = self.get_config('indicators')
        
        if 'indicators' not in indicators_config:
            raise ValueError("No indicators section found in indicators configuration")
            
        indicators = indicators_config['indicators']
        
        if indicator_name not in indicators:
            available_indicators = list(indicators.keys())
            raise ValueError(f"Unknown indicator: {indicator_name}. Available indicators: {available_indicators}")
            
        indicator_config = indicators[indicator_name]
        
        if not indicator_config.get('enabled', True):
            raise ValueError(f"Indicator {indicator_name} is disabled")
            
        return indicator_config
        
    def get_connection_config(self, connection_name: str) -> Dict[str, Any]:
        """Get connection configuration with validation"""
        connections_config = self.get_config('connections')
        
        if 'connections' not in connections_config:
            raise ValueError("No connections section found in connections configuration")
            
        connections = connections_config['connections']
        
        if connection_name not in connections:
            available_connections = list(connections.keys())
            raise ValueError(f"Unknown connection: {connection_name}. Available connections: {available_connections}")
            
        connection_config = connections[connection_name]
        
        if not connection_config.get('enabled', True):
            raise ValueError(f"Connection {connection_name} is disabled")
            
        return connection_config
        
    def get_strategy_indicators(self, strategy_name: str) -> List[str]:
        """Get list of indicators required by strategy"""
        strategy_config = self.get_strategy_config(strategy_name)
        
        if 'required_indicators' in strategy_config:
            return strategy_config['required_indicators']
            
        plugin = strategy_config.get('plugin', '')
        raise ValueError(
            f"Cannot determine required indicators for strategy '{strategy_name}' (plugin: {plugin}). "
            f"Add 'required_indicators' list to strategies.yaml."
        )
            
    def list_enabled_strategies(self) -> List[str]:
        """Get list of enabled strategy names"""
        strategies_config = self.get_config('strategies')
        
        enabled = []
        for strategy_name, config in strategies_config.items():
            if config.get('enabled', True):
                enabled.append(strategy_name)
                
        return enabled
        
    def list_enabled_indicators(self) -> List[str]:
        """Get list of enabled indicator names"""
        indicators_config = self.get_config('indicators')
        
        enabled = []
        if 'indicators' in indicators_config:
            for indicator_name, config in indicators_config['indicators'].items():
                if config.get('enabled', True):
                    enabled.append(indicator_name)
                    
        return enabled
        
    def list_enabled_connections(self) -> List[str]:
        """Get list of enabled connection names"""
        connections_config = self.get_config('connections')
        
        enabled = []
        if 'connections' in connections_config:
            for connection_name, config in connections_config['connections'].items():
                if config.get('enabled', True):
                    enabled.append(connection_name)
                    
        return enabled

def get_global_config_manager() -> UniversalConfigManager:
    """
    Get global config manager instance
    """
    if not hasattr(get_global_config_manager, '_instance'):
        get_global_config_manager._instance = UniversalConfigManager()
        
    return get_global_config_manager._instance