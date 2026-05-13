#!/usr/bin/env python3
"""
Aggregation Configuration Manager
================================

Manages aggregation rules and source timeframe mappings.
"""

import yaml
import logging
from typing import Dict, Optional, Set
from pathlib import Path


class AggregationConfig:
    """Configuration manager for aggregation rules and source timeframe mappings"""
    
    def __init__(self, config_path: str = "config/aggregation.yaml"):
        self.config_path = Path(config_path)
        self.logger = logging.getLogger(__name__)
        self._source_mapping: Dict[str, str] = {}
        self._settings: Dict = {}
        self._load_config()
    
    def _load_config(self) -> None:
        """Load aggregation configuration from YAML file"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Aggregation config not found: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        if not config or 'aggregation' not in config:
            raise ValueError("Invalid aggregation config: missing 'aggregation' section")
        
        agg = config['aggregation']

        if 'source_mapping' in agg:
            # Legacy format: explicit per-symbol keys
            self._source_mapping = agg['source_mapping']
        elif 'timeframe_rules' in agg:
            # New format: expand rules × enabled connections from connections.yaml
            self._source_mapping = self._expand_timeframe_rules(agg['timeframe_rules'])
        else:
            raise ValueError("Invalid aggregation config: need 'source_mapping' or 'timeframe_rules'")

        if not self._source_mapping:
            raise ValueError("Invalid aggregation config: empty source_mapping after expansion")

        # Load settings (top-level 'settings' key or nested inside 'aggregation')
        self._settings = config.get('settings', agg.get('settings', {}))
        
        self.logger.info(f"Loaded aggregation config with {len(self._source_mapping)} mappings")
    
    def _expand_timeframe_rules(self, rules: list) -> Dict[str, str]:
        """Expand timeframe_rules × enabled connections into source_mapping."""
        connections_path = self.config_path.parent / 'connections.yaml'
        if not connections_path.exists():
            raise FileNotFoundError(f"connections.yaml not found at {connections_path}")

        with open(connections_path, 'r', encoding='utf-8') as f:
            connections_cfg = yaml.safe_load(f)

        mapping: Dict[str, str] = {}
        for conn in connections_cfg.get('connections', {}).values():
            if not conn.get('enabled'):
                continue
            exchange = conn['exchange']
            symbol = conn['symbol']
            for rule in rules:
                source = rule['source']
                for target in rule.get('targets', []):
                    mapping[f"{exchange}:{symbol}:{target}"] = source

        return mapping

    def get_source_timeframe(self, exchange: str, symbol: str, target_timeframe: str) -> Optional[str]:
        """
        Get source timeframe for given target from configuration
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol 
            target_timeframe: Target timeframe
            
        Returns:
            Source timeframe string or None if not configured
        """
        key = f"{exchange}:{symbol}:{target_timeframe}"
        return self._source_mapping.get(key)
    
    def get_all_mappings(self) -> Dict[str, str]:
        """Get all source timeframe mappings"""
        return self._source_mapping.copy()
    
    def get_supported_targets(self, exchange: str, symbol: str) -> Set[str]:
        """Get all supported target timeframes for exchange/symbol"""
        prefix = f"{exchange}:{symbol}:"
        targets = {
            key.split(':')[2] for key in self._source_mapping.keys() 
            if key.startswith(prefix)
        }
        return targets
    
    def get_setting(self, key: str):
        """Get configuration setting"""
        return self._settings.get(key)
    
    def is_aggregation_supported(self, exchange: str, symbol: str, target_timeframe: str) -> bool:
        """Check if aggregation is supported for given parameters"""
        key = f"{exchange}:{symbol}:{target_timeframe}"
        return key in self._source_mapping
    
    def reload_config(self) -> None:
        """Reload configuration from file"""
        self._load_config()
        self.logger.info("Aggregation configuration reloaded")
    
    def __repr__(self) -> str:
        return f"AggregationConfig(mappings={len(self._source_mapping)}, config_path='{self.config_path}')"