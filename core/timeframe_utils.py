#!/usr/bin/env python3
"""
Timeframe Utilities
==================

Universal utilities for working with timeframes in the trading bot.
This module provides consistent timeframe conversion and period calculation
across all services.
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional
from core.exceptions import ConfigurationError


class TimeframeUtils:
    """Universal timeframe utilities for the trading bot"""
    
    # Standard timeframe mapping to seconds
    TIMEFRAME_SECONDS: Dict[str, int] = {
        '1m': 60,
        '3m': 180, 
        '5m': 300,
        '15m': 900,
        '30m': 1800,
        '1h': 3600,
        '2h': 7200,
        '4h': 14400,
        '6h': 21600,
        '8h': 28800,
        '12h': 43200,
        '1d': 86400,
        '3d': 259200,
        '1w': 604800,
        '1M': 2592000  # Approximate month
    }
    
    @classmethod
    def get_timeframe_seconds(cls, timeframe: str) -> int:
        """
        Convert timeframe string to seconds
        
        Args:
            timeframe: Timeframe string (e.g., '1m', '1h', '1d')
            
        Returns:
            Number of seconds in the timeframe
            
        Raises:
            ConfigurationError: If timeframe is not supported
        """
        seconds = cls.TIMEFRAME_SECONDS.get(timeframe)
        if seconds is None:
            raise ConfigurationError(f"Unsupported timeframe: {timeframe}")
        return seconds
    
    @classmethod
    def get_period_start(cls, timestamp: int, timeframe: str) -> int:
        """
        Get period start timestamp for given timeframe
        
        Args:
            timestamp: Reference timestamp
            timeframe: Target timeframe
            
        Returns:
            Period start timestamp aligned to timeframe boundaries
        """
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        
        if timeframe in ['1m', '3m', '5m', '15m', '30m']:
            # Minute-based timeframes
            minutes = cls.get_timeframe_seconds(timeframe) // 60
            aligned_minute = (dt.minute // minutes) * minutes
            period_start = dt.replace(minute=aligned_minute, second=0, microsecond=0)
            
        elif timeframe in ['1h', '2h', '6h', '8h', '12h']:
            # Hour-based timeframes
            hours = cls.get_timeframe_seconds(timeframe) // 3600
            aligned_hour = (dt.hour // hours) * hours
            period_start = dt.replace(hour=aligned_hour, minute=0, second=0, microsecond=0)
            
        elif timeframe == '4h':
            # Special case for 4h - align to 0, 4, 8, 12, 16, 20 hours
            aligned_hour = (dt.hour // 4) * 4
            period_start = dt.replace(hour=aligned_hour, minute=0, second=0, microsecond=0)
            
        elif timeframe == '1d':
            # Daily - align to midnight UTC
            period_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            
        elif timeframe == '3d':
            # 3-day periods - align to start of epoch modulo 3 days
            days_since_epoch = dt.toordinal() - datetime(1970, 1, 1).toordinal()
            aligned_days = (days_since_epoch // 3) * 3
            epoch_start = datetime(1970, 1, 1, tzinfo=timezone.utc)
            period_start = epoch_start + timedelta(days=aligned_days)
            
        elif timeframe == '1w':
            # Weekly - align to Monday 00:00 UTC
            days_since_monday = dt.weekday()
            period_start = (dt - timedelta(days=days_since_monday)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            
        elif timeframe == '1M':
            # Monthly - align to first day of month
            period_start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
        else:
            raise ConfigurationError(f"Unsupported timeframe for period calculation: {timeframe}")
        
        return int(period_start.timestamp())
    
    @classmethod
    def get_period_end(cls, period_start: int, timeframe: str) -> int:
        """
        Get period end timestamp for given timeframe and period start
        
        Args:
            period_start: Period start timestamp
            timeframe: Target timeframe
            
        Returns:
            Period end timestamp
        """
        dt = datetime.fromtimestamp(period_start, tz=timezone.utc)
        
        if timeframe in ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '6h', '8h', '12h']:
            # Simple addition for standard timeframes
            seconds = cls.get_timeframe_seconds(timeframe)
            period_end = dt + timedelta(seconds=seconds)
            
        elif timeframe == '4h':
            period_end = dt + timedelta(hours=4)
            
        elif timeframe == '1d':
            period_end = dt + timedelta(days=1)
            
        elif timeframe == '3d':
            period_end = dt + timedelta(days=3)
            
        elif timeframe == '1w':
            period_end = dt + timedelta(weeks=1)
            
        elif timeframe == '1M':
            # Handle month boundaries properly
            if dt.month == 12:
                period_end = dt.replace(year=dt.year + 1, month=1)
            else:
                period_end = dt.replace(month=dt.month + 1)
                
        else:
            raise ConfigurationError(f"Unsupported timeframe for period calculation: {timeframe}")
        
        return int(period_end.timestamp())
    
    @classmethod
    def get_period_info(cls, timestamp: int, timeframe: str) -> Tuple[int, int]:
        """
        Get both period start and end for a timestamp and timeframe
        
        Args:
            timestamp: Reference timestamp
            timeframe: Target timeframe
            
        Returns:
            Tuple of (period_start, period_end)
        """
        period_start = cls.get_period_start(timestamp, timeframe)
        period_end = cls.get_period_end(period_start, timeframe)
        return period_start, period_end
    
    @classmethod
    def should_aggregate(cls, source_timeframe: str, target_timeframe: str) -> bool:
        """
        Determine if aggregation should be performed from source to target timeframe
        
        Args:
            source_timeframe: Source timeframe
            target_timeframe: Target timeframe
            
        Returns:
            True if target timeframe is higher than source timeframe
        """
        source_seconds = cls.get_timeframe_seconds(source_timeframe)
        target_seconds = cls.get_timeframe_seconds(target_timeframe)
        return target_seconds > source_seconds
    
    @classmethod
    def get_candles_per_period(cls, source_timeframe: str, target_timeframe: str) -> int:
        """
        Calculate how many source candles fit in one target period
        
        Args:
            source_timeframe: Source timeframe
            target_timeframe: Target timeframe
            
        Returns:
            Number of source candles per target period
        """
        source_seconds = cls.get_timeframe_seconds(source_timeframe)
        target_seconds = cls.get_timeframe_seconds(target_timeframe)
        
        if target_seconds % source_seconds != 0:
            raise ConfigurationError(
                f"Target timeframe {target_timeframe} is not evenly divisible by "
                f"source timeframe {source_timeframe}"
            )
        
        return target_seconds // source_seconds
    
    @classmethod
    def is_period_complete(cls, current_timestamp: int, period_start: int, timeframe: str) -> bool:
        """
        Check if a period is complete based on current time
        
        Args:
            current_timestamp: Current timestamp
            period_start: Period start timestamp
            timeframe: Target timeframe
            
        Returns:
            True if the period is complete
        """
        period_end = cls.get_period_end(period_start, timeframe)
        return current_timestamp >= period_end
    
    @classmethod
    def get_supported_timeframes(cls) -> list:
        """
        Get list of all supported timeframes
        
        Returns:
            List of supported timeframe strings
        """
        return list(cls.TIMEFRAME_SECONDS.keys())
    
    @classmethod
    def validate_timeframe(cls, timeframe: str) -> bool:
        """
        Validate if timeframe is supported
        
        Args:
            timeframe: Timeframe string to validate
            
        Returns:
            True if timeframe is supported
        """
        return timeframe in cls.TIMEFRAME_SECONDS