class TradingBotException(Exception):
    """Base exception for trading bot"""
    pass

class ConfigurationError(TradingBotException):
    """Configuration validation error"""
    pass

class DatabaseError(TradingBotException):
    """Database operation error"""
    pass

class ExchangeError(TradingBotException):
    """Exchange API error"""
    pass

class RateLimitError(ExchangeError):
    """Rate limit exceeded error"""
    pass

class ValidationError(TradingBotException):
    """Data validation error"""
    pass

class GapRecoveryError(TradingBotException):
    """Gap recovery operation error"""
    pass

class IndicatorError(TradingBotException):
    """Indicator calculation error"""
    pass