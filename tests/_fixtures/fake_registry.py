"""Stand-in for a host app's builtin-registry module, used by test_plugin_registry.py."""
FAKE_BUILTINS = {"example": "code here"}

# Used by test_aggregation.py to exercise load_aggregation_plugin()'s builtin-registry branch.
BUILTIN_AGGREGATIONS = {
    "my_agg": '''
OUTPUT_TABLE = "spread_btc_eth"
SOURCES = {
    "btc": SourceRef("candles_btc_usdt", field="close"),
    "eth": SourceRef("candles_eth_usdt", field="close"),
}
COMBINE_SQL = "btc - eth"
''',
}
