"""
tradingkit.executor._worker — subprocess entry point.

Reads a pickle payload from stdin, executes the task, writes pickle result to stdout.
Memory limit applied via resource module (Unix only).
"""
from __future__ import annotations

import argparse
import asyncio
import pickle
import struct
import sys


def _apply_memory_limit(mb: int) -> None:
    try:
        import resource
        limit = mb * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
    except (ImportError, ValueError):
        pass  # Windows or unprivileged environment


async def _run_indicator(payload: dict) -> dict:
    import numpy as np
    import polars as pl
    import pyarrow as pa

    indicator = pickle.loads(payload["indicator"])

    buf = pa.py_buffer(payload["data"])
    reader = pa.ipc.open_stream(buf)
    df = pl.from_arrow(reader.read_all())

    from tradingkit.indicator import IndicatorContext
    ctx = IndicatorContext(df)
    series: pl.Series = indicator(ctx)
    return {"values": series.to_numpy().astype(np.float64).tobytes()}


async def _run_strategy(payload: dict) -> dict:
    strategy = pickle.loads(payload["strategy"])
    bar = pickle.loads(payload["bar"])
    signal = await strategy.on_bar(bar)
    return {"signal": signal.to_dict() if signal is not None else None}


async def _run_source(payload: dict) -> dict:
    import polars as pl
    import pyarrow as pa

    source = pickle.loads(payload["source"])
    df: pl.DataFrame = await source.get_historical_data(
        symbol=payload["symbol"],
        timeframe=payload["timeframe"],
        start_ts=payload["start_ts"],
        end_ts=payload["end_ts"],
        limit=payload["limit"],
    )
    arrow_table = df.to_arrow()
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, arrow_table.schema)
    writer.write_table(arrow_table)
    writer.close()
    return {"data": sink.getvalue().to_pybytes()}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--memory-mb", type=int, default=512)
    args = parser.parse_args()

    _apply_memory_limit(args.memory_mb)

    raw_stdin = sys.stdin.buffer.read()
    if len(raw_stdin) < 4:
        sys.exit(1)

    payload_len = struct.unpack(">I", raw_stdin[:4])[0]
    payload = pickle.loads(raw_stdin[4:4 + payload_len])

    task = payload.get("task")
    try:
        if task == "indicator":
            result = await _run_indicator(payload)
        elif task == "strategy":
            result = await _run_strategy(payload)
        elif task == "source":
            result = await _run_source(payload)
        else:
            raise ValueError(f"Unknown task: {task!r}")
    except Exception as exc:
        result = {"error": str(exc)}

    raw = pickle.dumps(result)
    sys.stdout.buffer.write(struct.pack(">I", len(raw)))
    sys.stdout.buffer.write(raw)
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    asyncio.run(main())
