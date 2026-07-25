# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.1.1] - Unreleased

### Fixed
- `Signal.is_buy` / `Signal.is_sell` were referenced by `PipelineResult.trades` but never
  defined on `Signal`, so calling `.trades` on any pipeline result raised `AttributeError`.
- `Fold` SQL generation for parametric ClickHouse aggregate functions: quantile-family
  functions (`quantile`, `topK`, `groupArrayMovingAvg`, ...) now repeat their parameter in
  the `Merge` expression as ClickHouse requires — previously `quantileMerge` silently
  computed the median instead of the requested level. Combinator-style functions (`sumIf`,
  ...) keep their arguments flat and correctly drop them in `Merge`.
- Stale `mholovion` GitHub username references replaced with `nivolon` throughout the
  source and docs.

### Added
- `[project.urls]` (Homepage/Repository/Issues) in `pyproject.toml`.
- Compiled C++ kernels are now cached by content hash (SHA-256) inside the runner process,
  so identical `.so` bytes are written to disk and `dlopen`'d only once instead of on every
  call.

### Changed
- CI (`ruff check .`) was silently exposed to drift: an unbounded `ruff>=0.4` dev dependency
  meant a newer ruff release could expand its default rule set and fail CI on old code with
  no corresponding change. Pinned to `ruff>=0.4,<0.17` and cleaned up ~180 pre-existing
  findings (mostly `UP037`/`UP045`/`I001` mechanical modernizations, plus a handful of real
  fixes — `ClassVar` on shared class-level lookup tables, `Self` return types on `__aenter__`,
  a `NaN` check rewritten from `x == x` to `math.isnan(x)`, and a Docker container-stop
  callback rebound through `functools.partial` instead of a loop-variable-capturing lambda);
  `ruff check .` is now clean.

## [0.1.0] - 2026-07-21

Initial public release.
