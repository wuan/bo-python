# Fuzzing

This directory contains [Atheris](https://github.com/google/atheris) fuzz
drivers for the parts of `blitzortung` that consume untrusted input:

| Driver | Target |
| --- | --- |
| `fuzz_websocket.py` | `blitzortung.websocket.decode` (remote websocket payloads) |
| `fuzz_strike_from_line.py` | `blitzortung.builder.strike.Strike.from_line` (text feed) |
| `fuzz_strike_from_json.py` | `blitzortung.builder.strike.Strike.from_json` (websocket JSON) |

The actual entry points live in `fuzz/harnesses.py` and are reused by the
property-based tests in `tests/fuzz/`, so they are verified with every test
run even where Atheris is unavailable.

## Local usage

Atheris provides Linux wheels only. On Linux (including WSL) install it and
build the package, then run a driver:

```bash
pip install atheris
pip install -e .
python fuzz/fuzz_websocket.py fuzz/corpus/websocket -max_total_time=60
```

On macOS Atheris has to be built against a locally compiled clang with
`compiler-rt`; see the [Atheris documentation](https://github.com/google/atheris#building-atheris).
Without it, use the Hypothesis-based tests instead (see below).

By default each driver only reads input from the command line. Pass a corpus
directory (for example `fuzz/corpus/websocket`) to let it learn from and save
interesting inputs. Crash reproducers are written to the working directory.

## Continuous fuzzing

`.github/workflows/fuzz.yml` runs every driver on a schedule and on manual
dispatch. It installs Atheris and runs each target for a bounded amount of
time, uploading any crash artifacts.

## Property-based fuzzing with Hypothesis

`tests/fuzz/` contains Hypothesis property tests that target the same
parsers. They run as part of the normal test suite and support longer runs:

```bash
# default (250 examples per test)
poetry run pytest tests/fuzz

# extended run (e.g. in CI or before a release)
BLITZORTUNG_FUZZ_EXAMPLES=20000 poetry run pytest tests/fuzz
```

To skip them during a normal test run:

```bash
poetry run pytest -m "not fuzz"
```
