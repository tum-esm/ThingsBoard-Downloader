## Installation

**Set up virtual environment and install dependencies:**

```bash
python3.12 -m venv .venv
source .venv/bin/activate
poetry install --with dev
```

## Initial setup

- Copy and fill `config.template.json` and rename to `config.json`
- Run `update_local_keys.py` to create config/keys.json with all available remote keys
- Open and configure `keys.json` to include/exclude keys to download

## Run mypy type check script

```
bash scripts/run_mypy.sh
```
