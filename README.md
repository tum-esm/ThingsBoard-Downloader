## Installation

**Set up virtual environment and install dependencies:**

```bash
python3.12 -m venv .venv
source .venv/bin/activate
poetry install --with dev
```

## Run mypy type check script

```
bash scripts/run_mypy.sh
```

## Initial setup

- Copy and fill `config.template.json` and rename to `config.json`
- Copy and fill `devides.template.json` and rename to `devices.json`
- Run `update_local_keys` to create config/keys.json with all available remote keys
