# ThingsBoard Downloader

## Project Description

**ThingsBoard Downloader** is a Python-based tool that retrieves telemetry data from a ThingsBoard instance. It authenticates using a JWT token, fetches available telemetry keys, and **downloads device-specific data** for local storage and analysis. The downloaded data is stored in **Parquet files**, ensuring efficient storage and fast querying. The project supports configuration through JSON files and logs its operations for tracking purposes.

### **Key Features**

- ✅ **Secure authentication** using JWT tokens
- ✅ **Automatic key retrieval** from ThingsBoard
- ✅ **Efficient storage** using Parquet files for fast querying
- ✅ **Customizable data selection** via JSON configuration
- ✅ **Error logging and monitoring** for troubleshooting

---

## Installation

### **Prerequisites**

- Python **3.12 or later**
- Poetry installed (`pip install poetry`)

### **Set up the virtual environment and install dependencies**

```bash
python3 -m venv .venv  # Create virtual environment
source .venv/bin/activate  # Activate it
poetry install --with dev  # Install dependencies
```

---

## Initial Setup

1. **Configure the JSON files**:

   - Copy `config.template.json` → Rename it to `config.json`
   - Modify `config.json` with your ThingsBoard details

2. **Retrieve available telemetry keys**:

   - Run:

   ```bash
   python update_local_keys.py
   ```

   - This will create `config/keys.json`, listing all available remote keys.

3. **Select the telemetry keys you want**:
   - Open `keys.json` and **include/exclude** the keys you want to download.

---

## Running the Downloader

After setting up `config.json` and `keys.json`, start the download process:

```bash
python main.py
```

- **Logs** will be generated in the `logs/` folder.
- **Downloaded data** will be saved as **Parquet files** in the `data/` directory.

---

## Running Type Checks (MyPy)

To ensure type safety and catch potential errors, run:

```bash
bash scripts/run_mypy.sh
```

---

## Troubleshooting

### **Common Issues and Fixes**

#### **1. Poetry is not installed**

If you get an error related to `poetry`, install it using:

```bash
pip install poetry
```

#### **2. Virtual environment issues**

If `source .venv/bin/activate` doesn’t work, try:

```bash
.venv\Scripts\activate  # for Windows (PowerShell or CMD)
```

#### **3. ThingsBoard API authentication failure**

- Ensure your `config.json` contains the correct host URL, username, and password.
