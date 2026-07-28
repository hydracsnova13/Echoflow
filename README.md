# EcoFlow DAG Governor

A highly concurrent, deterministic Directed Acyclic Graph (DAG) orchestrator for AI pipelines, built with Go, Wails (React/Vanilla UI), and Python.

## System Prerequisites
Before building, ensure your system has the core compilers installed:
1. **Go** (1.20 or newer)
2. **Node.js** (18 or newer - required for building the frontend)
3. **Wails CLI**: Install via terminal using `go install github.com/wailsapp/wails/v2/cmd/wails@latest`

## Step 1: Initialize the AI Environment
The backend orchestration relies on an isolated Python environment. Open your terminal in the project root and run:


# Create the virtual environment
```cmd
python -m venv .venv
```

# Activate the environment
```cmd
.venv\Scripts\activate
```

# Install the Python dependencies
```cmd
pip install -r requirements.txt
```

# Run the automated setup
```cmd
python setup_script.py
```

## Step 2: Build and Run
With the environment configured, use the Wails CLI to compile the native desktop application.

For Development (Live Reloading):
```cmd
wails dev
```

For Production (Compiles a standalone .exe):
```cmd
wails build
```

The compiled executable will be located in build/bin/EcoFlow.exe.