# Multi-Agent Search & Optimizer

A FastAPI-powered system that optimizes user prompts and decomposes complex queries into independent search tasks for automated bots.

## Prerequisites

- Python 3.11+ or Docker
- DeepSeek API Key (set in `.env` file)

```env
DEEPSEEK_API_KEY=your_key_here
```

## How to Run

### Using Docker (Recommended)
```bash
docker build -t prompt-optimizer .
docker run -p 8000:8000 --env-file .env prompt-optimizer
```

### Local Setup
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn src.api.app:app --host 0.0.0.0 --port 8000
```

## How to Verify

### 1. Optimize Prompt
```bash
curl -X POST "http://localhost:8000/v1/optimize" \
     -H "Content-Type: application/json" \
     -d '{"prompt": "how to build a house"}'
```

### 2. Decompose into Search Tasks
```bash
curl -X POST "http://localhost:8000/v1/decompose" \
     -H "Content-Type: application/json" \
     -d '{"prompt": "research renewable energy trends 2024", "depth": "medium"}'
```

### 3. Monitor Tasks
```bash
curl "http://localhost:8000/v1/tasks"
```

## Features
- **PromptOptimizerAgent**: Enhances prompt quality and structure.
- **OrchestratorAgent**: Splits queries into specific search tasks with IDs and status tracking.
- **TaskManager**: In-memory state management for task lifecycles.
