# Local Development Setup Guide

This guide will help you run the Medical Bill Calculator application locally on your machine.

## Prerequisites

1. **Python 3.11+** - [Download Python](https://www.python.org/downloads/)
2. **Node.js 18+** and npm - [Download Node.js](https://nodejs.org/)
3. **Neo4j Database** (optional, for full functionality) - [Download Neo4j Desktop](https://neo4j.com/download/) or use Neo4j AuraDB Free tier
4. **Gemini API Key** (optional, for PDF parsing features) - [Get API Key](https://makersuite.google.com/app/apikey)

## Quick Start

### Step 1: Backend Setup (FastAPI)

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure database settings:**
   
   Copy the example config file:
   ```bash
   copy configs\db_config.json.example configs\db_config.json
   ```
   
   Edit `configs\db_config.json` with your settings:
   ```json
   {
     "neo4j": {
       "uri": "bolt://localhost:7687",
       "user": "neo4j",
       "password": "your_neo4j_password"
     },
     "gemini": {
       "api_key": "your_gemini_api_key"
     },
     "data": {
       "directory": "law_versions/ny_2018_01/Hamza_Manual_CSV/"
     }
   }
   ```
   
   **OR** use environment variables (recommended):
   ```bash
   # Windows PowerShell
   $env:NEO4J_URI="bolt://localhost:7687"
   $env:NEO4J_USER="neo4j"
   $env:NEO4J_PASSWORD="your_password"
   $env:GEMINI_API_KEY="your_api_key"
   $env:DATA_DIR="law_versions/ny_2018_01/Hamza_Manual_CSV/"
   ```

3. **Start the backend server:**
   ```bash
   python main_asgi.py
   ```
   
   Or using uvicorn directly:
   ```bash
   uvicorn main_asgi:app --host 0.0.0.0 --port 8000 --reload
   ```
   
   The API will be available at: `http://localhost:8000`
   
   Test it: `http://localhost:8000/v1/health`

### Step 2: Frontend Setup (React + Vite)

1. **Navigate to the UI directory:**
   ```bash
   cd ui
   ```

2. **Install Node dependencies:**
   ```bash
   npm install
   ```

3. **Create environment file:**
   
   Create a file `ui\.env` (or `ui\.env.local`) with:
   ```
   VITE_API_BASE_URL=http://localhost:8000
   ```

4. **Start the development server:**
   ```bash
   npm run dev
   ```
   
   The frontend will be available at: `http://localhost:8080`

## Running Both Services

You'll need **two terminal windows**:

### Terminal 1 - Backend:
```bash
# In project root
python main_asgi.py
```

### Terminal 2 - Frontend:
```bash
# In ui directory
cd ui
npm run dev
```

Then open your browser to: **http://localhost:8080**

## Troubleshooting

### Backend Issues

**Problem: ModuleNotFoundError for fastapi/uvicorn**
- **Solution:** Install missing packages:
  ```bash
  pip install fastapi uvicorn python-dotenv
  ```

**Problem: Neo4j connection failed**
- **Solution:** 
  - Make sure Neo4j is running locally (check Neo4j Desktop)
  - Verify your connection URI, username, and password
  - For testing without Neo4j, the app will show errors but may still work for some endpoints

**Problem: Port 8000 already in use**
- **Solution:** Change the port in `main_asgi.py` or use:
  ```bash
  uvicorn main_asgi:app --host 0.0.0.0 --port 8001
  ```
  Then update `VITE_API_BASE_URL` in frontend `.env` file

### Frontend Issues

**Problem: "VITE_API_BASE_URL is not configured yet"**
- **Solution:** Create `ui\.env` file with `VITE_API_BASE_URL=http://localhost:8000`

**Problem: CORS errors**
- **Solution:** The backend already has CORS middleware configured. Make sure:
  - Backend is running on port 8000
  - Frontend is pointing to the correct backend URL
  - Both are running simultaneously

**Problem: Port 8080 already in use**
- **Solution:** Vite will automatically try the next available port, or change it in `ui/vite.config.ts`

## Optional: Running Without Full Stack

If you want to test the frontend without the backend:

1. You can use the `simple_server.py` which has mock calculations:
   ```bash
   python simple_server.py --port 8080
   ```
   This serves both frontend and a simple mock API.

2. Or modify the frontend to work without API calls (not recommended for development).

## Environment Variables Reference

### Backend (.env or environment variables):
- `NEO4J_URI` - Neo4j database URI (default: `bolt://localhost:7687`)
- `NEO4J_USER` - Neo4j username (default: `neo4j`)
- `NEO4J_PASSWORD` - Neo4j password
- `GEMINI_API_KEY` - Google Gemini API key for PDF parsing
- `DATA_DIR` - Path to data directory (relative to project root)

### Frontend (ui/.env):
- `VITE_API_BASE_URL` - Backend API URL (default: `http://localhost:8000`)

## Next Steps

- Check API health: `http://localhost:8000/v1/health`
- View API docs: `http://localhost:8000/docs` (FastAPI auto-generated Swagger UI)
- Test calculator: Open `http://localhost:8080` and try calculating fees

