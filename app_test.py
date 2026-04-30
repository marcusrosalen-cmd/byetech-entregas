from fastapi import FastAPI
import os

app = FastAPI()

@app.get("/")
def root():
    return {"ok": True, "env": os.getenv("DATABASE_URL", "not set")[:30]}

@app.get("/api/health")
def health():
    return {"status": "ok", "port": os.getenv("PORT", "not set")}
