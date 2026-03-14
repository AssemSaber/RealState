import uuid
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

from app.agent import chat

app = FastAPI(title="Real Estate Agent API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatRequest(BaseModel):
    message: str
    session_id: str | None = None


class ChatResponse(BaseModel):
    response: str
    session_id: str


@app.post("/chat", response_model=ChatResponse)
async def chat_endpoint(req: ChatRequest):
    session_id = req.session_id or str(uuid.uuid4())
    reply = await chat(req.message, session_id)
    return ChatResponse(response=reply, session_id=session_id)


@app.get("/")
async def serve_frontend():
    return FileResponse("frontend/index.html")
