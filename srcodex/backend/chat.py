from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from services.claude_service import ClaudeService
from fastapi.responses import StreamingResponse
import json


app = FastAPI()

try:
    claude_service = ClaudeService()
except ValueError as e:
    print(f"Warning: {e}")
    claude_service = None

class ChatRequest(BaseModel):
    """Request body for chat endpoint"""
    message: str
    conversation_history: list = []  # Optional: previous messages for context

class ChatResponse(BaseModel):
    """Response body for chat endpoint"""
    response: str

@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Chat endpoint - sends message to Claude and returns response
    Example:
        POST /api/chat
        {"message": "What does functionX do?"}

        Returns:
        {"response": "FunctionX is a function that..."}
    """
    if not claude_service:
        raise HTTPException(status_code=500, detail="Claude service not initialized - check API key")

    if not request.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    try:
        response = claude_service.send_message_with_tools(request.message, request.conversation_history)
        return ChatResponse(response=response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Claude API error: {str(e)}")

@app.post("/api/chat/stream")
async def chat_stream(request: ChatRequest):
    """
    Streaming chat endpoint - streams Claude's response in real-time

    Streams newline-delimited JSON objects:
    - Text chunks: {"type": "text", "content": "..."}
    - Token metadata: {"type": "tokens", "input": 1234, "output": 56, "total": 1290}
    """
    if not claude_service:
        raise HTTPException(status_code=500, detail="Claude service not initialized")

    if not request.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    try:
        def generate():
            for chunk in claude_service.stream_message_with_tools(request.message, request.conversation_history):
                # Stream as newline-delimited JSON
                yield json.dumps(chunk) + "\n"

        return StreamingResponse(generate(), media_type="application/x-ndjson")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Claude API error: {str(e)}")