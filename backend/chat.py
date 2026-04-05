from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from services.claude_service import ClaudeService
from fastapi.responses import StreamingResponse


app = FastAPI()

try:
    claude_service = ClaudeService()
except ValueError as e:
    print(f"Warning: {e}")
    claude_service = None

class ChatRequest(BaseModel):
    """Request body for chat endpoint"""
    message: str

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
        response = claude_service.send_message(request.message)
        return ChatResponse(response=response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Claude API error: {str(e)}")

@app.post("/api/chat/stream")
async def chat_stream(request: ChatRequest):
    """Streaming chat endpoint - streams Claude's response in real-time"""
    if not claude_service:
        raise HTTPException(status_code=500, detail="Claude service not initialized")

    if not request.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    try:
        def generate():
            for chunk in claude_service.stream_message(request.message):
                yield f"{chunk}"

        return StreamingResponse(generate(), media_type="text/plain")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Claude API error: {str(e)}")