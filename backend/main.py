"""
srcodex Backend - Semantic Graph API
"""

from fastapi import FastAPI

app = FastAPI(title="srcodex API", version="0.1.0")

@app.get("/")
async def root():
    """ API Root, returns basic information """
    return {
        "name": "srcodex API",
        "version": "0.1.0"
    }
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
    
    

