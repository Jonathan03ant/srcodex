"""
srcodex Backend - Semantic Graph API
"""

from fastapi import FastAPI
from pathlib import Path 
import sys
sys.path.insert(0, str(Path(__file__).parent))
from services.file_tree import FileTreeService 
DB_PATH = Path(__file__).parent.parent / "data" / "pmfw_main.db" ##Database path

app = FastAPI(title="srcodex API", version="0.1.0")
file_tree_service = FileTreeService(str(DB_PATH))

@app.get("/")
async def root():
    """ API Root, returns basic information """
    return {
        "name": "srcodex API",
        "version": "0.1.0"
    }

@app.get("/projects/{project_id}/root")
async def get_project_root(project_id: str):
    """
    Get Project root metadata
    Example: GET /projects/pmfw_main/root
    """
    if project_id != Path(DB_PATH).stem:
        return {"error": f"Project '{project_id}' not found!"}, 404
    
    return file_tree_service.get_root()

@app.get("/projects/{project_id}/children")
def get_children(project_id:str, path: str = ""):
    """
    Get Immediate children of a directory.
    Examples: 
        GET /projects/pmfw_main/children?path=
        → Returns root children: [mp1/, mpccx/, common/, test/]
        GET /projects/pmfw_main/children?path=mp1/src/app/
        → Returns contents of mp1/src/app/
    """
    if project_id != Path(DB_PATH).stem:
        return {"error": f"Project '{project_id}' not found!"}, 404
    
    return file_tree_service.get_children(path)
        
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
    
