"""
srcodex Backend - Semantic Graph API
"""

from fastapi import FastAPI
from pathlib import Path
from typing import Dict, List, Any
import sys
sys.path.insert(0, str(Path(__file__).parent))
from services.file_tree import FileTreeService
DB_PATH = Path(__file__).parent.parent / "data" / "pmfw_main.db" ##Database path

app = FastAPI(title="srcodex API", version="0.1.0")
file_tree_service = FileTreeService(str(DB_PATH))

@app.get("/")
async def root() -> Dict[str, str]:
    """ API Root, returns basic information """
    return {
        "name": "srcodex API",
        "version": "0.1.0"
    }

@app.get("/projects/{project_id}/root")
async def get_project_root(project_id: str) -> Dict[str, Any]:
    """
    Get Project root metadata
    Example: GET /projects/pmfw_main/root
    """
    if project_id != Path(DB_PATH).stem:
        return {"error": f"Project '{project_id}' not found!"}, 404

    return file_tree_service.get_root()

@app.get("/projects/{project_id}/children")
def get_children(project_id: str, path: str = "") -> List[Dict[str, Any]]:
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

@app.get("/projects/{project_id}/search/files")
def search_file(project_id: str, q: str = "") -> List[Dict[str, Any]]:
    """
    Search for files by name or path (like Ctrl+P in VSCode).
    Examples:
        GET /projects/pmfw_main/search/files?q=msg
        → Returns files matching "msg": [msg.c, msg.h, ...]
        GET /projects/pmfw_main/search/files?q=app/pow
        → Returns files in "app" matching "pow": [mp1/src/app/power.c, ...]
    """
    if project_id != Path(DB_PATH).stem:
        return {"error": f"Project '{project_id}' not found!"}, 404

    return file_tree_service.search_file(q)

@app.get("/projects/{project_id}/search/symbols")
def search_symbol_global(project_id: str, q: str = "") -> List[Dict[str, Any]]:
    """
    Search for symbols globally across entire codebase (like Ctrl+Shift+F in VSCode).
    Examples:
        GET /projects/pmfw_main/search/symbols?q=voltage
        → Returns all voltage-related symbols (variables, macros, structs, ...)
        GET /projects/pmfw_main/search/symbols?q=Init
        → Returns all initialization functions
    """
    if project_id != Path(DB_PATH).stem:
        return {"error": f"Project '{project_id}' not found!"}, 404

    return file_tree_service.search_symbol_global(q)

@app.get("/projects/{project_id}/files/{file_path:path}/symbols")
def search_symbol_infile(project_id: str, file_path: str, q: str = "") -> List[Dict[str, Any]]:
    """
    Search symbols within a specific file (like Ctrl+F in VSCode on open file).
    Examples:
        GET /projects/pmfw_main/files/mp1/src/app/msg.c/symbols?q=Isr
        → Returns all symbols matching "Isr" in msg.c: [IsrHostMsg, IsrBiosMsg, ...]
        GET /projects/pmfw_main/files/mp1/src/app/power.c/symbols?q=Init
        → Returns all Init functions in power.c
    """
    if project_id != Path(DB_PATH).stem:
        return {"error": f"Project '{project_id}' not found!"}, 404

    return file_tree_service.search_symbol_infile(q, file_path)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
    
