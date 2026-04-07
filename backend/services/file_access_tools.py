"""
File Access Tools for Claude
Provides local filesystem access to Claude via tool calling
"""
import os
from pathlib import Path
from typing import Dict, List, Any


# TODO (hardcoded project rule for demo)
PROJECT_ROOT = Path("/utg/pmfwex")


def read_file(file_path: str):
    pass

def list_directories(dir_path: str=""):
    pass

def search_files(pattern: str, search_path: str = ""):
    pass