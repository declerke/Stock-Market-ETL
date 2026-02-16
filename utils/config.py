import os
from pathlib import Path
from typing import Dict
from dotenv import load_dotenv

load_dotenv('.pyenv')

def load_config() -> Dict[str, str]:
    config = {}
    pyenv_path = Path(__file__).parent.parent / '.pyenv'
    
    if not pyenv_path.exists():
        raise FileNotFoundError(
            f".pyenv file not found at {pyenv_path}. "
            "Please create it based on .pyenv.example"
        )
    
    with open(pyenv_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                key, value = line.split('=', 1)
                config[key] = value
    
    return config

def get_credentials_path() -> Path:
    creds_path = Path(__file__).parent.parent / 'gcp_credentials.json'
    if not creds_path.exists():
        raise FileNotFoundError(
            f"GCP credentials not found at {creds_path}. "
            "Please download your service account key and save it as gcp_credentials.json"
        )
    return creds_path

class Config:
    SIMFIN_API_KEY = os.getenv('sim-fin-api-key')
    GCP_PROJECT_ID = os.getenv('project-name')
    GCS_BUCKET = os.getenv('bucket-name')
    GCP_REGION = os.getenv('region')
    DATASET_NAME = os.getenv('dataset-name')
    GCP_CREDENTIALS_PATH = 'gcp_credentials.json'

CONFIG = load_config()
CREDENTIALS_PATH = get_credentials_path()