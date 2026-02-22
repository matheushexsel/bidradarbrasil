"""
Entrypoint principal da API.
Importa do módulo api/main.py para manter compatibilidade
com o comando uvicorn main:app do Docker.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from api.main import app  # noqa: F401 - re-export para uvicorn
