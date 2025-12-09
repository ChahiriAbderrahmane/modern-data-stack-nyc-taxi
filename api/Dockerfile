FROM python:3.10-slim

WORKDIR /app

# Copie des dépendances et installation
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie de tout le dossier 'app' dans le conteneur
COPY app /app/app

# On lance le serveur
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]