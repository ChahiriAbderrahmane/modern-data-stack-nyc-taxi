import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# On récupère l'URL depuis le docker-compose ou une valeur par défaut
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://dbt_user:dbt_password@localhost:5434/dwh_db"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Fonction utilitaire pour récupérer la session DB dans chaque route
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()