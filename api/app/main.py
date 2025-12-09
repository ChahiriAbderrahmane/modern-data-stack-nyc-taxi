from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from datetime import date
from typing import Optional
from . import models, database

app = FastAPI(title="NYC Taxi Gold Layer API")

# Route 1 : Récupérer les métriques financières
@app.get("/metrics/daily")
def get_daily_metrics(
    start_date: Optional[date] = None, 
    end_date: Optional[date] = None,
    limit: int = 10, 
    db: Session = Depends(database.get_db)
):
    """
    Récupère les métriques financières.
    - Filtre optionnel par date de début et de fin.
    """
    query = db.query(models.DailyMetric)

    # Application des filtres si l'utilisateur les fournit
    if start_date:
        query = query.filter(models.DailyMetric.pickup_date >= start_date)
    if end_date:
        query = query.filter(models.DailyMetric.pickup_date <= end_date)

    # Tri et exécution
    data = query.order_by(models.DailyMetric.pickup_date.desc())\
                .limit(limit)\
                .all()
    
    if not data:
        # Code 404 si aucune donnée trouvée pour cette période
        raise HTTPException(status_code=404, detail="No data found for this period")

    return data