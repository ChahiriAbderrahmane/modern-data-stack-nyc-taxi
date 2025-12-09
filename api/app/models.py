from sqlalchemy import Column, Date, Integer, Float, String
from .database import Base

class DailyMetric(Base):
    __tablename__ = "agg_daily_metrics"

    __table_args__ = {"schema": "gold"}

    pickup_date = Column(Date, primary_key=True, index=True)
    total_revenue_usd = Column(Float)
    total_trips = Column(Integer)
    avg_fare_usd = Column(Float)