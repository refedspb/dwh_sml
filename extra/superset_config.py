"""Конфигурация демонстрационного Superset (не для продакшена)."""
import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me-demo-only")
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
