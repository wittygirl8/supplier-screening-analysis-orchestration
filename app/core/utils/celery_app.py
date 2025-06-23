# app/celery_app.py

from celery import Celery

BROKER_URL = "amqp://guest:guest@rabbitmq:5672//"

celery_app = Celery("ens_tasks", broker=BROKER_URL, backend="rpc://")
