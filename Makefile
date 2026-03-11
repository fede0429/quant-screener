run:
	PYTHONPATH=. uvicorn app.main:app --reload

migrate:
	PYTHONPATH=. alembic upgrade head

revision:
	PYTHONPATH=. alembic revision --autogenerate -m "auto"

test:
	PYTHONPATH=. pytest -q

seed:
	PYTHONPATH=. python scripts/seed_demo_data.py

demo:
	PYTHONPATH=. python scripts/run_demo_pipeline.py
