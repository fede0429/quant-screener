# Adapter Mapping

- api/factor_engine.py -> app/services/feature_service.py
- api/portfolio_lab.py -> app/services/portfolio_service.py
- api/portfolio_advisor.py -> app/services/portfolio_service.py
- api/model_engine.py -> app/services/model_service.py
- api/data_fetcher.py -> future app/services/data_service.py

Recommended order:
1. Feature adapter
2. Portfolio adapter
3. Model adapter
4. Batch import script
