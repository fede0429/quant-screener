# Integration Notes

## What this pack adds
- A data adapter layer placeholder for `api/data_fetcher.py`
- A paper execution service that can be plugged behind approved proposals
- A replay bootstrap helper that writes audit events for imported runs
- A single bootstrap script to create:
  research reports -> portfolio run -> proposals -> risk decisions -> paper orders

## quant-screener mapping
- api/data_fetcher.py -> app/services/data_service.py
- api/monitor_engine.py -> app/services/replay_import_service.py
- api/server.py -> keep as research-core entry during migration
