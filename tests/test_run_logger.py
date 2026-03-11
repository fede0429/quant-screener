from app.observability.run_logger import RunLogger

def test_run_logger_collects_events():
    logger = RunLogger()
    logger.info("started", run_id="r1")
    logger.warning("warned")
    assert len(logger.snapshot()) == 2
    assert logger.snapshot()[0]["level"] == "INFO"
