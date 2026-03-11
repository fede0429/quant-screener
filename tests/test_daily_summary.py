from app.ops.daily_summary import DailySummaryBuilder


def test_daily_summary_builder():
    summary = DailySummaryBuilder().build(
        run_id="r1",
        reports_count=2,
        proposals_count=2,
        approved_count=1,
        rejected_count=1,
        fills_count=1,
    )
    assert summary["run_id"] == "r1"
    assert summary["status"] == "paper_complete"
