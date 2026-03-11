class ExecutionModeGuard:
    def __init__(self, allow_live: bool = False, live_whitelist: list[str] | None = None):
        self.allow_live = allow_live
        self.live_whitelist = set(live_whitelist or [])

    def validate(self, strategy_name: str, mode: str) -> None:
        if mode == "paper":
            return
        if mode != "live":
            raise ValueError("unsupported execution mode")
        if not self.allow_live:
            raise ValueError("live execution is disabled")
        if self.live_whitelist and strategy_name not in self.live_whitelist:
            raise ValueError("strategy is not approved for live execution")
