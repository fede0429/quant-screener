class MetricsCollector:
    def __init__(self):
        self.counters = {}

    def inc(self, name: str, value: int = 1):
        self.counters[name] = self.counters.get(name, 0) + value

    def set(self, name: str, value):
        self.counters[name] = value

    def snapshot(self):
        return dict(self.counters)
