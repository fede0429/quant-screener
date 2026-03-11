from datetime import datetime

class RunLogger:
    def __init__(self):
        self.events = []

    def log(self, level: str, message: str, **kwargs):
        event = {
            "ts": datetime.utcnow().isoformat(),
            "level": level,
            "message": message,
            "context": kwargs,
        }
        self.events.append(event)
        return event

    def info(self, message: str, **kwargs):
        return self.log("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs):
        return self.log("WARNING", message, **kwargs)

    def error(self, message: str, **kwargs):
        return self.log("ERROR", message, **kwargs)

    def snapshot(self):
        return list(self.events)
