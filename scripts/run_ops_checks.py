from app.ops.service_health import ServiceHealthChecker
from app.ops.strategy_bootstrap import build_default_registry


def main():
    checker = ServiceHealthChecker()
    results = checker.run_all()
    for item in results:
        print(item)

    registry = build_default_registry()
    print("strategies:", registry.list())


if __name__ == "__main__":
    main()
