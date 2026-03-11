from app.observability.config_validator import ConfigValidator

def main():
    validator = ConfigValidator()
    for path, config_type in [
        ("app/config/policy.paper.yaml", "policy"),
        ("app/config/strategy.quality_growth_v2.yaml", "strategy"),
    ]:
        try:
            print(validator.validate(path, config_type))
        except FileNotFoundError:
            print({"path": path, "ok": False, "error": "file_not_found"})

if __name__ == "__main__":
    main()
