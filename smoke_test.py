import os
import json
import time
from datetime import datetime


def main() -> int:
    # Set env vars only for local smoke test (do NOT commit credentials in code).
    # The real service reads these from Cloud Run env vars.
    os.environ.setdefault("OTELMS_USERNAME", os.environ.get("OTELMS_USERNAME", ""))
    os.environ.setdefault("OTELMS_PASSWORD", os.environ.get("OTELMS_PASSWORD", ""))
    os.environ.setdefault("GCS_BUCKET", os.environ.get("GCS_BUCKET", "otelms-data"))
    os.environ.setdefault("CALENDAR_RENDER_TIMEOUT", os.environ.get("CALENDAR_RENDER_TIMEOUT", "120"))

    if not os.environ.get("OTELMS_USERNAME") or not os.environ.get("OTELMS_PASSWORD"):
        raise SystemExit("Missing OTELMS_USERNAME/OTELMS_PASSWORD env vars for smoke test.")

    # Import after env vars are set (main.py validates env vars at import time).
    import main as scraper  # type: ignore

    driver = None
    try:
        driver = scraper.setup_driver()
        scraper.login_to_otelms(driver)
        try:
            data = scraper.extract_calendar_data(driver)
        except Exception:
            # Local debug artifacts (since GCS creds may not exist in this environment)
            ts = int(time.time())
            try:
                with open(f"/workspace/local_timeout_{ts}.html", "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
            except Exception:
                pass
            try:
                driver.save_screenshot(f"/workspace/local_timeout_{ts}.png")
            except Exception:
                pass
            try:
                diag = scraper.collect_calendar_diagnostics(driver)
                with open(f"/workspace/local_timeout_{ts}.json", "w", encoding="utf-8") as f:
                    json.dump(diag, f, ensure_ascii=False, indent=2)
                print(json.dumps({"diagnostics": diag}, ensure_ascii=False, indent=2))
            except Exception:
                pass
            raise

        result = {
            "status": "success" if data else "warning",
            "data_points": len(data),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "sample": data[:3],
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())

