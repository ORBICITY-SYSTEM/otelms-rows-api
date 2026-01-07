"""
OTELMS Calendar Scraper v12.0
==========================================================================
Cloud Run hardened calendar scraper.

Key goals:
- Avoid flaky "JS render" timeouts by waiting on actual booking nodes.
- Avoid missing bookings due to calendar virtualization by scanning the calendar viewport.
- Avoid missing bookings due to date window differences by scanning multiple calendar views
  (month shifts) and de-duplicating by `resid`.
"""

import os
import sys
import json
import time
import logging
import re
import html as _html
import requests
from urllib.parse import quote as _urlquote
from datetime import datetime
from typing import List, Dict, Optional, Any
from flask import Flask, jsonify, request
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, 
    StaleElementReferenceException, WebDriverException
)
from google.cloud import storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Scraper version
SCRAPER_VERSION = "v12.9"

def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}

def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except Exception:
        return default

def _env_int_list(name: str, default: List[int]) -> List[int]:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    out: List[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except Exception:
            continue
    return out or default

def _debug_artifacts_enabled() -> bool:
    return _env_bool("DEBUG_ARTIFACTS", False)

def _env_str_list(name: str, default: List[str]) -> List[str]:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts or default

# Validate required environment variables
REQUIRED_ENV_VARS = ['OTELMS_USERNAME', 'OTELMS_PASSWORD', 'GCS_BUCKET']
for var in REQUIRED_ENV_VARS:
    if not os.environ.get(var):
        logger.error(f"Missing required environment variable: {var}")
        sys.exit(1)

# Configuration
OTELMS_USERNAME = os.environ['OTELMS_USERNAME']
OTELMS_PASSWORD = os.environ['OTELMS_PASSWORD']
OTELMS_LOGIN_URL = "https://116758.otelms.com/login_c2/"
OTELMS_CALENDAR_URL = "https://116758.otelms.com/reservation_c2/calendar/"
OTELMS_STATUS_URL = "https://116758.otelms.com/reservation_c2/status"
OTELMS_RLIST_URL = "https://116758.otelms.com/reservation_c2/rlist/1"
GCS_BUCKET = os.environ['GCS_BUCKET']
ROWS_API_KEY = os.environ.get('ROWS_API_KEY', '' )
ROWS_SPREADSHEET_ID = os.environ.get('ROWS_SPREADSHEET_ID', '')
ROWS_TABLE_ID = os.environ.get('ROWS_TABLE_ID', 'Table1')
ROWS_CALENDAR_TABLE_ID = os.environ.get('ROWS_CALENDAR_TABLE_ID', ROWS_TABLE_ID)
ROWS_STATUS_TABLE_ID = os.environ.get('ROWS_STATUS_TABLE_ID', 'Status')
ROWS_RLIST_CREATED_TABLE_ID = os.environ.get('ROWS_RLIST_CREATED_TABLE_ID', '')
ROWS_RLIST_CHECKIN_TABLE_ID = os.environ.get('ROWS_RLIST_CHECKIN_TABLE_ID', '')
ROWS_RLIST_CHECKOUT_TABLE_ID = os.environ.get('ROWS_RLIST_CHECKOUT_TABLE_ID', '')
ROWS_SYNC_MODE = os.environ.get('ROWS_SYNC_MODE', 'append').strip().lower()  # append|overwrite
ROWS_HISTORY_TABLE_ID = os.environ.get('ROWS_HISTORY_TABLE_ID', '')
SKIP_ROWS_IF_UNCHANGED = _env_bool("SKIP_ROWS_IF_UNCHANGED", True)

# Default active categories for rlist (can override via RLIST_ACTIVE_CATEGORIES env var, comma-separated)
DEFAULT_RLIST_ACTIVE_CATEGORIES = [
    "Suite with Sea view",
    "Delux suite with sea view",
    "Superior Suite with Sea View",
    "Interconnected Family Room",
]

MAX_RETRIES = 3
RETRY_DELAY = 5

app = Flask(__name__)

def setup_driver() -> webdriver.Chrome:
    """Setup Chrome driver with anti-detection measures"""
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,3000')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    # Reduce timer throttling / background rendering issues in headless containers
    chrome_options.add_argument('--disable-background-timer-throttling')
    chrome_options.add_argument('--disable-backgrounding-occluded-windows')
    chrome_options.add_argument('--disable-renderer-backgrounding')
    chrome_options.add_argument('--no-first-run')
    chrome_options.add_argument('--no-default-browser-check')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-popup-blocking')
    chrome_options.add_argument('--hide-scrollbars')
    chrome_options.add_argument('--mute-audio')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    chrome_options.binary_location = "/opt/chrome/chrome"
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    # Block known third-party widgets that can slow down/hang page load
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Network.setBlockedURLs", {"urls": [
            "https://otelmschat.otelms.com/*",
            "https://use.fontawesome.com/*",
        ]})
    except Exception as e:
        logger.debug(f"CDP network blocking unavailable: {e}")

    return driver

def _safe_execute(driver: webdriver.Chrome, script: str, default: Any = None) -> Any:
    try:
        return driver.execute_script(script)
    except Exception:
        return default

def collect_calendar_diagnostics(driver: webdriver.Chrome) -> Dict[str, Any]:
    """Collect high-signal diagnostics to understand render failures."""
    return {
        "url": getattr(driver, "current_url", ""),
        "readyState": _safe_execute(driver, "return document.readyState", ""),
        "hasJQuery": bool(_safe_execute(driver, "return typeof window.jQuery !== 'undefined'", False)),
        "jQueryActive": _safe_execute(driver, "return (window.jQuery ? window.jQuery.active : null)", None),
        "calendarTdCount": _safe_execute(driver, "return document.querySelectorAll('td.calendar_td').length", 0),
        "calendarItemCount": _safe_execute(driver, "return document.querySelectorAll('div.calendar_item').length", 0),
        "calendarItemResidCount": _safe_execute(driver, "return document.querySelectorAll('div.calendar_item[resid]').length", 0),
        "calendarContainerPresent": bool(_safe_execute(driver, "return !!document.querySelector('.calendar_container')", False)),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "version": SCRAPER_VERSION,
    }

def save_debug_artifacts(driver: webdriver.Chrome, name: str, extra: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Save screenshot and page source to GCS for debugging"""
    try:
        timestamp = int(time.time())
        
        # Save screenshot
        screenshot_data = driver.get_screenshot_as_png()
        
        # Save page source
        page_source = driver.page_source
        
        # Upload to GCS
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        
        screenshot_blob = bucket.blob(f'debug/{name}_{timestamp}.png')
        screenshot_blob.upload_from_string(screenshot_data, content_type='image/png')
        
        source_blob = bucket.blob(f'debug/{name}_{timestamp}.html')
        source_blob.upload_from_string(page_source, content_type='text/html')
        
        if extra is not None:
            extra_blob = bucket.blob(f'debug/{name}_{timestamp}.json')
            extra_blob.upload_from_string(
                json.dumps(extra, ensure_ascii=False, indent=2),
                content_type='application/json'
            )

        logger.info(f"Debug artifacts saved: {name}_{timestamp}")
        return f"gs://{GCS_BUCKET}/debug/{name}_{timestamp}"
        
    except Exception as e:
        logger.error(f"Failed to save debug artifacts: {e}")
        return None

def retry_on_failure(func, max_retries=MAX_RETRIES, delay=RETRY_DELAY):
    """Retry decorator for flaky operations"""
    def wrapper(*args, **kwargs):
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay * (attempt + 1))
        return None
    return wrapper

def login_to_otelms(driver: webdriver.Chrome) -> None:
    """Login to OTELMS with multiple fallback strategies"""
    logger.info("Navigating to login page...")
    driver.get(OTELMS_LOGIN_URL)
    
    wait = WebDriverWait(driver, 15)
    
    try:
        # Wait for login form
        username_field = wait.until(EC.presence_of_element_located((By.ID, "userLogin")))
        password_field = wait.until(EC.presence_of_element_located((By.ID, "password")))
        
        username_field.clear()
        username_field.send_keys(OTELMS_USERNAME)
        password_field.clear()
        password_field.send_keys(OTELMS_PASSWORD)
        
        logger.info("Credentials entered, attempting login...")
        
        # Try Enter key first (most reliable)
        try:
            password_field.send_keys(Keys.RETURN)
            logger.info("Submitted via Enter key")
        except:
            # Fallback to button click
            submit_selectors = [
                (By.XPATH, "//button[contains(text(), 'შესვლა')]"),
                (By.XPATH, "//button[contains(text(), 'Login')]"),
                (By.CSS_SELECTOR, "button[type='submit']"),
                (By.XPATH, "//form//button"),
                (By.CSS_SELECTOR, "input[type='submit']"),
            ]
            
            button_found = False
            for by, selector in submit_selectors:
                try:
                    submit_button = driver.find_element(by, selector)
                    submit_button.click()
                    logger.info(f"Submitted via button: {selector}")
                    button_found = True
                    break
                except NoSuchElementException:
                    continue
            
            if not button_found:
                save_debug_artifacts(driver, 'login_no_button')
                raise Exception("Could not find submit button")
        
        # Wait for successful login (URL change or specific element)
        try:
            wait.until(EC.url_changes(OTELMS_LOGIN_URL))
            logger.info(f"Login successful! Redirected to: {driver.current_url}")
        except TimeoutException:
            # Check if we're already logged in (URL didn't change but we're on dashboard)
            if driver.current_url != OTELMS_LOGIN_URL:
                logger.info("Login successful (already logged in)")
            else:
                save_debug_artifacts(driver, 'login_failed')
                raise Exception("Login failed - no redirect occurred")
        
        # Extra wait for page stabilization
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
        
    except Exception as e:
        save_debug_artifacts(driver, 'login_error')
        raise Exception(f"Login failed: {e}")

def _kick_calendar_render(driver: webdriver.Chrome) -> None:
    """
    OTELMS calendar scrolling is usually inside `.calendar_container` (not window).
    We scroll both the container and window to trigger any lazy rendering/handlers.
    """
    driver.execute_script(
        """
        const el = document.querySelector('.calendar_container');
        if (el) {
          // Vertical
          el.scrollTop = el.scrollHeight;
          el.dispatchEvent(new Event('scroll', {bubbles: true}));
          // Horizontal (some calendars virtualize columns)
          el.scrollLeft = el.scrollWidth;
          el.dispatchEvent(new Event('scroll', {bubbles: true}));
          el.scrollTop = 0;
          el.scrollLeft = 0;
          el.dispatchEvent(new Event('scroll', {bubbles: true}));
        }
        window.scrollTo(0, document.body.scrollHeight);
        window.scrollTo(0, 0);
        """
    )

def _submit_calendar_form(driver: webdriver.Chrome, month_shift: int, today: bool, date_shift: str = "0") -> None:
    """
    The calendar is primarily server-rendered on POST to /reservation_c2/calendar
    using hidden inputs:
      - month_shift (int)
      - today (0/1)
      - date_shift ("0" or "YYYY-MM-DD")
    """
    driver.execute_script(
        """
        const monthShift = String(arguments[0]);
        const todayVal = arguments[1] ? "1" : "0";
        const dateShift = String(arguments[2] || "0");

        const frm = document.getElementById('frmdata');
        if (!frm) return;

        const ms = document.getElementById('month_shift');
        const td = document.getElementById('today');
        const ds = document.getElementById('date_shift');
        if (ms) ms.value = monthShift;
        if (td) td.value = todayVal;
        if (ds) ds.value = dateShift;

        const dateInput = document.getElementById('datein100');
        if (dateInput && dateShift && dateShift !== "0") {
          dateInput.value = dateShift;
        }
        frm.submit();
        """,
        int(month_shift),
        bool(today),
        str(date_shift),
    )

def _get_calendar_container_metrics(driver: webdriver.Chrome) -> Dict[str, int]:
    metrics = driver.execute_script(
        """
        const el = document.querySelector('.calendar_container');
        if (!el) return {present: 0, scrollHeight: 0, clientHeight: 0, scrollWidth: 0, clientWidth: 0};
        return {
          present: 1,
          scrollHeight: el.scrollHeight || 0,
          clientHeight: el.clientHeight || 0,
          scrollWidth: el.scrollWidth || 0,
          clientWidth: el.clientWidth || 0
        };
        """
    )
    if not isinstance(metrics, dict):
        return {"present": 0, "scrollHeight": 0, "clientHeight": 0, "scrollWidth": 0, "clientWidth": 0}
    return {
        "present": int(metrics.get("present") or 0),
        "scrollHeight": int(metrics.get("scrollHeight") or 0),
        "clientHeight": int(metrics.get("clientHeight") or 0),
        "scrollWidth": int(metrics.get("scrollWidth") or 0),
        "clientWidth": int(metrics.get("clientWidth") or 0),
    }

def _scroll_calendar_container(driver: webdriver.Chrome, top: int = 0, left: int = 0) -> None:
    driver.execute_script(
        """
        const el = document.querySelector('.calendar_container');
        if (!el) return;
        el.scrollTop = arguments[0];
        el.scrollLeft = arguments[1];
        el.dispatchEvent(new Event('scroll', {bubbles: true}));
        """,
        int(top),
        int(left),
    )

def _collect_calendar_items_js(driver: webdriver.Chrome) -> List[Dict[str, Any]]:
    raw = driver.execute_script(
        """
        return Array.from(document.querySelectorAll('div.calendar_item[resid]')).map(el => {
          const resid = el.getAttribute('resid');
          const status = el.getAttribute('status') || '';
          const element_id = el.getAttribute('id') || '';
          const booking_nam = (el.querySelector('.calendar_booking_nam')?.textContent || '').trim();
          const booking_info = (el.querySelector('.calendar_booking_info')?.textContent || '').trim();
          const balance = (el.querySelector('.balance_negative span, .balance_positive span')?.textContent || '').trim();
          const tooltip = (el.getAttribute('data-title') || '').trim();
          return {resid, status, element_id, booking_nam, booking_info, balance, tooltip};
        });
        """
    )
    return raw if isinstance(raw, list) else []

def _parse_tooltip_fields(tooltip_html: str) -> Dict[str, str]:
    """
    Tooltip is HTML containing structured lines in Georgian, e.g.
      "შეკვეთა №7296 (ჯავშანი), whatsapp 577250205"
      "სტუმარი:  ჯაბა პაშკოვსკი"
      "შემოსვლა: 2025-12-27"
      "გასვლა: 2026-01-08"
      "ბალანსი: -500.00, (500.00)"
    """
    if not tooltip_html:
        return {}

    text = _html.unescape(tooltip_html)
    # Convert common separators to newlines before stripping tags.
    text = re.sub(r"</div>\s*<div[^>]*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<br\\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("\r", "")
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

    out: Dict[str, str] = {}
    for ln in lines:
        if ln.startswith("შეკვეთა №"):
            m = re.search(r"შეკვეთა №\s*(\d+)", ln)
            if m:
                out["booking_id"] = m.group(1)
            # Everything after comma is usually source/contact
            parts = [p.strip() for p in ln.split(",") if p.strip()]
            if len(parts) >= 2:
                out["source"] = parts[1]
        elif "სტუმარი:" in ln:
            out["guest_name"] = ln.split("სტუმარი:", 1)[1].strip()
        elif ln.startswith("შემოსვლა:"):
            out["date_in"] = ln.split(":", 1)[1].strip()
        elif ln.startswith("გასვლა:"):
            out["date_out"] = ln.split(":", 1)[1].strip()
        elif "ბალანსი:" in ln:
            # Take the first numeric value after "ბალანსი:"
            m = re.search(r"ბალანსი:\s*([+-]?\d+(?:\.\d+)?)", ln)
            if m:
                out["balance"] = m.group(1)
        elif "ტელეფონი:" in ln:
            out["phone"] = ln.split("ტელეფონი:", 1)[1].strip()
        elif "პასუხისმგებელი:" in ln:
            out["responsible"] = ln.split("პასუხისმგებელი:", 1)[1].strip()

    return out

def scan_calendar_items(driver: webdriver.Chrome, max_scan_seconds: int = 60) -> List[Dict[str, Any]]:
    """
    Some OTELMS calendars virtualize DOM: only visible rows' bookings exist in the DOM.
    This scans the scroll container top→bottom to accumulate all unique `resid` items.
    """
    start = time.time()
    items_by_resid: Dict[str, Dict[str, Any]] = {}

    metrics = _get_calendar_container_metrics(driver)
    if metrics["present"] != 1 or metrics["clientHeight"] <= 0:
        # No container; just collect what exists.
        for it in _collect_calendar_items_js(driver):
            resid = (it.get("resid") or "").strip()
            if resid:
                items_by_resid[resid] = it
        return list(items_by_resid.values())

    max_top = max(0, metrics["scrollHeight"] - metrics["clientHeight"])
    max_left = max(0, metrics["scrollWidth"] - metrics["clientWidth"])
    step_y = max(200, int(metrics["clientHeight"] * 0.85))
    step_x = max(200, int(metrics["clientWidth"] * 0.85))

    def collect_now() -> int:
        before = len(items_by_resid)
        for it in _collect_calendar_items_js(driver):
            resid = (it.get("resid") or "").strip()
            if resid and resid not in items_by_resid:
                items_by_resid[resid] = it
        return len(items_by_resid) - before

    # Prime with whatever is currently visible
    collect_now()

    last_new_time = time.time()

    # Scan a grid of scroll positions (x and y). Some calendars virtualize both axes.
    left_positions = list(range(0, max_left + 1, step_x))
    if max_left not in left_positions:
        left_positions.append(max_left)

    # Two passes over X to catch late renders (0->max, then max->0)
    for left_positions_pass in (left_positions, list(reversed(left_positions))):
        for left in left_positions_pass:
            if time.time() - start > max_scan_seconds:
                break
            # Down then up for each column
            for direction in (1, -1):
                positions = range(0, max_top + 1, step_y) if direction == 1 else range(max_top, -1, -step_y)
                for top in positions:
                    if time.time() - start > max_scan_seconds:
                        break
                    try:
                        _scroll_calendar_container(driver, top=top, left=left)
                    except Exception:
                        pass
                    # Allow time for virtualized content to mount
                    time.sleep(0.6)
                    new_found = collect_now()
                    if new_found > 0:
                        last_new_time = time.time()

                    # If nothing new has appeared for a while, stop early.
                    if len(items_by_resid) > 0 and (time.time() - last_new_time) > 6.0:
                        break

    # Reset scroll
    try:
        _scroll_calendar_container(driver, top=0, left=0)
    except Exception:
        pass

    return list(items_by_resid.values())

def ensure_calendar_rendered(driver: webdriver.Chrome, calendar_url: str, timeout_seconds: int = 120) -> int:
    """
    Wait until bookings are actually rendered into the DOM.

    Strategy:
    - Wait for calendar grid/container to exist.
    - "Kick" render via search form submit if available.
    - Poll for `div.calendar_item[resid]` count > 0 and stable.
    - Use jQuery activity (when present) as an additional signal.
    """
    wait = WebDriverWait(driver, min(timeout_seconds, 30))
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))

    # Calendar grid (server-rendered) tends to exist before bookings.
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "td.calendar_td, .calendar_container")))
    except TimeoutException:
        # Still allow the polling loop to run; diagnostics will show what's missing.
        pass

    # If a search button exists, submit once to force server-side calendar render.
    try:
        search_btn = driver.find_element(By.ID, "search_form_submit")
        driver.execute_script("arguments[0].click();", search_btn)
        WebDriverWait(driver, 30).until(lambda d: d.execute_script("return document.readyState") in ("interactive", "complete"))
        time.sleep(0.5)
    except Exception:
        pass

    last_count = -1
    last_increase = time.time()
    start = time.time()

    while time.time() - start < timeout_seconds:
        # Sometimes the app redirects back to login during/after calendar load.
        if "login" in (driver.current_url or "").lower():
            logger.warning("Detected redirect to login during calendar render; re-authenticating...")
            login_to_otelms(driver)
            driver.get(calendar_url)
            time.sleep(0.5)

        # Actively trigger scroll handlers / lazy rendering during the wait.
        try:
            _kick_calendar_render(driver)
        except Exception:
            pass

        count = _safe_execute(driver, "return document.querySelectorAll('div.calendar_item[resid]').length", 0)
        ajax_active = _safe_execute(driver, "return (window.jQuery ? window.jQuery.active : 0)", 0)

        # We want "some bookings" and "count stopped increasing" for a short window.
        if isinstance(count, int) and count > 0:
            now = time.time()
            if count > last_count:
                last_increase = now
                last_count = count
            # If jQuery is present but background XHR keeps happening, don't block forever.
            if (now - last_increase) >= 8 and (ajax_active == 0 or (now - start) > 10):
                return last_count

        time.sleep(1.5)

    raise TimeoutException(f"Timed out after {timeout_seconds}s waiting for calendar bookings to render")

def _load_calendar_view(driver: webdriver.Chrome, month_shift: int, today: bool, date_shift: str) -> int:
    """Load a specific calendar view by submitting the server-rendered calendar form."""
    driver.get(OTELMS_CALENDAR_URL)

    if "login" in (driver.current_url or "").lower():
        logger.warning("Calendar URL redirected to login; re-authenticating...")
        login_to_otelms(driver)
        driver.get(OTELMS_CALENDAR_URL)

    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.ID, "frmdata")))
    _submit_calendar_form(driver, month_shift=month_shift, today=today, date_shift=date_shift)

    render_timeout = _env_int("CALENDAR_RENDER_TIMEOUT", 300)
    rendered_count = ensure_calendar_rendered(driver, OTELMS_CALENDAR_URL, timeout_seconds=render_timeout)
    return rendered_count

def extract_calendar_data(driver: webdriver.Chrome) -> Dict[str, Any]:
    """
    Extract calendar data across one or more calendar views.

    By default v12.0 scans month shifts -1,0,1 to reduce "missing bookings"
    caused by differing default date windows in headless environments.
    """
    month_shifts = _env_int_list("CALENDAR_MONTH_SHIFTS", default=[-1, 0, 1])
    today = _env_bool("CALENDAR_TODAY", True)
    date_shift = os.environ.get("CALENDAR_DATE_SHIFT", "0").strip() or "0"
    scan_seconds = _env_int("CALENDAR_SCAN_SECONDS", 90)

    config = {
        "month_shifts": month_shifts,
        "today": today,
        "date_shift": date_shift,
        "scan_seconds": scan_seconds,
        "render_timeout_seconds": _env_int("CALENDAR_RENDER_TIMEOUT", 300),
    }

    logger.info(
        "Calendar scan config: "
        f"month_shifts={config['month_shifts']}, "
        f"today={config['today']}, "
        f"date_shift={config['date_shift']}, "
        f"scan_seconds={config['scan_seconds']}, "
        f"render_timeout={config['render_timeout_seconds']}"
    )

    items_by_resid: Dict[str, Dict[str, Any]] = {}
    views_scanned: List[Dict[str, Any]] = []

    try:
        for ms in month_shifts:
            # IMPORTANT:
            # If `today=1`, OTELMS tends to force the calendar to the current date/month,
            # which can effectively ignore `month_shift`. Only use `today=1` for the "current"
            # view (month_shift=0) when no explicit date_shift is provided.
            effective_today = bool(today and ms == 0 and date_shift == "0")

            logger.info(
                f"Loading calendar view: month_shift={ms}, today={effective_today}, date_shift={date_shift}"
            )
            rendered_count = _load_calendar_view(
                driver,
                month_shift=ms,
                today=effective_today,
                date_shift=date_shift,
            )
            logger.info(f"Rendered {rendered_count} booking nodes in view month_shift={ms}")

            if _debug_artifacts_enabled():
                save_debug_artifacts(driver, f'calendar_view_ms_{ms}', extra=collect_calendar_diagnostics(driver))

            raw_items = scan_calendar_items(driver, max_scan_seconds=scan_seconds)
            views_scanned.append({
                "month_shift": ms,
                "today": effective_today,
                "date_shift": date_shift,
                "rendered_count": rendered_count,
                "scanned_count": len(raw_items),
            })
            logger.info(f"Scanned {len(raw_items)} unique items in this view (month_shift={ms})")

            for item in raw_items:
                resid = (item.get("resid") or "").strip()
                if not resid:
                    continue

                # Normalize / enrich
                tooltip = (item.get("tooltip") or "").strip()
                tooltip_fields = _parse_tooltip_fields(tooltip)

                # Parse `booking_nam` (fast path)
                booking_nam = (item.get("booking_nam") or "").strip()
                booking_id = ""
                guest_name = ""
                if "B:" in booking_nam:
                    parts = booking_nam.split("B:", 1)[1].split(",")
                    if len(parts) >= 2:
                        booking_id = parts[0].strip()
                        guest_name = parts[1].strip()

                # Fall back to tooltip if parsing failed
                booking_id = booking_id or tooltip_fields.get("booking_id", "") or resid
                guest_name = guest_name or tooltip_fields.get("guest_name", "")

                source = (item.get("booking_info") or "").strip().rstrip(",") or tooltip_fields.get("source", "")
                balance = (item.get("balance") or "").strip() or tooltip_fields.get("balance", "")

                candidate: Dict[str, Any] = {
                    "resid": resid,
                    "booking_id": booking_id,
                    "guest_name": guest_name,
                    "source": source,
                    "balance": balance,
                    "status": (item.get("status") or "").strip(),
                    "element_id": (item.get("element_id") or "").strip(),
                    "date_in": tooltip_fields.get("date_in", ""),
                    "date_out": tooltip_fields.get("date_out", ""),
                    "phone": tooltip_fields.get("phone", ""),
                    "responsible": tooltip_fields.get("responsible", ""),
                    "tooltip": tooltip if _debug_artifacts_enabled() else "",
                    "calendar_month_shift": ms,
                    "extracted_at": datetime.utcnow().isoformat() + "Z",
                }

                existing = items_by_resid.get(resid)
                if not existing:
                    items_by_resid[resid] = candidate
                else:
                    # Merge: keep the first but fill missing fields with newer data.
                    for k, v in candidate.items():
                        if k not in existing or existing[k] in ("", None):
                            existing[k] = v

        data_rows = list(items_by_resid.values())
        logger.info(f"Extracted {len(data_rows)} unique booking records across {len(month_shifts)} views")
        return {
            "rows": data_rows,
            "views_scanned": views_scanned,
            "config": config,
        }

    except TimeoutException:
        save_debug_artifacts(driver, 'calendar_timeout', extra=collect_calendar_diagnostics(driver))
        raise Exception("Calendar render timeout - booking blocks not found")
    except Exception as e:
        save_debug_artifacts(driver, 'calendar_error', extra=collect_calendar_diagnostics(driver))
        raise Exception(f"Calendar extraction failed: {e}")

def save_to_gcs(data: List[Dict], bucket_name: str) -> str:
    """Save data to GCS with validation"""
    try:
        logger.info(f"Saving {len(data)} records to GCS...")
        
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        # Validate bucket exists
        if not bucket.exists():
            raise Exception(f"GCS bucket '{bucket_name}' does not exist")
        
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        filename = f'otelms_calendar_{timestamp}.json'
        
        blob = bucket.blob(filename)
        blob.upload_from_string(
            json.dumps(data, indent=2, ensure_ascii=False),
            content_type='application/json'
        )
        
        logger.info(f"Saved to gs://{bucket_name}/{filename}")
        return filename
        
    except Exception as e:
        logger.error(f"GCS save failed: {e}")
        raise

def save_json_to_gcs(data: Any, bucket_name: str, prefix: str) -> str:
    """Save arbitrary JSON-serializable data to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        raise Exception(f"GCS bucket '{bucket_name}' does not exist")

    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename = f'{prefix}_{timestamp}.json'
    blob = bucket.blob(filename)
    blob.upload_from_string(
        json.dumps(data, indent=2, ensure_ascii=False),
        content_type='application/json'
    )
    logger.info(f"Saved to gs://{bucket_name}/{filename}")
    return filename

def _gcs_read_json(bucket_name: str, blob_name: str) -> Any:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if not blob.exists():
        return None
    raw = blob.download_as_bytes()
    return json.loads(raw.decode("utf-8"))

def _gcs_write_json(bucket_name: str, blob_name: str, data: Any) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False, indent=2),
        content_type="application/json",
    )

def _make_index(rows: List[Dict[str, Any]], key_fields: List[str]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        parts = [str(r.get(k, "")).strip() for k in key_fields]
        key = "|".join(parts).strip()
        if not key or key == "|".join([""] * len(key_fields)):
            continue
        idx[key] = r
    return idx

def _diff_rows(prev: List[Dict[str, Any]], cur: List[Dict[str, Any]], key_fields: List[str], track_fields: List[str]) -> List[Dict[str, Any]]:
    """
    Return change events:
      - create: new key appears
      - delete: key disappears
      - update: tracked field changed
    """
    prev_idx = _make_index(prev, key_fields)
    cur_idx = _make_index(cur, key_fields)
    now = datetime.utcnow().isoformat() + "Z"

    events: List[Dict[str, Any]] = []

    for k, row in cur_idx.items():
        if k not in prev_idx:
            events.append({
                "source": "unknown",
                "entity_key": k,
                "change_type": "create",
                "field": "",
                "old_value": "",
                "new_value": "",
                "detected_at": now,
            })
            continue
        old = prev_idx[k]
        for f in track_fields:
            ov = str(old.get(f, "")).strip()
            nv = str(row.get(f, "")).strip()
            if ov != nv:
                events.append({
                    "source": "unknown",
                    "entity_key": k,
                    "change_type": "update",
                    "field": f,
                    "old_value": ov,
                    "new_value": nv,
                    "detected_at": now,
                })

    for k in prev_idx.keys():
        if k not in cur_idx:
            events.append({
                "source": "unknown",
                "entity_key": k,
                "change_type": "delete",
                "field": "",
                "old_value": "",
                "new_value": "",
                "detected_at": now,
            })

    return events

def _append_history(events: List[Dict[str, Any]], source: str, snapshot_file: str) -> bool:
    if not ROWS_HISTORY_TABLE_ID or not events:
        return False
    # Fill source + snapshot reference
    for e in events:
        e["source"] = source
        e["snapshot_file"] = snapshot_file
    return sync_to_rows(
        events,
        table_id=ROWS_HISTORY_TABLE_ID,
        mode="append",
        mapper=lambda item: [
            item.get("source", ""),
            item.get("entity_key", ""),
            item.get("change_type", ""),
            item.get("field", ""),
            item.get("old_value", ""),
            item.get("new_value", ""),
            item.get("snapshot_file", ""),
            item.get("detected_at", ""),
        ],
    )

def _rows_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {ROWS_API_KEY}",
        "Content-Type": "application/json",
    }

def _rows_get(path: str) -> requests.Response:
    url = f"https://api.rows.com/v1{path}"
    return requests.get(url, headers={"Authorization": f"Bearer {ROWS_API_KEY}"}, timeout=30)

def _rows_post_json(path: str, payload: Any, accept_json: bool = True) -> requests.Response:
    url = f"https://api.rows.com/v1{path}"
    headers = _rows_headers().copy()
    if accept_json:
        headers["Accept"] = "application/json"
    return requests.post(url, headers=headers, json=payload, timeout=30)

def _a1_col(n: int) -> str:
    """1-indexed column number -> A1-style column label (A, B, ..., AA, AB, ...)."""
    if n <= 0:
        raise ValueError("Column index must be >= 1")
    out = ""
    while n:
        n, rem = divmod(n - 1, 26)
        out = chr(ord("A") + rem) + out
    return out

def _rows_append_range_for_width(width: int) -> str:
    """
    Rows API append requires a range in the URL (A1 notation).
    IMPORTANT: the range must accommodate *multiple rows* being appended.
    Use an unbounded row range across columns (e.g. A:K), not a single row (A1:K1).
    """
    last = _a1_col(width)
    return f"A:{last}"

def _rows_clear_table(table_id: str) -> bool:
    """
    Best-effort "overwrite" support. Rows API has evolved; we attempt common clear endpoints.
    If unsupported, we fall back to append mode.
    """
    # Candidate endpoints (try in order)
    candidates = [
        f"https://api.rows.com/v1/spreadsheets/{ROWS_SPREADSHEET_ID}/tables/{table_id}/values:clear",
        f"https://api.rows.com/v1/spreadsheets/{ROWS_SPREADSHEET_ID}/tables/{table_id}/values/clear",
    ]
    for url in candidates:
        try:
            resp = requests.post(url, headers=_rows_headers(), json={}, timeout=30)
            if resp.status_code in (200, 204):
                logger.info(f"Rows table cleared via {url}")
                return True
            if resp.status_code in (404, 405):
                continue
            logger.warning(f"Rows clear failed ({resp.status_code}) via {url}: {resp.text}")
        except Exception as e:
            logger.warning(f"Rows clear error via {url}: {e}")
    return False

def _rows_append_values(table_id: str, values: List[List[Any]]) -> bool:
    """
    Append rows into a table using the official Rows API endpoint:
      POST /spreadsheets/{spreadsheet_id}/tables/{table_id}/values/{range}:append
    """
    if not values:
        return True

    width = max((len(r) for r in values if isinstance(r, list)), default=0)
    if width <= 0:
        return True

    a1_range = _rows_append_range_for_width(width)
    encoded_range = _urlquote(a1_range, safe="")
    url = f"https://api.rows.com/v1/spreadsheets/{ROWS_SPREADSHEET_ID}/tables/{table_id}/values/{encoded_range}:append"
    payload = {"values": values}

    for attempt in range(3):
        resp = requests.post(url, headers=_rows_headers(), json=payload, timeout=30)
        if resp.status_code in (200, 201):
            return True
        if resp.status_code == 429:
            retry_after = int(resp.headers.get('Retry-After', 60))
            logger.warning(f"Rows rate limited, retrying after {retry_after}s...")
            time.sleep(retry_after)
            continue
        logger.error(f"Rows append failed: {resp.status_code} - {resp.text}")
        return False

    return False

def _rows_overwrite_cells(table_id: str, a1_range: str, row_values: List[str]) -> bool:
    """
    Write a single row into an explicit range using the official cells/{range} overwrite endpoint.
    """
    encoded_range = _urlquote(a1_range, safe="")
    cells_row = [{"value": v} for v in row_values]
    payload = {"cells": [cells_row]}
    resp = _rows_post_json(
        f"/spreadsheets/{ROWS_SPREADSHEET_ID}/tables/{table_id}/cells/{encoded_range}",
        payload,
        accept_json=True,
    )
    if resp.status_code in (200, 202):
        return True
    logger.error(f"Rows overwrite cells failed: {resp.status_code} - {resp.text}")
    return False

def _rows_create_table(page_id: str, name: str) -> Optional[Dict[str, Any]]:
    resp = _rows_post_json(
        f"/spreadsheets/{ROWS_SPREADSHEET_ID}/pages/{page_id}/tables",
        {"name": name},
        accept_json=True,
    )
    if resp.status_code in (200, 201):
        try:
            return resp.json()
        except Exception:
            return None
    logger.error(f"Rows create table failed: {resp.status_code} - {resp.text}")
    return None

def sync_to_rows(data: List[Dict], table_id: str, mode: str, mapper) -> bool:
    """Sync data to Rows.com (append or best-effort overwrite)."""
    if not ROWS_API_KEY or not ROWS_SPREADSHEET_ID:
        logger.info("Rows.com credentials not configured, skipping sync")
        return False
    
    try:
        rows_values = [mapper(item) for item in data]
        logger.info(f"Syncing {len(rows_values)} records to Rows.com (table={table_id}, mode={mode})...")

        if mode == "overwrite":
            cleared = _rows_clear_table(table_id)
            if not cleared:
                logger.warning("Rows overwrite requested but clear endpoint unavailable; falling back to append")
            else:
                logger.info("Rows table cleared; proceeding with append of full dataset")

        ok = _rows_append_values(table_id, rows_values)
        if ok:
            logger.info(f"Successfully synced {len(rows_values)} rows to Rows.com")
        return ok
            
    except Exception as e:
        logger.error(f"Rows.com sync error: {e}")
        return False

def extract_status_data(driver: webdriver.Chrome) -> List[Dict[str, Any]]:
    """
    Extract data from /reservation_c2/status (daily operational view).
    Since OTELMS UI can vary, we use robust heuristics:
    - Collect links/texts containing booking numbers like "#7504"
    - Extract booking_id, room, guest (best-effort), and raw text
    """
    logger.info("Loading status page...")
    driver.get(OTELMS_STATUS_URL)

    if "login" in (driver.current_url or "").lower():
        logger.warning("Status URL redirected to login; re-authenticating...")
        login_to_otelms(driver)
        driver.get(OTELMS_STATUS_URL)

    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
    # Let dynamic widgets settle
    time.sleep(1.0)

    items = driver.execute_script(
        r"""
        function clean(s){ return (s||'').replace(/\s+/g,' ').trim(); }
        function findColumnTitle(el){
          // Walk up and try to find a nearby header element
          let cur = el;
          for (let i=0;i<6 && cur;i++){
            const header = cur.querySelector && cur.querySelector('h1,h2,h3,h4,.title,.panel-title');
            if (header && clean(header.textContent)) return clean(header.textContent);
            cur = cur.parentElement;
          }
          return '';
        }
        const out = [];
        const candidates = Array.from(document.querySelectorAll('a,div,span,li'));
        const seen = new Set();
        for (const el of candidates){
          const t = clean(el.textContent);
          if (!t) continue;
          const m = t.match(/#(\d{3,})/);
          if (!m) continue;
          const booking_id = m[1];
          const key = booking_id + '|' + t;
          if (seen.has(key)) continue;
          seen.add(key);
          out.push({
            booking_id,
            text: t,
            href: el.getAttribute && (el.getAttribute('href') || ''),
            column: findColumnTitle(el)
          });
        }
        return out;
        """
    ) or []

    # Normalize output
    rows: List[Dict[str, Any]] = []
    for it in items:
        try:
            booking_id = str(it.get("booking_id") or "").strip()
            if not booking_id:
                continue
            text = str(it.get("text") or "")
            # Attempt to extract room like "C 1256" or "A 1806"
            room = ""
            m_room = re.search(r"\b([A-Z])\s*([0-9]{3,4})\b", text)
            if m_room:
                room = f"{m_room.group(1)} {m_room.group(2)}"
            rows.append({
                "booking_id": booking_id,
                "room": room,
                "column": str(it.get("column") or ""),
                "href": str(it.get("href") or ""),
                "text": text,
                "extracted_at": datetime.utcnow().isoformat() + "Z",
            })
        except Exception:
            continue

    logger.info(f"Extracted {len(rows)} status items")
    return rows

def _set_rlist_date_range(driver: webdriver.Chrome, start_date: str, end_date: str) -> None:
    """
    Attempt to set the rlist date range filter.
    UI shows a single date-range input like "YYYY-MM-DD - YYYY-MM-DD".
    We try common patterns:
    - Any input containing " - " and matching YYYY-MM-DD.
    - Two separate date inputs (start/end).
    """
    driver.execute_script(
        """
        const start = arguments[0];
        const end = arguments[1];
        const rangeValue = `${start} - ${end}`;

        const inputs = Array.from(document.querySelectorAll('input'));
        // Prefer a single range input that already contains " - "
        const range = inputs.find(i => (i.value || '').includes(' - ') && (i.value || '').match(/\\d{4}-\\d{2}-\\d{2}/));
        if (range) {
          // If daterangepicker is used, set via its API so hidden fields update.
          try {
            if (window.jQuery) {
              const $r = window.jQuery(range);
              const drp = $r.data('daterangepicker');
              if (drp && drp.setStartDate && drp.setEndDate) {
                drp.setStartDate(start);
                drp.setEndDate(end);
                // Trigger apply
                $r.trigger('apply.daterangepicker', drp);
              }
            }
          } catch(e) {}
          range.focus();
          range.value = rangeValue;
          range.dispatchEvent(new Event('input', {bubbles:true}));
          range.dispatchEvent(new Event('change', {bubbles:true}));
          return true;
        }

        // Fallback: find two date inputs
        const dateInputs = inputs.filter(i => (i.type || '').toLowerCase() === 'text' && (i.value || '').match(/^\\d{4}-\\d{2}-\\d{2}$/));
        if (dateInputs.length >= 2) {
          dateInputs[0].focus();
          dateInputs[0].value = start;
          dateInputs[0].dispatchEvent(new Event('input', {bubbles:true}));
          dateInputs[0].dispatchEvent(new Event('change', {bubbles:true}));
          dateInputs[1].focus();
          dateInputs[1].value = end;
          dateInputs[1].dispatchEvent(new Event('input', {bubbles:true}));
          dateInputs[1].dispatchEvent(new Event('change', {bubbles:true}));
          return true;
        }
        return false;
        """,
        start_date,
        end_date,
    )

def _set_rlist_sort(driver: webdriver.Chrome, sort_mode: str) -> None:
    """
    Sort modes requested:
      - created: "შექმნის თარიღი"
      - checkin: "შესვლის თარიღი"
      - checkout: "გასვლის თარიღი"
    We select by visible text on any <select>.
    """
    # Match the exact dropdown labels seen in OTELMS UI.
    # Requested:
    # - ანგარიშგება შემოსვლის თარიღით
    # - ანგარიშგება შექმნის თარიღის მიხედვით
    # - ანგარიშგება განთავსების დღეების მიხედვით
    sort_text = {
        "created": "შექმნის თარიღი",
        "checkin": "შემოსვლის თარიღი",
        "stay_days": "განთავსების დღეები",
    }.get(sort_mode, "შექმნის თარიღი")

    driver.execute_script(
        """
        const target = arguments[0];
        const selects = Array.from(document.querySelectorAll('select'));
        for (const sel of selects) {
          const opt = Array.from(sel.options || []).find(o => (o.textContent||'').trim() === target);
          if (!opt) continue;
          sel.value = opt.value;
          sel.dispatchEvent(new Event('change', {bubbles:true}));
          return true;
        }
        return false;
        """,
        sort_text,
    )

def _set_rlist_status(driver: webdriver.Chrome, status_text: str = "ყველა") -> bool:
    """
    Ensure rlist "სტატუსი" filter is set to "ყველა" (All), unless overridden.
    We select by visible text on any <select>.
    """
    status_text = (status_text or "").strip() or "ყველა"
    return bool(driver.execute_script(
        """
        const target = arguments[0];
        const selects = Array.from(document.querySelectorAll('select'));
        for (const sel of selects) {
          const opt = Array.from(sel.options || []).find(o => (o.textContent||'').trim() === target);
          if (!opt) continue;
          sel.value = opt.value;
          sel.dispatchEvent(new Event('change', {bubbles:true}));
          return true;
        }
        return false;
        """,
        status_text,
    ))

def _set_rlist_categories(driver: webdriver.Chrome, category_names: List[str]) -> bool:
    """
    Enforce that only the provided categories are selected in the rlist "კატეგორია" multi-select.
    Best-effort strategy:
    - Prefer a <select multiple> that contains the category option labels.
    - Fallback to toggling checkbox inputs whose nearby text matches.
    """
    if not category_names:
        return False
    selected = driver.execute_script(
        """
        const wanted = new Set((arguments[0] || []).map(s => String(s).trim().toLowerCase()).filter(Boolean));
        if (wanted.size === 0) return {ok:false, selected:0, method:"none"};

        function clean(s){ return (s||'').replace(/\\s+/g,' ').trim(); }

        // Strategy 1: <select multiple> with matching option texts
        const selects = Array.from(document.querySelectorAll('select[multiple]'));
        for (const sel of selects) {
          const opts = Array.from(sel.options || []);
          if (opts.length < 10) continue; // likely not categories
          // Must contain at least one wanted label
          const hasAny = opts.some(o => wanted.has(clean(o.textContent).toLowerCase()));
          if (!hasAny) continue;
          let count = 0;
          for (const o of opts) {
            const t = clean(o.textContent).toLowerCase();
            const should = wanted.has(t);
            o.selected = should;
            if (should) count++;
          }
          sel.dispatchEvent(new Event('change', {bubbles:true}));
          return {ok:true, selected:count, method:"select"};
        }

        // Strategy 2: checkbox list (often rendered by multiselect plugins)
        const boxes = Array.from(document.querySelectorAll('input[type="checkbox"]'));
        let selectedCount = 0;
        for (const cb of boxes) {
          // try to find human-readable label near checkbox
          const labelText = clean((cb.closest('label')?.textContent) || (cb.parentElement?.textContent) || '');
          if (!labelText) continue;
          const key = labelText.toLowerCase();
          const should = wanted.has(key);
          if (cb.checked !== should) {
            cb.checked = should;
            cb.dispatchEvent(new Event('change', {bubbles:true}));
          }
          if (should) selectedCount++;
        }
        return {ok:true, selected:selectedCount, method:"checkbox"};
        """,
        category_names,
    )
    try:
        logger.info(f"Rlist categories applied: method={selected.get('method')}, selected={selected.get('selected')}")
    except Exception:
        pass
    return bool(selected and selected.get("ok"))

def _click_rlist_search(driver: webdriver.Chrome) -> None:
    """Click the search button on rlist (best-effort)."""
    driver.execute_script(
        """
        const candidates = Array.from(document.querySelectorAll('button,input[type="submit"]'));
        const btn = candidates.find(b => ((b.textContent||'') + ' ' + (b.value||'')).toLowerCase().includes('ძიებ') || ((b.textContent||'') + ' ' + (b.value||'')).toLowerCase().includes('search'));
        if (btn) { btn.click(); return true; }
        // Fallback: click any primary button
        const primary = candidates.find(b => (b.className||'').toLowerCase().includes('btn') && (b.className||'').toLowerCase().includes('primary'));
        if (primary) { primary.click(); return true; }
        return false;
        """
    )

def extract_rlist_data(driver: webdriver.Chrome, start_date: str, end_date: str, sort_mode: str) -> List[Dict[str, Any]]:
    """Extract reporting list rows from /reservation_c2/rlist/1 for a date range and sort mode."""
    logger.info(f"Loading rlist page (sort={sort_mode})...")
    driver.get(OTELMS_RLIST_URL)

    if "login" in (driver.current_url or "").lower():
        logger.warning("Rlist URL redirected to login; re-authenticating...")
        login_to_otelms(driver)
        driver.get(OTELMS_RLIST_URL)

    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
    time.sleep(1.0)

    _set_rlist_date_range(driver, start_date=start_date, end_date=end_date)
    active_categories = _env_str_list("RLIST_ACTIVE_CATEGORIES", DEFAULT_RLIST_ACTIVE_CATEGORIES)
    _set_rlist_categories(driver, active_categories)
    _set_rlist_status(driver, _env_str_list("RLIST_STATUS", ["ყველა"])[0])
    _set_rlist_sort(driver, sort_mode=sort_mode)
    _click_rlist_search(driver)

    # Wait for table to be present
    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
    time.sleep(1.0)

    payload = driver.execute_script(
        """
        function clean(s){ return (s||'').replace(/\\s+/g,' ').trim(); }
        const table = document.querySelector('table');
        if (!table) return {headers: [], rows: []};
        const thead = table.querySelector('thead');
        const tbody = table.querySelector('tbody');
        const headerCells = Array.from((thead ? thead.querySelectorAll('th') : table.querySelectorAll('tr th')) || []);
        const headers = headerCells.map(th => clean(th.textContent));
        const bodyRows = Array.from((tbody ? tbody.querySelectorAll('tr') : table.querySelectorAll('tbody tr')) || []);
        const rows = bodyRows.map(tr => Array.from(tr.querySelectorAll('td')).map(td => clean(td.textContent)));
        return {headers, rows};
        """
    ) or {"headers": [], "rows": []}

    headers: List[str] = payload.get("headers") or []
    rows_raw: List[List[str]] = payload.get("rows") or []

    # Fallback: if headers missing, use expected schema by column positions from screenshot.
    # Columns shown: #, room, guest, source, checkin, nights, checkout, amount, paid, balance, created_at
    if not headers or len(headers) < 8:
        headers = ["#", "room", "guest", "source", "check_in", "nights", "check_out", "amount", "paid", "balance", "created_at"]

    out: List[Dict[str, Any]] = []
    for r in rows_raw:
        if not r or all(not c for c in r):
            continue
        # Map by position to stable keys (ignore localized header variations)
        def get(i: int) -> str:
            return r[i].strip() if i < len(r) else ""
        out.append({
            "room": get(1),
            "guest": get(2),
            "source": get(3),
            "check_in": get(4),
            "nights": get(5),
            "check_out": get(6),
            "amount": get(7),
            "paid": get(8),
            "balance": get(9),
            "created_at": get(10),
            "sort_mode": sort_mode,
            "range_start": start_date,
            "range_end": end_date,
            "extracted_at": datetime.utcnow().isoformat() + "Z",
        })

    logger.info(f"Extracted {len(out)} rlist rows (sort={sort_mode})")
    return out

@app.route('/', methods=['GET', 'POST'])
@app.route('/scrape', methods=['GET', 'POST'])
def scrape():
    """Calendar scraping endpoint (backwards compatible)."""
    driver = None
    start_time = time.time()
    
    try:
        logger.info(f"=== OTELMS Calendar Scraper {SCRAPER_VERSION} Started ===")
        
        # Setup browser
        driver = setup_driver()
        logger.info("Chrome driver initialized")
        
        # Login with retry
        login_func = retry_on_failure(lambda: login_to_otelms(driver))
        login_func()
        
        # Extract data with retry
        extract_func = retry_on_failure(lambda: extract_calendar_data(driver))
        extract_result = extract_func()
        calendar_data = extract_result.get("rows", [])
        views_scanned = extract_result.get("views_scanned", [])
        scan_config = extract_result.get("config", {})
        
        if not calendar_data:
            return jsonify({
                'status': 'warning',
                'message': 'No data extracted (calendar may be empty)',
                'data_points': 0,
                'views_scanned': views_scanned,
                'scan_config': scan_config,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }), 200
        
        # Save to GCS snapshot
        filename = save_json_to_gcs(calendar_data, GCS_BUCKET, prefix="otelms_calendar")

        # Change detection + history (calendar)
        state_blob = "state/latest_calendar.json"
        prev = _gcs_read_json(GCS_BUCKET, state_blob) or []
        events = _diff_rows(
            prev=prev,
            cur=calendar_data,
            key_fields=["booking_id"],
            track_fields=["guest_name", "source", "balance", "status", "date_in", "date_out", "phone", "responsible"],
        )
        _append_history(events, source="calendar", snapshot_file=filename)
        _gcs_write_json(GCS_BUCKET, state_blob, calendar_data)

        # Sync to Rows.com (calendar) unless unchanged
        rows_synced = False
        if (not SKIP_ROWS_IF_UNCHANGED) or events:
            rows_synced = sync_to_rows(
                calendar_data,
                table_id=ROWS_CALENDAR_TABLE_ID,
                mode=ROWS_SYNC_MODE,
                mapper=lambda item: [
                    item.get('booking_id', ''),
                    item.get('guest_name', ''),
                    item.get('source', ''),
                    item.get('balance', ''),
                    item.get('status', ''),
                    item.get('resid', ''),
                    item.get('date_in', ''),
                    item.get('date_out', ''),
                    item.get('phone', ''),
                    item.get('responsible', ''),
                    item.get('extracted_at', ''),
                ],
            )
        
        elapsed = time.time() - start_time
        
        logger.info(f"=== SUCCESS in {elapsed:.2f}s ===")
        
        return jsonify({
            'status': 'success',
            'message': f'Extracted {len(calendar_data)} booking records',
            'filename': filename,
            'rows_synced': rows_synced,
            'data_points': len(calendar_data),
            'views_scanned': views_scanned,
            'scan_config': scan_config,
            'elapsed_seconds': round(elapsed, 2),
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }), 200
        
    except Exception as e:
        elapsed = time.time() - start_time
        error_msg = str(e)
        
        logger.error(f"ERROR after {elapsed:.2f}s: {error_msg}", exc_info=True)
        
        return jsonify({
            'status': 'error',
            'message': error_msg,
            'elapsed_seconds': round(elapsed, 2),
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }), 500
        
    finally:
        if driver:
            try:
                driver.quit()
                logger.info("Browser closed")
            except Exception as e:
                logger.error(f"Error closing browser: {e}")

@app.route('/scrape/status', methods=['GET', 'POST'])
def scrape_status():
    """Status scraping endpoint (daily operational view)."""
    driver = None
    start_time = time.time()

    try:
        logger.info(f"=== OTELMS Status Scraper {SCRAPER_VERSION} Started ===")
        driver = setup_driver()
        logger.info("Chrome driver initialized")

        login_func = retry_on_failure(lambda: login_to_otelms(driver))
        login_func()

        status_func = retry_on_failure(lambda: extract_status_data(driver))
        status_data = status_func()

        filename = save_json_to_gcs(status_data, GCS_BUCKET, prefix="otelms_status")

        # Change detection + history (status)
        state_blob = "state/latest_status.json"
        prev = _gcs_read_json(GCS_BUCKET, state_blob) or []
        # status entries can repeat by booking_id; use composite key
        events = _diff_rows(
            prev=prev,
            cur=status_data,
            key_fields=["booking_id", "room", "column"],
            track_fields=["href", "text"],
        )
        _append_history(events, source="status", snapshot_file=filename)
        _gcs_write_json(GCS_BUCKET, state_blob, status_data)

        rows_synced = False
        if (not SKIP_ROWS_IF_UNCHANGED) or events:
            rows_synced = sync_to_rows(
                status_data,
                table_id=ROWS_STATUS_TABLE_ID,
                mode=ROWS_SYNC_MODE,
                mapper=lambda item: [
                    item.get('booking_id', ''),
                    item.get('room', ''),
                    item.get('column', ''),
                    item.get('href', ''),
                    item.get('text', ''),
                    item.get('extracted_at', ''),
                ],
            )

        elapsed = time.time() - start_time
        return jsonify({
            "status": "success",
            "message": f"Extracted {len(status_data)} status items",
            "filename": filename,
            "rows_synced": rows_synced,
            "data_points": len(status_data),
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }), 200
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"ERROR after {elapsed:.2f}s: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": str(e),
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }), 500
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

@app.route('/scrape/all', methods=['GET', 'POST'])
def scrape_all():
    """Run both calendar and status scrapes in one call."""
    start_time = time.time()
    driver = None

    try:
        logger.info(f"=== OTELMS Full Scrape {SCRAPER_VERSION} Started ===")
        driver = setup_driver()
        login_to_otelms(driver)

        calendar_result = extract_calendar_data(driver)
        calendar_rows = calendar_result.get("rows", [])
        calendar_file = save_json_to_gcs(calendar_rows, GCS_BUCKET, prefix="otelms_calendar")

        status_rows = extract_status_data(driver)
        status_file = save_json_to_gcs(status_rows, GCS_BUCKET, prefix="otelms_status")

        elapsed = time.time() - start_time
        return jsonify({
            "status": "success",
            "calendar": {"data_points": len(calendar_rows), "filename": calendar_file},
            "status_page": {"data_points": len(status_rows), "filename": status_file},
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }), 200
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"ERROR after {elapsed:.2f}s: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": str(e),
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }), 500
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

@app.route('/scrape/rlist/dec2025', methods=['GET', 'POST'])
def scrape_rlist_dec2025():
    """Extract Dec 2025 rlist in three sorts into three tables."""
    driver = None
    start_time = time.time()

    try:
        driver = setup_driver()
        login_to_otelms(driver)

        start_date = "2025-12-01"
        end_date = "2025-12-31"

        results: Dict[str, Any] = {"status": "success", "range": {"start": start_date, "end": end_date}, "runs": {}}
        for mode, table_id in (
            ("created", ROWS_RLIST_CREATED_TABLE_ID),
            ("checkin", ROWS_RLIST_CHECKIN_TABLE_ID),
            ("stay_days", ROWS_RLIST_CHECKOUT_TABLE_ID),
        ):
            data = extract_rlist_data(driver, start_date=start_date, end_date=end_date, sort_mode=mode)
            gcs_file = save_json_to_gcs(data, GCS_BUCKET, prefix=f"otelms_rlist_{mode}")
            rows_ok = False
            if table_id:
                rows_ok = sync_to_rows(
                    data,
                    table_id=table_id,
                    mode=ROWS_SYNC_MODE,
                    mapper=lambda item: [
                        item.get("room", ""),
                        item.get("guest", ""),
                        item.get("source", ""),
                        item.get("check_in", ""),
                        item.get("nights", ""),
                        item.get("check_out", ""),
                        item.get("amount", ""),
                        item.get("paid", ""),
                        item.get("balance", ""),
                        item.get("created_at", ""),
                        item.get("range_start", ""),
                        item.get("range_end", ""),
                        item.get("extracted_at", ""),
                    ],
                )
            results["runs"][mode] = {"data_points": len(data), "filename": gcs_file, "rows_synced": rows_ok, "rows_table_id_set": bool(table_id)}

        elapsed = time.time() - start_time
        results["elapsed_seconds"] = round(elapsed, 2)
        results["timestamp"] = datetime.utcnow().isoformat() + "Z"
        return jsonify(results), 200
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"ERROR after {elapsed:.2f}s: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": str(e),
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }), 500
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

@app.route('/scrape/rlist', methods=['POST'])
def scrape_rlist_custom():
    """Custom rlist scrape: JSON body {start_date,end_date,sort_mode,table_id(optional)}."""
    driver = None
    start_time = time.time()
    try:
        payload = request.get_json(silent=True) or {}
        start_date = str(payload.get("start_date") or "").strip()
        end_date = str(payload.get("end_date") or "").strip()
        sort_mode = str(payload.get("sort_mode") or "created").strip()
        table_id = str(payload.get("table_id") or "").strip()
        if not start_date or not end_date:
            return jsonify({"status": "error", "message": "start_date and end_date are required (YYYY-MM-DD)"}), 400

        driver = setup_driver()
        login_to_otelms(driver)

        data = extract_rlist_data(driver, start_date=start_date, end_date=end_date, sort_mode=sort_mode)
        gcs_file = save_json_to_gcs(data, GCS_BUCKET, prefix=f"otelms_rlist_{sort_mode}")

        rows_ok = False
        if table_id:
            rows_ok = sync_to_rows(
                data,
                table_id=table_id,
                mode=ROWS_SYNC_MODE,
                mapper=lambda item: [
                    item.get("room", ""),
                    item.get("guest", ""),
                    item.get("source", ""),
                    item.get("check_in", ""),
                    item.get("nights", ""),
                    item.get("check_out", ""),
                    item.get("amount", ""),
                    item.get("paid", ""),
                    item.get("balance", ""),
                    item.get("created_at", ""),
                    item.get("range_start", ""),
                    item.get("range_end", ""),
                    item.get("extracted_at", ""),
                ],
            )

        elapsed = time.time() - start_time
        return jsonify({
            "status": "success",
            "message": f"Extracted {len(data)} rlist rows ({sort_mode})",
            "filename": gcs_file,
            "rows_synced": rows_ok,
            "data_points": len(data),
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }), 200
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"ERROR after {elapsed:.2f}s: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": str(e),
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }), 500
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

@app.route('/rows/bootstrap/rlist', methods=['POST'])
def rows_bootstrap_rlist():
    """
    Create three Rows tables (created/checkin/checkout) for rlist ingestion, and write header rows.

    Default mapping:
      created   -> Page1
      checkin   -> Page2
      stay_days -> Page3

    Body (optional):
      {
        "page_names": {"created":"Page1","checkin":"Page2","stay_days":"Page3"},
        "table_names": {"created":"OTELMS RList (Created date)","checkin":"OTELMS RList (Check-in date)","stay_days":"OTELMS RList (Stay days)"}
      }
    """
    if not ROWS_API_KEY or not ROWS_SPREADSHEET_ID:
        return jsonify({"status": "error", "message": "ROWS_API_KEY and ROWS_SPREADSHEET_ID must be configured"}), 400

    payload = request.get_json(silent=True) or {}
    page_names = payload.get("page_names") or {"created": "Page1", "checkin": "Page2", "stay_days": "Page3"}
    table_names = payload.get("table_names") or {
        "created": "OTELMS RList (Created date)",
        "checkin": "OTELMS RList (Check-in date)",
        "stay_days": "OTELMS RList (Stay days)",
    }

    # Header schema for rlist (stable order)
    header = [
        "room", "guest", "source", "check_in", "nights", "check_out",
        "amount", "paid", "balance", "created_at",
        "range_start", "range_end", "extracted_at",
    ]
    header_range = f"A1:{_a1_col(len(header))}1"

    # Fetch spreadsheet info to map page name -> page_id
    resp = _rows_get(f"/spreadsheets/{ROWS_SPREADSHEET_ID}")
    if resp.status_code != 200:
        return jsonify({"status": "error", "message": f"Rows spreadsheet fetch failed: {resp.status_code} - {resp.text}"}), 500

    ss = resp.json()
    pages = ss.get("pages") or []
    page_name_to_id = {str(p.get("name")): str(p.get("id")) for p in pages if p.get("id") and p.get("name")}

    created: Dict[str, Any] = {}
    for key in ("created", "checkin", "stay_days"):
        pn = str(page_names.get(key) or "").strip()
        if not pn or pn not in page_name_to_id:
            created[key] = {"ok": False, "error": f"Page '{pn}' not found in spreadsheet"}
            continue

        page_id = page_name_to_id[pn]
        tname = str(table_names.get(key) or f"OTELMS RList ({key})")

        # Idempotent: reuse existing table with same name on the page if present
        existing_table_id = ""
        for p in pages:
            if str(p.get("id")) != page_id:
                continue
            for tt in (p.get("tables") or []):
                if str(tt.get("name")) == tname and tt.get("id"):
                    existing_table_id = str(tt.get("id"))
                    break
        if existing_table_id:
            table_id = existing_table_id
        else:
            t = _rows_create_table(page_id, tname)
            if not t or not t.get("id"):
                created[key] = {"ok": False, "error": "Failed to create table"}
                continue
            table_id = str(t.get("id"))

        ok_header = _rows_overwrite_cells(table_id, header_range, header)
        created[key] = {
            "ok": True,
            "page_name": pn,
            "page_id": page_id,
            "table_name": tname,
            "table_id": table_id,
            "header_written": ok_header,
        }

    # Provide env var hints
    env_hints = {
        "ROWS_RLIST_CREATED_TABLE_ID": created.get("created", {}).get("table_id", ""),
        "ROWS_RLIST_CHECKIN_TABLE_ID": created.get("checkin", {}).get("table_id", ""),
        "ROWS_RLIST_CHECKOUT_TABLE_ID": created.get("stay_days", {}).get("table_id", ""),
    }

    return jsonify({
        "status": "success",
        "created": created,
        "env_hints": env_hints,
        "note": "Add the env_hints values to Cloud Run env.yaml and redeploy.",
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }), 200

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        "version": SCRAPER_VERSION,
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
