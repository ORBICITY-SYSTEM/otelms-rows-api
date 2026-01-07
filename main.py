"""
OTELMS Calendar Scraper v11.5 FINAL - Fixed Timeout + Lazy Loading
==========================================================================
Bulletproof scraper with correct HTML structure parsing
"""

import os
import sys
import json
import time
import logging
import requests
from datetime import datetime
from typing import List, Dict, Optional, Any
from flask import Flask, jsonify
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
SCRAPER_VERSION = "v11.6"

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
GCS_BUCKET = os.environ['GCS_BUCKET']
ROWS_API_KEY = os.environ.get('ROWS_API_KEY', '' )
ROWS_SPREADSHEET_ID = os.environ.get('ROWS_SPREADSHEET_ID', '')
ROWS_TABLE_ID = os.environ.get('ROWS_TABLE_ID', 'Table1')

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
        "jQueryActive": _safe_execute(driver, "return (window.jQuery && window.jQuery.active) || null", None),
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

def ensure_calendar_rendered(driver: webdriver.Chrome, timeout_seconds: int = 120) -> int:
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

    stable_hits = 0
    last_count = -1
    start = time.time()

    while time.time() - start < timeout_seconds:
        # Actively trigger scroll handlers / lazy rendering during the wait.
        try:
            _kick_calendar_render(driver)
        except Exception:
            pass

        count = _safe_execute(driver, "return document.querySelectorAll('div.calendar_item[resid]').length", 0)
        ajax_active = _safe_execute(driver, "return (window.jQuery && window.jQuery.active) || 0", 0)

        # We want "some bookings" and "count not changing" for a couple cycles.
        if isinstance(count, int) and count > 0:
            if count == last_count:
                stable_hits += 1
            else:
                stable_hits = 0
            last_count = count
            # If jQuery is present but background XHR keeps happening, don't block forever.
            if stable_hits >= 2 and (ajax_active == 0 or (time.time() - start) > 10):
                return count

        time.sleep(1.5)

    raise TimeoutException(f"Timed out after {timeout_seconds}s waiting for calendar bookings to render")

def extract_calendar_data(driver: webdriver.Chrome) -> List[Dict]:
    """Extract calendar data with correct HTML structure parsing"""
    logger.info("Loading calendar page...")
    driver.get(OTELMS_CALENDAR_URL)
    
    try:
        # If we got redirected back to login, re-auth and reload calendar.
        if "login" in (driver.current_url or "").lower():
            logger.warning("Calendar URL redirected to login; re-authenticating...")
            login_to_otelms(driver)
            driver.get(OTELMS_CALENDAR_URL)

        # Wait for actual rendered bookings (not just page load).
        render_timeout = int(os.environ.get("CALENDAR_RENDER_TIMEOUT", "120"))
        logger.info(f"Waiting for calendar bookings to render (timeout={render_timeout}s)...")
        rendered_count = ensure_calendar_rendered(driver, timeout_seconds=render_timeout)
        logger.info(f"Calendar bookings rendered: {rendered_count} elements with resid")

        save_debug_artifacts(driver, 'calendar_rendered', extra=collect_calendar_diagnostics(driver))
        
        # Extract booking blocks
        data_rows = []
        seen_bookings = set()

        # Extract via JS to avoid stale element reference issues.
        raw_items = driver.execute_script(
            """
            return Array.from(document.querySelectorAll('div.calendar_item[resid]')).map(el => {
              const resid = el.getAttribute('resid');
              const status = el.getAttribute('status') || '';
              const element_id = el.getAttribute('id') || '';
              const booking_nam = (el.querySelector('.calendar_booking_nam')?.textContent || '').trim();
              const booking_info = (el.querySelector('.calendar_booking_info')?.textContent || '').trim();
              const balance = (el.querySelector('.balance_negative span, .balance_positive span')?.textContent || '').trim();
              return {resid, status, element_id, booking_nam, booking_info, balance};
            });
            """
        ) or []

        logger.info(f"Found {len(raw_items)} booking blocks (JS extraction)")

        for item in raw_items:
            try:
                resid = (item.get("resid") or "").strip()
                if not resid or resid in seen_bookings:
                    continue
                seen_bookings.add(resid)

                status = (item.get("status") or "").strip()
                element_id = (item.get("element_id") or "").strip()
                booking_nam = (item.get("booking_nam") or "").strip()
                booking_info = (item.get("booking_info") or "").strip().rstrip(",")
                balance = (item.get("balance") or "").strip()

                booking_id = None
                guest_name = None

                # Parse calendar_booking_nam: "B:7296,  ჯაბა პაშკოვსკი, "
                if "B:" in booking_nam:
                    parts = booking_nam.split("B:", 1)[1].split(",")
                    if len(parts) >= 2:
                        booking_id = parts[0].strip()
                        guest_name = parts[1].strip()

                if booking_id or guest_name:
                    data_rows.append({
                        "resid": resid,
                        "booking_id": booking_id or resid,  # Fallback to resid
                        "guest_name": guest_name or "",
                        "source": booking_info,
                        "balance": balance,
                        "status": status,
                        "element_id": element_id,
                        "extracted_at": datetime.utcnow().isoformat() + "Z",
                    })
                else:
                    logger.warning(f"Skipping resid {resid}: no booking_id or guest_name found")
            except Exception as e:
                logger.error(f"Error parsing booking item: {e}")
                continue
        
        logger.info(f"Extracted {len(data_rows)} unique booking records")
        return data_rows
        
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

def sync_to_rows(data: List[Dict]) -> bool:
    """Sync data to Rows.com with proper append logic and rate limit handling"""
    if not ROWS_API_KEY or not ROWS_SPREADSHEET_ID:
        logger.info("Rows.com credentials not configured, skipping sync")
        return False
    
    try:
        logger.info(f"Syncing {len(data)} records to Rows.com...")
        
        # Rows.com API endpoint for appending
        url = f"https://api.rows.com/v1/spreadsheets/{ROWS_SPREADSHEET_ID}/tables/{ROWS_TABLE_ID}/values:append"
        
        headers = {
            "Authorization": f"Bearer {ROWS_API_KEY}",
            "Content-Type": "application/json"
        }
        
        # Prepare rows
        rows_data = []
        for item in data:
            rows_data.append([
                item.get('booking_id', '' ),
                item.get('guest_name', ''),
                item.get('source', ''),
                item.get('balance', ''),
                item.get('status', ''),
                item.get('resid', ''),
                item.get('extracted_at', '')
            ])
        
        payload = {"values": rows_data}
        
        # Retry with exponential backoff for rate limits
        for attempt in range(3):
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code in [200, 201]:
                logger.info(f"Successfully synced {len(rows_data)} rows to Rows.com")
                return True
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limited, retrying after {retry_after}s...")
                time.sleep(retry_after)
            else:
                logger.error(f"Rows.com sync failed: {response.status_code} - {response.text}")
                return False
        
        return False
            
    except Exception as e:
        logger.error(f"Rows.com sync error: {e}")
        return False

@app.route('/', methods=['GET', 'POST'])
@app.route('/scrape', methods=['GET', 'POST'])
def scrape():
    """Main scraping endpoint with comprehensive error handling"""
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
        calendar_data = extract_func()
        
        if not calendar_data:
            return jsonify({
                'status': 'warning',
                'message': 'No data extracted (calendar may be empty)',
                'data_points': 0,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }), 200
        
        # Save to GCS
        filename = save_to_gcs(calendar_data, GCS_BUCKET)
        
        # Sync to Rows.com
        rows_synced = sync_to_rows(calendar_data)
        
        elapsed = time.time() - start_time
        
        logger.info(f"=== SUCCESS in {elapsed:.2f}s ===")
        
        return jsonify({
            'status': 'success',
            'message': f'Extracted {len(calendar_data)} booking records',
            'filename': filename,
            'rows_synced': rows_synced,
            'data_points': len(calendar_data),
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
