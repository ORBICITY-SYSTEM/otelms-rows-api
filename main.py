"""
OTELMS Calendar Scraper v11.4 FINAL - Fixed CSS Selector for Accurate Parsing
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
from typing import List, Dict, Optional
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
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    chrome_options.binary_location = "/opt/chrome/chrome"
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def save_debug_artifacts(driver: webdriver.Chrome, name: str) -> Optional[str]:
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

def extract_calendar_data(driver: webdriver.Chrome) -> List[Dict]:
    """Extract calendar data with correct HTML structure parsing"""
    logger.info("Loading calendar page...")
    driver.get(OTELMS_CALENDAR_URL)
    
    wait = WebDriverWait(driver, 20)
    
    try:
        # Wait for calendar to load
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.calendar_item')))
        logger.info("Calendar loaded successfully")
        
        # Wait for dynamic content
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
        time.sleep(2)
        
        save_debug_artifacts(driver, 'calendar_loaded')
        
        # Extract booking blocks
        data_rows = []
        seen_bookings = set()
        
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, 'div.calendar_item[resid]')
                logger.info(f"Found {len(elements)} booking blocks (attempt {attempt + 1})")
                
                for idx, element in enumerate(elements):
                    try:
                        # Re-fetch to avoid stale reference
                        elements = driver.find_elements(By.CSS_SELECTOR, 'div.calendar_item[resid]')
                        if idx >= len(elements):
                            break
                        element = elements[idx]
                        
                        # Extract attributes
                        resid = element.get_attribute('resid')
                        status = element.get_attribute('status')
                        element_id = element.get_attribute('id')
                        
                        if not resid:
                            continue
                        
                        # Duplicate detection
                        if resid in seen_bookings:
                            continue
                        seen_bookings.add(resid)
                        
                        # Extract nested elements
                        booking_id = None
                        guest_name = None
                        source = None
                        balance = None
                        
                        # Parse calendar_booking_nam: "B:7296,  ჯაბა პაშკოვსკი, "
                        try:
                            booking_nam = element.find_element(By.CLASS_NAME, 'calendar_booking_nam').text.strip()
                            if 'B:' in booking_nam:
                                # Split by "B:" and then by comma
                                parts = booking_nam.split('B:')[1].split(',')
                                if len(parts) >= 2:
                                    booking_id = parts[0].strip()
                                    guest_name = parts[1].strip()
                        except NoSuchElementException:
                            logger.debug(f"calendar_booking_nam not found for resid {resid}")
                        except Exception as e:
                            logger.debug(f"Error parsing booking_nam for resid {resid}: {e}")
                        
                        # Parse calendar_booking_info: "whatsapp 577250205, " or "პირდაპირი გაყიდვა, "
                        try:
                            source = element.find_element(By.CLASS_NAME, 'calendar_booking_info').text.strip().rstrip(',')
                        except NoSuchElementException:
                            logger.debug(f"calendar_booking_info not found for resid {resid}")
                        
                        # Parse balance: .balance_negative or .balance_positive span
                        try:
                            balance_elem = element.find_element(By.CSS_SELECTOR, '.balance_negative span, .balance_positive span')
                            balance = balance_elem.text.strip()
                        except NoSuchElementException:
                            logger.debug(f"balance not found for resid {resid}")
                        
                        # Data validation - at least booking_id or guest_name must exist
                        if booking_id or guest_name:
                            data_rows.append({
                                'resid': resid,
                                'booking_id': booking_id or resid,  # Fallback to resid
                                'guest_name': guest_name or '',
                                'source': source or '',
                                'balance': balance or '',
                                'status': status or '',
                                'element_id': element_id or '',
                                'extracted_at': datetime.utcnow().isoformat() + 'Z'
                            })
                            logger.debug(f"Extracted booking: resid={resid}, booking_id={booking_id}, guest={guest_name}")
                        else:
                            logger.warning(f"Skipping resid {resid}: no booking_id or guest_name found")
                        
                    except StaleElementReferenceException:
                        logger.warning(f"Stale element at index {idx}, retrying...")
                        continue
                    except Exception as e:
                        logger.error(f"Error parsing element {idx}: {e}")
                        continue
                
                break  # Success, exit retry loop
                
            except StaleElementReferenceException:
                if attempt == max_attempts - 1:
                    raise
                logger.warning("Stale elements detected, retrying...")
                time.sleep(1)
        
        logger.info(f"Extracted {len(data_rows)} unique booking records")
        return data_rows
        
    except TimeoutException:
        save_debug_artifacts(driver, 'calendar_timeout')
        raise Exception("Calendar page timeout - booking blocks not found")
    except Exception as e:
        save_debug_artifacts(driver, 'calendar_error')
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
        logger.info("=== OTELMS Calendar Scraper v11.4 FINAL Started ===")
        
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
        'version': 'v11.4-final',
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
