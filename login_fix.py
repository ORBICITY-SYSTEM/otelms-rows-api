# Replace login function in main.py

def login_to_otelms(driver):
    """Login to OTELMS"""
    print("Logging into OTELMS...")
    driver.get(OTELMS_LOGIN_URL)
    
    wait = WebDriverWait(driver, 10)
    username_field = wait.until(EC.presence_of_element_located((By.ID, "userLogin")))
    password_field = driver.find_element(By.ID, "password")
    
    username_field.clear()
    username_field.send_keys(OTELMS_USERNAME)
    password_field.clear()
    password_field.send_keys(OTELMS_PASSWORD)
    
    # Try multiple selectors for submit button
    try:
        submit_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
    except:
        try:
            submit_button = driver.find_element(By.CSS_SELECTOR, "input[type='submit']")
        except:
            submit_button = driver.find_element(By.XPATH, "//button[contains(text(), 'შესვლა') or contains(text(), 'Login')]")
    
    submit_button.click()
    
    wait.until(EC.url_changes(OTELMS_LOGIN_URL))
    print("Login successful!")
