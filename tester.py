import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

CANDIDATE_PROFILES = ["Profile 75", "Profile 2", "Default"]  # order of preference

def is_present(d, by, sel):
    try:
        d.find_element(by, sel)
        return True
    except NoSuchElementException:
        return False

def wait_until_logged_in(driver, timeout=60):
    """
    Considered 'logged in' when the real composer textarea exists and is enabled,
    and there is no visible 'Log in' button.
    """
    start = time.time()
    while time.time() - start < timeout:
        # real composer?
        try:
            box = driver.find_element(By.CSS_SELECTOR, 'textarea[data-testid="textbox"]')
            ready = box.is_displayed() and box.is_enabled()
        except Exception:
            ready = False

        # login button present?
        login_btn = is_present(driver, By.XPATH, '//button[contains(.,"Log in")]') or \
                    is_present(driver, By.XPATH, '//a[contains(.,"Log in")]')

        if ready and not login_btn:
            time.sleep(0.5)
            return True

        time.sleep(0.5)
    raise TimeoutException("Login not detected within timeout.")

def make_driver_for_profile(profile_dir_name: str):
    chrome_opts = Options()

    # Path to your Chrome user data root
    user_data_dir = os.path.join(
        os.environ.get("LOCALAPPDATA", r"C:\Users\shatc\AppData\Local"),
        r"Google\Chrome\User Data"
    )

    chrome_opts.add_argument(f"--user-data-dir={user_data_dir}")
    chrome_opts.add_argument(f"--profile-directory={profile_dir_name}")

    # stability flags
    chrome_opts.add_argument("--disable-extensions")
    chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
    chrome_opts.add_argument("--disable-features=BlockThirdPartyCookies")
    chrome_opts.add_argument("--start-maximized")
    chrome_opts.add_argument("--lang=en-US")

    # IMPORTANT: Do NOT use headless for the first login
    # chrome_opts.add_argument("--headless=new")

    d = webdriver.Chrome(options=chrome_opts)
    d.set_page_load_timeout(120)
    return d

def find_logged_in_profile():
    for prof in CANDIDATE_PROFILES:
        print(f"→ Trying profile: {prof}")
        d = None
        try:
            d = make_driver_for_profile(prof)
            d.get("https://chatgpt.com/")
            try:
                wait_until_logged_in(d, timeout=20)
                print(f"✓ Logged in with profile: {prof}")
                return prof
            except Exception as e:
                print(f"  Not logged in for {prof}: {e}")
        except Exception as e:
            print(f"  Failed to open {prof}: {e}")
        finally:
            try:
                if d:
                    d.quit()
            except Exception:
                pass
    return None

if __name__ == "__main__":
    prof = find_logged_in_profile()
    print("Logged-in profile found:", prof or "None")

    if not prof:
        print("\nNext step:")
        print(r'1) Manually open Chrome with a candidate profile, e.g.:')
        print(r'   start chrome --user-data-dir="C:\Users\shatc\AppData\Local\Google\Chrome\User Data" --profile-directory="Profile 75"')
        print("2) Visit https://chatgpt.com/ and log in fully (see the chat box).")
        print("3) Close that window, then run this tester again.")
