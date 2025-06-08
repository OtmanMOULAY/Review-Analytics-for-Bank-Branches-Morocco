from datetime import datetime, timedelta

import time
import csv
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import urllib.parse

def extract_reviews_from_url(url, output_file):

    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless")  # Run Chrome in headless mode
    driver = webdriver.Chrome(options=options)
    reviews = []
    seen_review_ids = set()
    location_name = "Unknown Location"
    location_address = "Unknown Address"
    try:
        print(f"Opening URL: {url}")
        driver.get(url)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[@role='region']"))
        )
    
        try:
           
            location_selectors = [
                "//h1[@data-attrid='title']",  # Main title
                "//h1[contains(@class, 'DUwDvf')]",  # Alternative class
                "//div[@data-attrid='title']//span",  # Title in span
                "//h1[@class='x3AX1-LfntMc-header-title-title']",  # Another possible selector
                "//div[contains(@class, 'lMbq3e')]//h1",  # Header with h1
                "//span[@class='DUwDvf lfPIob']"  # Specific span class
            ]
            
            for selector in location_selectors:
                try:
                    location_element = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, selector))
                    )
                    location_name = location_element.text.strip()
                    if location_name and location_name != "":
                        print(f"Found location name: {location_name}")
                        break
                except:
                    continue
                    
            # If still not found, try extracting from URL
            if location_name == "Unknown Location":
                parsed_url = urllib.parse.urlparse(url)
                path_parts = parsed_url.path.split('/')
                for part in path_parts:
                    if part and '@' not in part and 'maps' not in part.lower():
                        # Decode URL encoding
                        decoded_part = urllib.parse.unquote(part)
                        if len(decoded_part) > 3:  # Reasonable length for a location name
                            location_name = decoded_part.replace('+', ' ')
                            print(f"Extracted location from URL: {location_name}")
                            break
                            
        except Exception as e:
            print(f"Could not extract location name: {e}")
        
        # Extract location address from the page
        try:
            # Try multiple selectors for address
            address_selectors = [
                "//div[@data-item-id='address']//span[@class='LrzXr']",  # Address span
                "//div[@data-item-id='address']//span",  # Any span in address div
                "//button[@data-item-id='address']//div[contains(@class, 'fontBodyMedium')]",  # Address button
                "//button[@data-item-id='address']//span",  # Span in address button
                "//div[contains(@class, 'Io6YTe fontBodyMedium')]",  # Address with specific classes
                "//span[contains(text(), ',') and contains(text(), ' ')]",  # Generic address pattern
                "//div[@role='region']//span[contains(text(), ',')]",  # Address with comma in region
                "//button[contains(@aria-label, 'Address')]//span",  # Address button with aria-label
                "//div[@jsaction and contains(text(), ',')]"  # Div with jsaction containing comma
            ]
            
            for selector in address_selectors:
                try:
                    address_elements = driver.find_elements(By.XPATH, selector)
                    for element in address_elements:
                        potential_address = element.text.strip()
                        # Check if this looks like an address (contains comma and reasonable length)
                        if (potential_address and 
                            ',' in potential_address and 
                            len(potential_address) > 10 and 
                            len(potential_address) < 200 and
                            not potential_address.lower().startswith('http') and
                            not 'review' in potential_address.lower()):
                            location_address = potential_address
                            print(f"Found location address: {location_address}")
                            break
                    if location_address != "Unknown Address":
                        break
                except:
                    continue
            
            # Alternative method: Look for address patterns in all text elements
            if location_address == "Unknown Address":
                try:
                    all_text_elements = driver.find_elements(By.XPATH, "//span | //div")
                    for element in all_text_elements[:50]:  # Limit search to avoid performance issues
                        try:
                            text = element.text.strip()
                            # Pattern matching for address-like text
                            if (text and 
                                ',' in text and 
                                len(text) > 15 and 
                                len(text) < 150 and
                                (' St' in text or ' Ave' in text or ' Rd' in text or ' Blvd' in text or
                                 'Street' in text or 'Avenue' in text or 'Road' in text or 'Boulevard' in text or
                                 any(char.isdigit() for char in text))):  # Contains numbers typical in addresses
                                location_address = text
                                print(f"Found address using pattern matching: {location_address}")
                                break
                        except:
                            continue
                except Exception as e:
                    print(f"Error in alternative address search: {e}")
                    
        except Exception as e:
            print(f"Could not extract location address: {e}")
        
        # Try to click "All reviews" button if available
        try:
            all_reviews_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label, 'Avis')]"))
            )
            driver.execute_script("arguments[0].click();", all_reviews_button)
            print("Clicked on 'All reviews' button")
            time.sleep(2)
        except Exception as e:
            print(f"Note: 'All reviews' button not found or not clickable: {e}")
        
        # Find the reviews container
        try:
            review_container = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='region' and contains(@style, 'overflow')]"))
            )
            print("Found reviews container")
        except Exception as e:
            print(f"Could not find reviews container, trying alternative xpath: {e}")
            try:
                review_container = driver.find_element(By.XPATH, "//div[contains(@class, 'DxyBCb')]")
                print("Found reviews container using alternative method")
            except Exception as e2:
                print(f"Failed to find review container: {e2}")
                return []

        # Try to get total review count
        max_reviews = 1000  # Default limit
        try:
            review_count_text = driver.find_element(By.XPATH, "//span[contains(@aria-label, 'reviews') or contains(@aria-label, 'avis')]").get_attribute("aria-label")
            count_digit = ''.join(filter(str.isdigit, review_count_text))
            if count_digit:
                max_reviews = int(count_digit)
                print(f"Total reviews to extract: approximately {max_reviews}")
        except Exception as e:
            print(f"Could not determine total review count: {e}")
            
        # Main scroll loop parameters
        no_change_count = 0
        max_no_change = 5  # Stop after 5 scroll attempts with no new content
        last_review_count = 0
        
        # Main scroll loop
        while no_change_count < max_no_change and len(seen_review_ids) < max_reviews:
            # Expand "More" buttons
            try:
                more_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Plus') or contains(text(), 'More')]")
                for btn in more_buttons[:10]:  # Limit to avoid too many clicks
                    try:
                        driver.execute_script("arguments[0].click();", btn)
                        time.sleep(0.1)
                    except:
                        pass
            except:
                pass
                
            # Extract visible reviews
            review_elements = driver.find_elements(By.XPATH, "//div[@data-review-id]")
            for review_element in review_elements:
                try:
                    review_id = review_element.get_attribute("data-review-id")
                    if review_id and review_id not in seen_review_ids:
                        try:
                            author = review_element.find_element(By.XPATH, ".//div[contains(@class, 'd4r55')]").text
                        except:
                            author = "Unknown"
                            
                        try:
                            rating_element = review_element.find_element(By.XPATH, ".//span[contains(@class, 'kvMYJc')]")
                            rating = rating_element.get_attribute("aria-label")
                        except:
                            rating = "Unknown"
                            
                        try:
                            date = review_element.find_element(By.XPATH, ".//span[contains(@class, 'rsqaWe')]").text
                        except:
                            date = "Unknown"
                            
                        try:
                            text = review_element.find_element(By.XPATH, ".//span[contains(@class, 'wiI7pd')]").text
                        except:
                            text = ""
                            
                        # Include location name and address in each review row
                        reviews.append([location_name, location_address, author, rating, date, text])
                        seen_review_ids.add(review_id)
                        if len(seen_review_ids) % 10 == 0:
                            print(f"Extracted {len(seen_review_ids)} reviews so far...")
                except Exception as e:
                    print(f"Error extracting review: {e}")
                    continue
            
            # Perform scroll using multiple methods
            try:
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", review_container)
                actions = ActionChains(driver)
                actions.move_to_element(review_container)
                actions.send_keys(Keys.PAGE_DOWN)
                actions.perform()
                review_container.send_keys(Keys.END)
                time.sleep(1.5)
            except Exception as e:
                print(f"Error during scrolling: {e}")
            
            # Check if we've loaded new reviews
            current_review_count = len(seen_review_ids)
            if current_review_count == last_review_count:
                no_change_count += 1
                print(f"No new reviews loaded. Attempt {no_change_count}/{max_no_change}")
            else:
                no_change_count = 0
                print(f"Now have {current_review_count} reviews, continuing to scroll...")
                
            last_review_count = current_review_count
            
            # Safety limit to prevent infinite loops
            if len(seen_review_ids) >= 1000:
                print("Reached maximum review extraction limit.")
                break
                
        print(f"Total reviews extracted from {url} ({location_name} - {location_address}): {len(reviews)}")
        
        # Write reviews to CSV file with location and address columns
        with open(output_file, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["bank", "Address", "Author", "Rating", "Date", "Review"])
            for review in reviews:
                writer.writerow(review)
                
        print(f"Reviews saved to {output_file}")
        return len(reviews)
    
    finally:
        driver.quit()

def get_url_list(**kwargs):
    """Return a list of Google Maps URLs to scrape from a CSV file."""
    csv_path = kwargs.get('csv_path', 'dags/url_list.csv')  # use forward slashes
  # default to 'urls.csv' if not provided
    df = pd.read_csv(csv_path)

    # Ensure the 'url' column exists
    if 'url' not in df.columns:
        raise ValueError("The CSV file must contain a column named 'url'.")

    # Drop NaN and return list of URLs
    url_list = df['url'].dropna().tolist()
    return [url_list]

def process_url(url, **kwargs):
    """Process a single Google Maps URL."""
    # Create output directory if it doesn't exist
    output_dir = os.path.join(os.getcwd(), 'google_maps_data')
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename based on URL
    url_part = url.split('/')[-1]
    output_file = os.path.join(output_dir, f"reviews_{url_part}_{datetime.now().strftime('%Y%m%d')}.csv")
    
    # Extract reviews
    review_count = extract_reviews_from_url(url, output_file)
    
    return {
        'url': url,
        'output_file': output_file,
        'review_count': review_count
    }

def combine_csv_files(**kwargs):
    """Combine all CSV files from this run into a single file."""
    ti = kwargs['ti']
    
    # Get all task IDs for the URL processing tasks
    task_ids = kwargs.get('task_ids', [])
    
    # Create a single output CSV file
    output_dir = os.path.join(os.getcwd(), 'google_maps_data')
    output_file = os.path.join(output_dir, f"all_reviews_{datetime.now().strftime('%Y%m%d')}.csv")
    
    all_reviews = []
    total_count = 0
    
    # Process each CSV file
    with open(output_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["bank", "Address", "Author", "Rating", "Date", "Review"])
        
        # Pull results from each task
        for task_id in task_ids:
            result = ti.xcom_pull(task_ids=task_id)
            if result:
                url = result.get('url')
                csv_file = result.get('output_file')
                review_count = result.get('review_count', 0)
                
                # Read this CSV and add to combined file
                if os.path.exists(csv_file):
                    with open(csv_file, 'r', encoding='utf-8') as infile:
                        reader = csv.reader(infile)
                        next(reader, None)  # Skip header
                        for row in reader:
                            # Add source URL as the last column
                            writer.writerow(row + [url])
                            total_count += 1
    
    print(f"Combined {total_count} reviews into {output_file}")
    ti = kwargs['ti']
    ti.xcom_push(key='combined_csv', value=output_file)  # Push the file path
    return output_file