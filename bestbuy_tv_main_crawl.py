import time
import random
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from lxml import html

# Database configuration
DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

class BestBuyTVCrawler:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.db_conn = None
        self.total_collected = 0
        self.error_messages = []

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            print("[OK] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False

    def setup_driver(self):
        """Setup Chrome WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--lang=en-US,en;q=0.9')

        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False
        }
        chrome_options.add_experimental_option("prefs", prefs)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.driver.set_page_load_timeout(60)
        self.wait = WebDriverWait(self.driver, 20)

        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });
                window.chrome = {
                    runtime: {}
                };
            '''
        })

        print("[OK] WebDriver setup complete")

    def load_page_urls(self):
        """Load page URLs from database"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT page_number, url
                FROM bby_page_url
                WHERE is_active = TRUE
                ORDER BY page_number
            """)

            urls = cursor.fetchall()
            cursor.close()
            print(f"[OK] Loaded {len(urls)} page URLs")
            return urls

        except Exception as e:
            print(f"[ERROR] Failed to load page URLs: {e}")
            return []

    def extract_text_safe(self, element, xpath):
        """Safely extract text from element using xpath"""
        try:
            result = element.xpath(xpath)
            if result:
                if isinstance(result[0], str):
                    return result[0].strip()
                else:
                    return result[0].text_content().strip()
            return None
        except Exception as e:
            return None

    def scrape_page(self, url, page_number):
        """Scrape a single Best Buy page"""
        try:
            print(f"\n[PAGE {page_number}] Accessing: {url[:80]}...")
            self.driver.get(url)

            print("[INFO] Waiting for page to load...")
            time.sleep(random.uniform(5, 8))

            # Wait for product list to load
            try:
                self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "product-list-item")))
                print("[OK] Product list loaded")
            except Exception as e:
                print(f"[WARNING] Product list not found: {e}")

            # Scroll down to load all products (lazy loading)
            print("[INFO] Scrolling to load all products...")
            for i in range(3):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                print(f"[DEBUG] Scroll {i+1}/3 completed")

            # Scroll back to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

            # Wait for skeleton loaders to disappear and real content to load
            print("[INFO] Waiting for content to fully load...")
            time.sleep(5)

            # Wait until skeleton shimmer disappears
            for attempt in range(10):
                page_source_check = self.driver.page_source
                if 'a-skeleton-shimmer' not in page_source_check:
                    print("[OK] Skeleton loaders gone, content loaded")
                    break
                print(f"[DEBUG] Waiting for skeleton to disappear (attempt {attempt+1}/10)...")
                time.sleep(2)

            # Additional wait for dynamic content
            time.sleep(3)

            # Get page source and parse with lxml
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Find all product containers
            # Base container: li with class "product-list-item product-list-item-gridView"
            containers = tree.xpath('//li[contains(@class, "product-list-item") and contains(@class, "product-list-item-gridView")]')
            print(f"[INFO] Found {len(containers)} product containers")

            collected_count = 0

            # Save HTML for debugging if first page
            if page_number == 1:
                with open(f'bestbuy_page_{page_number}_debug.html', 'w', encoding='utf-8') as f:
                    f.write(page_source)
                print(f"[DEBUG] Saved page source to bestbuy_page_{page_number}_debug.html")

            for idx, container in enumerate(containers, 1):
                try:
                    # Extract product name (Retailer_SKU_Name)
                    # Try multiple possible XPaths
                    product_name_elem = container.xpath('.//h2[contains(@class, "product-title")]')
                    if not product_name_elem:
                        product_name_elem = container.xpath('.//a[@class="product-list-item-link"]//h2')
                    if not product_name_elem:
                        product_name_elem = container.xpath('.//div[@class="sku-block-content-title"]//h2')

                    product_name = product_name_elem[0].text_content().strip() if product_name_elem else None

                    if not product_name:
                        # Save container HTML for debugging
                        if idx <= 3 and page_number == 1:
                            container_html = html.tostring(container, encoding='unicode', pretty_print=True)
                            with open(f'bestbuy_container_{idx}_debug.html', 'w', encoding='utf-8') as f:
                                f.write(container_html)
                            print(f"  [DEBUG] Saved container {idx} to bestbuy_container_{idx}_debug.html")
                        print(f"  [SKIP {idx}] No product name found")
                        continue

                    # Extract product URL
                    product_url_elem = container.xpath('.//a[@class="product-list-item-link"]/@href')
                    product_url = f"https://www.bestbuy.com{product_url_elem[0]}" if product_url_elem else None

                    # Extract Final_SKU_Price
                    price_elem = container.xpath('.//span[@data-testid="price-block-customer-price"]//span')
                    final_price = price_elem[0].text_content().strip() if price_elem else None

                    # Extract Savings
                    savings_elem = container.xpath('.//span[@data-testid="price-block-total-savings-text"]')
                    savings = savings_elem[0].text_content().strip() if savings_elem else None

                    # Extract Comparable_Pricing
                    comp_price_elem = container.xpath('.//span[@data-testid="price-block-regular-price-message-text"]//span')
                    comp_pricing = comp_price_elem[1].text_content().strip() if len(comp_price_elem) > 1 else None

                    # Extract Offer (+ X offers)
                    offer_elem = container.xpath('.//div[@data-testid="plus-x-offers"]//span[@class="font-sans text-default text-style-body-md-400"]')
                    offer = offer_elem[0].text_content().strip() if offer_elem else None

                    # Extract Pick-Up Availability
                    pickup_elem = container.xpath('.//div[@class="fulfillment"]//p[contains(., "Pick up")]')
                    pickup = pickup_elem[0].text_content().strip() if pickup_elem else None

                    # Extract Shipping Availability
                    shipping_elem = container.xpath('.//div[@class="fulfillment"]//p[contains(., "Get it") or contains(., "FREE")]')
                    shipping = shipping_elem[0].text_content().strip() if shipping_elem else None

                    # Extract Delivery Availability
                    delivery_elem = container.xpath('.//div[@class="fulfillment"]//p[contains(., "Delivery") or contains(., "Installation")]')
                    delivery = delivery_elem[0].text_content().strip() if delivery_elem else None

                    # Extract Star_Rating
                    rating_elem = container.xpath('.//span[@aria-hidden="true" and contains(@class, "font-weight-bold")]')
                    star_rating = rating_elem[0].text_content().strip() if rating_elem else None

                    # Extract SKU_Status (check for "Sponsored", "New!", etc.)
                    status_elem = container.xpath('.//div[@class="sponsored"]')
                    sku_status = "Sponsored" if status_elem else "Regular"

                    # Save to database
                    if self.save_to_db(
                        page_type='main',
                        product_name=product_name,
                        final_price=final_price,
                        savings=savings,
                        comp_pricing=comp_pricing,
                        offer=offer,
                        pickup=pickup,
                        shipping=shipping,
                        delivery=delivery,
                        star_rating=star_rating,
                        sku_status=sku_status,
                        product_url=product_url
                    ):
                        collected_count += 1
                        self.total_collected += 1
                        print(f"  [{idx}/{len(containers)}] {product_name[:60]}... | Price: {final_price}")

                except Exception as e:
                    print(f"  [ERROR {idx}] Failed to extract data: {e}")
                    continue

            print(f"[PAGE {page_number}] Collected {collected_count} products (Total: {self.total_collected})")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to scrape page {page_number}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def save_to_db(self, page_type, product_name, final_price, savings, comp_pricing,
                   offer, pickup, shipping, delivery, star_rating, sku_status, product_url):
        """Save product data to database"""
        try:
            cursor = self.db_conn.cursor()

            cursor.execute("""
                INSERT INTO bestbuy_tv_main_crawl
                (page_type, Retailer_SKU_Name, Final_SKU_Price, Savings, Comparable_Pricing,
                 Offer, Pick_Up_Availability, Shipping_Availability, Delivery_Availability,
                 Star_Rating, SKU_Status, Product_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (page_type, product_name, final_price, savings, comp_pricing,
                  offer, pickup, shipping, delivery, star_rating, sku_status, product_url))

            self.db_conn.commit()
            cursor.close()

            return True

        except Exception as e:
            print(f"[ERROR] Failed to save to DB: {e}")
            self.error_messages.append(f"DB save error: {e}")
            return False

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print("Best Buy TV Main Page Crawler")
            print("="*80)

            # Connect to database
            if not self.connect_db():
                return

            # Load page URLs
            page_urls = self.load_page_urls()
            if not page_urls:
                print("[ERROR] No page URLs found")
                return

            # Setup WebDriver
            self.setup_driver()

            # Scrape each page
            for page_number, url in page_urls:
                if not self.scrape_page(url, page_number):
                    print(f"[WARNING] Failed to scrape page {page_number}, continuing...")

                # Random delay between pages
                time.sleep(random.uniform(5, 8))

            print("\n" + "="*80)
            print(f"Best Buy Crawling completed! Total collected: {self.total_collected} products")
            print("="*80)

            if self.error_messages:
                print("\nErrors encountered:")
                for error in self.error_messages:
                    print(f"  - {error}")

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    try:
        crawler = BestBuyTVCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler terminated. Exiting...")
