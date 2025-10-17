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

class AmazonBSRCrawler:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.db_conn = None
        self.xpaths = {}
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

    def load_xpaths(self):
        """Load XPath selectors for BSR page from database"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT data_field, xpath, css_selector
                FROM xpath_selectors
                WHERE mall_name = 'Amazon' AND page_type = 'bsr_page' AND is_active = TRUE
            """)

            for row in cursor.fetchall():
                self.xpaths[row[0]] = {
                    'xpath': row[1],
                    'css': row[2]
                }

            cursor.close()
            print(f"[OK] Loaded {len(self.xpaths)} XPath selectors for BSR page")

            if len(self.xpaths) == 0:
                print("[WARNING] No XPath selectors found for BSR page!")
                print("Please add XPath selectors to xpath_selectors table with page_type='bsr_page'")
                return False

            return True

        except Exception as e:
            print(f"[ERROR] Failed to load XPaths: {e}")
            return False

    def load_page_urls(self):
        """Load BSR page URLs from database"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT page_number, url
                FROM bsr_page_urls
                WHERE is_active = TRUE
                ORDER BY page_number
            """)

            urls = cursor.fetchall()
            cursor.close()
            print(f"[OK] Loaded {len(urls)} BSR page URLs")
            return urls

        except Exception as e:
            print(f"[ERROR] Failed to load BSR page URLs: {e}")
            return []

    def setup_driver(self):
        """Setup Chrome WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)

        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            '''
        })

        print("[OK] WebDriver setup complete")

    def scroll_to_load_all(self):
        """Scroll down to load all 50 items on BSR page"""
        try:
            print("[INFO] Scrolling to load all items...")

            # Get initial height
            last_height = self.driver.execute_script("return document.body.scrollHeight")

            # Scroll down in steps
            for i in range(3):  # Try scrolling 3 times
                # Scroll to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)  # Wait for content to load

                # Calculate new height
                new_height = self.driver.execute_script("return document.body.scrollHeight")

                if new_height == last_height:
                    break  # No more content to load

                last_height = new_height

            # Scroll back to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

            print("[OK] Scrolling completed")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to scroll: {e}")
            return False

    def extract_text_safe(self, element, xpath):
        """Safely extract text from element using xpath"""
        try:
            result = element.xpath(xpath)
            if result:
                # Handle attribute extraction (e.g., @href)
                if isinstance(result[0], str):
                    return result[0].strip()
                # Handle element extraction
                else:
                    return result[0].text_content().strip()
            return None
        except Exception as e:
            return None

    def scrape_page(self, url, page_number):
        """Scrape a single BSR page"""
        try:
            print(f"\n[PAGE {page_number}] Accessing: {url[:80]}...")
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))

            # Scroll to load all items (up to 50)
            self.scroll_to_load_all()

            # Get page source and parse with lxml
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Find all BSR product items
            # You'll provide the XPath, using placeholder for now
            rank_xpath = self.xpaths.get('rank', {}).get('xpath', '')
            product_name_xpath = self.xpaths.get('product_name', {}).get('xpath', '')

            if not rank_xpath or not product_name_xpath:
                print("[ERROR] Required XPaths not found (rank, product_name)")
                return False

            print(f"[INFO] Using Rank XPath: {rank_xpath}")
            print(f"[INFO] Using Product Name XPath: {product_name_xpath}")

            # Extract rank and product name pairs
            # This will depend on the actual HTML structure
            # For now, creating a placeholder structure

            collected_count = 0

            # TODO: Implement actual extraction logic based on XPaths
            # This is a placeholder - needs to be adjusted based on actual HTML
            print(f"[INFO] XPath extraction logic will be implemented based on provided XPaths")

            print(f"[PAGE {page_number}] Collected {collected_count} products (Total: {self.total_collected})")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to scrape page {page_number}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def save_to_db(self, rank, product_name):
        """Save BSR data to database"""
        try:
            cursor = self.db_conn.cursor()

            cursor.execute("""
                INSERT INTO amazon_tv_bsr
                (Rank, Retailer_SKU_Name)
                VALUES (%s, %s)
            """, (rank, product_name))

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
            print("Amazon TV BSR (Best Sellers Rank) Crawler")
            print("="*80)

            # Connect to database
            if not self.connect_db():
                return

            # Load XPaths
            if not self.load_xpaths():
                print("[ERROR] Please add XPath selectors first!")
                return

            # Load page URLs
            page_urls = self.load_page_urls()
            if not page_urls:
                print("[ERROR] No BSR page URLs found")
                return

            # Setup WebDriver
            self.setup_driver()

            # Scrape each page
            for page_number, url in page_urls:
                if not self.scrape_page(url, page_number):
                    print(f"[WARNING] Failed to scrape page {page_number}, continuing...")

                # Random delay between pages
                time.sleep(random.uniform(2, 4))

            print("\n" + "="*80)
            print(f"BSR Crawling completed! Total collected: {self.total_collected} items")
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
        crawler = AmazonBSRCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler terminated. Exiting...")
