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

class AmazonTVCrawler:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        self.max_skus = 300

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
        """Load XPath selectors from database"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT data_field, xpath, css_selector
                FROM xpath_selectors
                WHERE mall_name = 'Amazon' AND page_type = 'main_page' AND is_active = TRUE
            """)

            for row in cursor.fetchall():
                self.xpaths[row[0]] = {
                    'xpath': row[1],
                    'css': row[2]
                }

            cursor.close()
            print(f"[OK] Loaded {len(self.xpaths)} XPath selectors")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to load XPaths: {e}")
            return False

    def load_page_urls(self):
        """Load page URLs from database"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT page_number, url
                FROM page_urls
                WHERE mall_name = 'Amazon' AND is_active = TRUE
                ORDER BY page_number
            """)

            urls = cursor.fetchall()
            cursor.close()
            print(f"[OK] Loaded {len(urls)} page URLs")
            return urls

        except Exception as e:
            print(f"[ERROR] Failed to load page URLs: {e}")
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

    def extract_text_safe(self, element, xpath):
        """Safely extract text from element using xpath"""
        try:
            result = element.xpath(xpath)
            if result:
                return result[0].strip() if isinstance(result[0], str) else result[0].text_content().strip()
            return None
        except:
            return None

    def scrape_page(self, url, page_number):
        """Scrape a single page"""
        try:
            print(f"\n[PAGE {page_number}] Accessing: {url[:80]}...")
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))

            # Get page source and parse with lxml
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Find all product containers (excluding ads/widgets)
            base_xpath = self.xpaths['base_container']['xpath']
            products = tree.xpath(base_xpath)

            print(f"[INFO] Found {len(products)} total containers")

            # Filter out excluded containers
            valid_products = []
            for product in products:
                # Check if it's a valid product (not ad/widget)
                cel_widget = product.get('cel_widget_id', '')
                component_type = product.get('data-component-type', '')

                # Exclude conditions
                if any([
                    'loom-desktop' in cel_widget,
                    'messaging' in component_type.lower(),
                    'video' in component_type.lower(),
                    'sb-themed' in cel_widget,
                    'multi-brand' in cel_widget
                ]):
                    continue

                valid_products.append(product)

            print(f"[INFO] Valid products after filtering: {len(valid_products)}")

            # Process up to 16 products per page
            collected_count = 0
            for idx, product in enumerate(valid_products[:16], 1):
                if self.total_collected >= self.max_skus:
                    print(f"[INFO] Reached maximum SKU limit ({self.max_skus})")
                    return False

                # Extract data
                data = {
                    'mall_name': 'Amazon',
                    'page_number': page_number,
                    'product_name': self.extract_text_safe(product, self.xpaths['product_name']['xpath']),
                    'purchase_history': self.extract_text_safe(product, self.xpaths['purchase_history']['xpath']),
                    'final_price': self.extract_text_safe(product, self.xpaths['final_price']['xpath']),
                    'original_price': self.extract_text_safe(product, self.xpaths['original_price']['xpath']),
                    'shipping_info': self.extract_text_safe(product, self.xpaths['shipping_info']['xpath']),
                    'stock_availability': self.extract_text_safe(product, self.xpaths['stock_availability']['xpath']),
                    'deal_badge': self.extract_text_safe(product, self.xpaths['deal_badge']['xpath']),
                    'product_url': self.extract_text_safe(product, self.xpaths['product_url']['xpath'])
                }

                # Save to database
                if self.save_to_db(data):
                    collected_count += 1
                    self.total_collected += 1
                    print(f"  [{idx}/16] Collected: {data['product_name'][:50]}...")

            print(f"[PAGE {page_number}] Collected {collected_count} products (Total: {self.total_collected}/{self.max_skus})")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to scrape page {page_number}: {e}")
            return True  # Continue to next page

    def save_to_db(self, data):
        """Save collected data to database"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                INSERT INTO collected_data
                (mall_name, page_number, retailer_sku_name, product_url,
                 final_sku_price, savings, comparable_pricing, offer, star_rating)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (mall_name, sku) DO NOTHING
            """, (
                data['mall_name'],
                data['page_number'],
                data['product_name'],
                data['product_url'],
                data['final_price'],
                data['original_price'],
                None,  # comparable_pricing
                data['deal_badge'],
                None   # star_rating
            ))

            self.db_conn.commit()
            cursor.close()
            return True

        except Exception as e:
            print(f"[ERROR] Failed to save to DB: {e}")
            return False

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print("Amazon TV Crawler - Starting")
            print("="*80)

            # Connect to database
            if not self.connect_db():
                return

            # Load XPaths and URLs
            if not self.load_xpaths():
                return

            page_urls = self.load_page_urls()
            if not page_urls:
                print("[ERROR] No page URLs found")
                return

            # Setup WebDriver
            self.setup_driver()

            # Scrape each page
            for page_number, url in page_urls:
                if self.total_collected >= self.max_skus:
                    break

                if not self.scrape_page(url, page_number):
                    break

                # Random delay between pages
                time.sleep(random.uniform(2, 4))

            print("\n" + "="*80)
            print(f"Crawling completed! Total collected: {self.total_collected} SKUs")
            print("="*80)

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")

        finally:
            if self.driver:
                self.driver.quit()
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    crawler = AmazonTVCrawler()
    crawler.run()
