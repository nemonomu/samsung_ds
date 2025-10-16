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

class AmazonTVCrawlerUnunique:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        self.max_skus = 300
        self.crawl_success = True
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
                # Handle attribute extraction (e.g., @href)
                if isinstance(result[0], str):
                    return result[0].strip()
                # Handle element extraction
                else:
                    return result[0].text_content().strip()
            return None
        except Exception as e:
            return None

    def extract_product_name(self, element):
        """Extract product name with multiple fallback XPaths"""
        # Try multiple XPath strategies in order of preference
        xpaths_to_try = [
            self.xpaths['product_name']['xpath'],  # Primary: .//h2//span
            './/h2/a/span',                         # Alternative 1: h2 > a > span
            './/a[.//h2]//span',                    # Alternative 2: span in a that has h2
            './/h2',                                # Alternative 3: h2 text content
            './/span[@class="a-size-medium"]',      # Alternative 4: by class
            './/span[@class="a-size-base-plus"]',   # Alternative 5: by class
        ]

        for idx, xpath in enumerate(xpaths_to_try):
            result = self.extract_text_safe(element, xpath)
            if result and len(result.strip()) > 0:
                # Debug: log which XPath worked for non-primary paths
                if idx > 0 and result:
                    pass  # Silently use fallback
                return result

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

            # Filter out excluded containers and sort by page order
            valid_products = []
            excluded_count = 0
            for product in products:
                # Check if it's a valid product (not ad/widget)
                cel_widget = product.get('cel_widget_id', '')
                component_type = product.get('data-component-type', '')
                data_component_id = product.get('data-component-id', '')

                # More specific exclude conditions - only exclude exact matches
                is_excluded = False

                # Exclude sponsored/ad widgets
                if 'loom-desktop' in cel_widget:
                    is_excluded = True
                elif 'sb-themed' in cel_widget:
                    is_excluded = True
                elif 'multi-brand' in cel_widget:
                    is_excluded = True
                # Only exclude messaging/video widgets, not video products
                elif component_type == 's-messaging-widget':
                    is_excluded = True
                elif 'VideoLandscapeCarouselWidget' in data_component_id:
                    is_excluded = True

                if is_excluded:
                    excluded_count += 1
                    continue

                # Get data-index for sorting
                data_index = product.get('data-index', '999')
                try:
                    data_index = int(data_index)
                except:
                    data_index = 999

                valid_products.append((data_index, product))

            if excluded_count > 0:
                print(f"[INFO] Excluded {excluded_count} containers (ads/widgets)")

            # Sort by data-index (page order)
            valid_products.sort(key=lambda x: x[0])
            valid_products = [product for _, product in valid_products]

            print(f"[INFO] Valid products after filtering: {len(valid_products)}")

            # Debug: Show warning if less than 16 products on early pages
            if page_number <= 10 and len(valid_products) < 16:
                print(f"[WARNING] Only {len(valid_products)} valid products found on page {page_number}")
                print(f"[DEBUG] Total containers: {len(products)}, Excluded: {excluded_count}, Valid: {len(valid_products)}")

            # Process up to 16 products per page
            collected_count = 0
            for idx, product in enumerate(valid_products[:16], 1):
                if self.total_collected >= self.max_skus:
                    print(f"[INFO] Reached maximum SKU limit ({self.max_skus})")
                    return False

                # Extract data
                product_url_path = self.extract_text_safe(product, self.xpaths['product_url']['xpath'])

                # DEBUG: Print URL extraction result for first product
                if idx == 1:
                    print(f"\n[DEBUG] URL XPath: {self.xpaths['product_url']['xpath']}")
                    print(f"[DEBUG] Extracted path: {product_url_path}")

                # Build complete URL
                product_url = f"https://www.amazon.com{product_url_path}" if product_url_path else None

                if idx == 1:
                    print(f"[DEBUG] Final URL: {product_url}\n")

                # Extract discount type and validate
                discount_type_raw = self.extract_text_safe(product, self.xpaths['deal_badge']['xpath'])
                # Only keep "Limited time deal", set others to None
                discount_type = discount_type_raw if discount_type_raw == "Limited time deal" else None

                # Extract product name with fallback XPaths
                product_name = self.extract_product_name(product)

                # Skip if no product name (critical field)
                if not product_name:
                    print(f"  [{idx}/16] SKIP: No product name found (tried all XPath alternatives)")
                    continue

                # Get ASIN
                asin = product.get('data-asin', 'NO-ASIN')

                data = {
                    'mall_name': 'Amazon',
                    'page_number': page_number,
                    'Retailer_SKU_Name': product_name,
                    'Number_of_units_purchased_past_month': self.extract_text_safe(product, self.xpaths['purchase_history']['xpath']),
                    'Final_SKU_Price': self.extract_text_safe(product, self.xpaths['final_price']['xpath']),
                    'Original_SKU_Price': self.extract_text_safe(product, self.xpaths['original_price']['xpath']),
                    'Shipping_Info': self.extract_text_safe(product, self.xpaths['shipping_info']['xpath']),
                    'Available_Quantity_for_Purchase': self.extract_text_safe(product, self.xpaths['stock_availability']['xpath']),
                    'Discount_Type': discount_type,
                    'Product_URL': product_url,
                    'ASIN': asin
                }

                # Save to database (always succeeds, no duplicate checking)
                if self.save_to_db(data):
                    collected_count += 1
                    self.total_collected += 1
                    print(f"  [{idx}/16] Collected: {data['Retailer_SKU_Name'][:50] if data['Retailer_SKU_Name'] else '[NO NAME]'}... | ASIN: {asin} | URL: {product_url[:50] if product_url else 'NULL'}...")
                else:
                    print(f"  [{idx}/16] FAILED to save: {data['Retailer_SKU_Name'][:40]}... (ASIN: {asin}) - database error")

            print(f"[PAGE {page_number}] Collected {collected_count} products (Total: {self.total_collected}/{self.max_skus})")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to scrape page {page_number}: {e}")
            return True  # Continue to next page

    def save_to_db(self, data):
        """Save collected data to both ununique tables (NO duplicate checking)"""
        try:
            cursor = self.db_conn.cursor()

            # Save to raw_data_ununique table - NO CONFLICT check, saves everything
            cursor.execute("""
                INSERT INTO raw_data_ununique
                (mall_name, page_number, Retailer_SKU_Name, Number_of_units_purchased_past_month,
                 Final_SKU_Price, Original_SKU_Price, Shipping_Info,
                 Available_Quantity_for_Purchase, Discount_Type, Product_URL, ASIN)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                data['mall_name'],
                data['page_number'],
                data['Retailer_SKU_Name'],
                data['Number_of_units_purchased_past_month'],
                data['Final_SKU_Price'],
                data['Original_SKU_Price'],
                data['Shipping_Info'],
                data['Available_Quantity_for_Purchase'],
                data['Discount_Type'],
                data['Product_URL'],
                data['ASIN']
            ))

            # Get the inserted ID
            raw_data_result = cursor.fetchone()

            # Always insert to Amazon_tv_main_crawled_ununique (since raw_data insert always succeeds)
            if raw_data_result:
                cursor.execute("""
                    INSERT INTO Amazon_tv_main_crawled_ununique
                    (mall_name, Retailer_SKU_Name, Number_of_units_purchased_past_month,
                     Final_SKU_Price, Original_SKU_Price, Shipping_Info,
                     Available_Quantity_for_Purchase, Discount_Type, ASIN)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data['mall_name'],
                    data['Retailer_SKU_Name'],
                    data['Number_of_units_purchased_past_month'],
                    data['Final_SKU_Price'],
                    data['Original_SKU_Price'],
                    data['Shipping_Info'],
                    data['Available_Quantity_for_Purchase'],
                    data['Discount_Type'],
                    data['ASIN']
                ))

            self.db_conn.commit()
            cursor.close()

            # Return True if insert succeeded
            return raw_data_result is not None

        except Exception as e:
            print(f"[ERROR] Failed to save to DB: {e}")
            self.error_messages.append(f"DB save error: {e}")
            return False

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print("Amazon TV Crawler - UNUNIQUE MODE (collects duplicates)")
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

            # Check if crawling was successful
            if self.total_collected < self.max_skus:
                print(f"\n[WARNING] Only collected {self.total_collected}/{self.max_skus} SKUs")
                print(f"Missing: {self.max_skus - self.total_collected} SKUs")
                if self.error_messages:
                    print("\nErrors encountered:")
                    for error in self.error_messages:
                        print(f"  - {error}")

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            if self.error_messages:
                print("\nPrevious errors:")
                for error in self.error_messages:
                    print(f"  - {error}")

        finally:
            if self.driver:
                self.driver.quit()
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    try:
        crawler = AmazonTVCrawlerUnunique()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler terminated. Exiting...")
    # Auto-exit, no input() needed
