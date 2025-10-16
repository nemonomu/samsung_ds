"""
Coolblue 가격 추출 시스템 - DB 기반 버전 (이미지 URL 추출 개선)
DB에서 URL 읽어와서 크롤링 후 결과 저장
파일명 형식: {수집일자}{수집시간}_{국가코드}_{쇼핑몰}.csv
"""
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import paramiko
import time
import random
import re
from datetime import datetime
import pytz
import logging
import os
import json
from io import StringIO

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# DB 설정
DB_CONFIG = {
    'user': 'admin',
    'password': 'samsungcrawl1!',
    'host': 'samsung-ssd-crawl-db.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 3306,
    'database': 'ssd_crawl_db'
}

# 파일서버 설정
FILE_SERVER_CONFIG = {
    'host': '3.36.101.24',
    'port': 22,
    'username': 'ftpuser',
    'password': 'samsung0701!',
    'upload_path': '/home/ftpuser/uploads'
}

class CoolblueScraper:
    def __init__(self):
        self.driver = None
        self.db_engine = None
        self.sftp_client = None
        self.korea_tz = pytz.timezone('Asia/Seoul')  # 이 줄 추가 필요

        # DB 연결 설정
        self.setup_db_connection()
        
        # DB에서 XPath 로드
        self.load_xpaths_from_db()
        
    def setup_db_connection(self):
        """DB 연결 설정"""
        try:
            # SQLAlchemy 엔진 생성
            connection_string = (
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
            self.db_engine = create_engine(connection_string)
            logger.info("✅ DB 연결 설정 완료")
            
        except Exception as e:
            logger.error(f"❌ DB 연결 실패: {e}")
            self.db_engine = None
    
    def load_xpaths_from_db(self):
        """DB에서 Coolblue용 선택자 로드"""
        try:
            query = """
            SELECT element_type, selector_value, priority
            FROM mall_selectors
            WHERE mall_name = 'coolblue' 
              AND country_code = 'nl'
              AND is_active = TRUE
            ORDER BY element_type, priority DESC
            """
            
            df = pd.read_sql(query, self.db_engine)
            
            # element_type별로 그룹화
            self.XPATHS = {}
            for element_type in df['element_type'].unique():
                type_selectors = df[df['element_type'] == element_type]['selector_value'].tolist()
                self.XPATHS[element_type] = type_selectors
            
            logger.info(f"✅ DB에서 선택자 로드 완료: {len(df)}개")
            
            # 기본값 설정 (DB에 없는 경우)
            if not self.XPATHS:
                logger.warning("⚠️ DB에 선택자가 없어 기본값 사용")
                self.XPATHS = {
                    'price': [
                        '/html/body/main/div[3]/div[2]/div[1]/div[2]/div[2]/div/div/form/div[1]/div[1]/div/div/span/span'
                    ],
                    'title': [
                        '/html/body/main/div[3]/div[2]/div[1]/div[2]/div[1]/div/div[1]/h1'
                    ],
                    'imageurl': [
                        '/html/body/main/div[3]/div[2]/div[1]/div[2]/div[1]/div/div[2]/div/div[2]/div/ul/li[1]/div/div/div/div/img'
                        
                        
                    ]
                }
                
        except Exception as e:
            logger.error(f"선택자 로드 실패: {e}")
            # 기본값 사용
            self.XPATHS = {
                'price': [],
                'title': [],
                'imageurl': []
            }
    
    def get_crawl_targets(self, limit=None, include_failed=False):
        """DB에서 크롤링 대상 URL 목록 조회"""
        try:
            if include_failed:
                # 최근 실패한 URL도 포함 (24시간 이내 실패 3회 미만)
                query = """
                WITH failed_counts AS (
                    SELECT url, COUNT(*) as fail_count
                    FROM amazon_crawl_logs
                    WHERE retailprice IS NULL
                      AND crawl_datetime >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                      AND country_code = 'nl'
                    GROUP BY url
                )
                SELECT DISTINCT t.*
                FROM samsung_price_tracking_list t
                LEFT JOIN failed_counts f ON t.url = f.url
                WHERE t.country = 'nl' 
                  AND t.mall_name = 'coolblue'
                  AND t.is_active = TRUE
                  AND (f.fail_count IS NULL OR f.fail_count < 3)
                ORDER BY COALESCE(f.fail_count, 0) DESC  -- 실패한 것 우선
                """
            else:
                query = """
                SELECT *
                FROM samsung_price_tracking_list
                WHERE country = 'nl' 
                  AND mall_name = 'coolblue'
                  AND is_active = TRUE
                """
                
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql(query, self.db_engine)
            logger.info(f"✅ 크롤링 대상 {len(df)}개 조회 완료")
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"크롤링 대상 조회 실패: {e}")
            return []
    
    def setup_driver(self):
        """Chrome 드라이버 설정"""
        logger.info("🔧 Chrome 드라이버 설정 중...")
        
        try:
            self.driver = uc.Chrome()
            self.driver.maximize_window()
            
            # 스텔스 모드 설정
            stealth_script = """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
            """
            self.driver.execute_script(stealth_script)
            
            logger.info("✅ 드라이버 설정 완료")
            return True
        except Exception as e:
            logger.error(f"❌ 드라이버 설정 실패: {e}")
            return False
    
    def initialize_session(self):
        """Coolblue 세션 초기화"""
        logger.info("Coolblue 세션 초기화...")
        
        try:
            # Coolblue 메인 페이지 접속
            self.driver.get("https://www.coolblue.nl")
            time.sleep(2)
            
            # 쿠키 팝업 처리
            self.handle_cookie_consent()
            
            # 세션이 제대로 설정되었는지 확인
            title = self.driver.title
            if "coolblue" in title.lower():
                logger.info("✅ Coolblue 세션 초기화 완료")
                return True
            else:
                logger.warning("⚠️ 세션 초기화 부분 성공")
                return True
                
        except Exception as e:
            logger.error(f"❌ 세션 초기화 실패: {e}")
            return False
    
    def handle_cookie_consent(self):
        """쿠키 동의 팝업 처리"""
        try:
            # DB에서 쿠키 관련 선택자 조회 (있는 경우)
            cookie_selectors = self.XPATHS.get('cookie_accept', [])
            
            # 기본 선택자 추가 (Coolblue 특화)
            default_selectors = [
                "button[name='accept']",  # Oké 버튼의 가능한 선택자
                "button.js-btn-cookie-accept",  # 가능한 클래스명
                "//button[contains(text(), 'Oké')]",  # 텍스트로 찾기
                "//button[contains(text(), 'Oke')]",  # 대체 철자
                "//button[@class='button' and contains(., 'Oké')]",
                "/html/body/div/div[1]/div[2]/div/div[1]/form/div/div[1]/button",
                "button[data-testid='accept-cookies']",
                "//button[contains(text(), 'Accepteren')]",
                "//button[contains(@class, 'accept')]",
                ".cookie-consent__button--accept",
                "#onetrust-accept-btn-handler"
            ]
            
            all_selectors = cookie_selectors + default_selectors
            
            time.sleep(2)
            
            for selector in all_selectors:
                try:
                    if selector.startswith('//'):
                        element = self.driver.find_element(By.XPATH, selector)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if element.is_displayed():
                        element.click()
                        logger.info("🍪 쿠키 동의 팝업 처리 완료")
                        time.sleep(1)
                        return True
                except:
                    continue
            
            # JavaScript로 직접 클릭 시도
            try:
                script = """
                var buttons = document.querySelectorAll('button');
                for (var i = 0; i < buttons.length; i++) {
                    if (buttons[i].textContent.includes('Oké') || 
                        buttons[i].textContent.includes('OK') ||
                        buttons[i].textContent.includes('Accept')) {
                        buttons[i].click();
                        return true;
                    }
                }
                return false;
                """
                result = self.driver.execute_script(script)
                if result:
                    logger.info("🍪 쿠키 동의 팝업 처리 완료 (JavaScript)")
                    time.sleep(1)
                    return True
            except:
                pass
            
            return False
            
        except Exception as e:
            logger.debug(f"쿠키 팝업 처리 중 오류 (무시): {e}")
            return False
    
    def extract_product_info(self, url, row_data, retry_count=0, max_retries=3):
        """제품 정보 추출 (재시도 로직 포함)"""
        try:
            logger.info(f"🔍 페이지 접속: {url} (시도: {retry_count + 1}/{max_retries + 1})")
            self.driver.get(url)
            
            # 페이지 로드 대기
            wait = WebDriverWait(self.driver, 20)
            time.sleep(random.uniform(3, 5))
            
            # 차단 감지
            title = self.driver.title
            blocked_patterns = ["Access Denied", "Blocked", "Robot", "Captcha", "Sorry", "Error"]
            for pattern in blocked_patterns:
                if pattern.lower() in title.lower():
                    logger.warning(f"⚠️ 차단 감지: {pattern}")
                    raise Exception(f"Blocked: {pattern}")
            
            # 현재 시간
            now_time = datetime.now(self.korea_tz)
            crawl_datetime_str = now_time.strftime('%Y-%m-%d %H:%M:%S')
            crawl_strdatetime = now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4]
            
            # 기본 결과 구조
            result = {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': row_data.get('country', 'nl'),
                'ships_from': 'NL',
                'channel': row_data.get('channel', 'Online'),
                'retailersku': row_data.get('retailersku', ''),
                'brand': row_data.get('brand', ''),
                'brand_eng': row_data.get('brand_eng', row_data.get('brand', '')),
                'form_factor': row_data.get('form_factor', ''),
                'segment_lv1': row_data.get('seg_lv1', ''),
                'segment_lv2': row_data.get('seg_lv2', ''),
                'segment_lv3': row_data.get('seg_lv3', ''),
                'capacity': row_data.get('capacity', ''),
                'item': row_data.get('item', ''),
                'retailprice': None,
                'sold_by': 'Coolblue',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_str,
                'crawl_strdatetime': crawl_strdatetime,
                'title': None,
                'vat': row_data.get('vat', 'o')
            }
            
            # 가격 추출
            try:
                price_found = False
                for xpath in self.XPATHS.get('price', []):
                    try:
                        price_element = self.driver.find_element(By.XPATH, xpath)
                        price_text = price_element.text.strip()
                        
                        if price_text:
                            # Coolblue는 유럽식 숫자 형식 사용 (쉼표가 소수점)
                            # 예: "1.299,99" -> 1299.99
                            price_text_clean = price_text.replace('.', '').replace(',', '.')
                            price_match = re.search(r'([\d,]+\.?\d*)', price_text_clean)
                            if price_match:
                                price_number = price_match.group(1)
                                result['retailprice'] = float(price_number)
                                logger.info(f"✅ 가격 추출 성공: €{result['retailprice']}")
                                price_found = True
                                break
                    except:
                        continue
                
                # CSS 선택자로 재시도
                if not price_found:
                    try:
                        price_element = self.driver.find_element(By.CSS_SELECTOR, "span.js-sales-price-current")
                        price_text = price_element.text
                        if price_text:
                            price_text_clean = price_text.replace('.', '').replace(',', '.').replace('€', '').strip()
                            result['retailprice'] = float(price_text_clean)
                            logger.info(f"✅ 가격 추출 성공 (CSS): €{result['retailprice']}")
                    except:
                        logger.warning("모든 가격 추출 방법 실패")
                        
            except Exception as e:
                logger.warning(f"가격 추출 실패: {e}")
            
            # 제목 추출
            try:
                for xpath in self.XPATHS.get('title', []):
                    try:
                        title_element = self.driver.find_element(By.XPATH, xpath)
                        result['title'] = title_element.text.strip()
                        logger.info(f"제목: {result['title']}")
                        break
                    except:
                        continue
            except Exception as e:
                logger.warning(f"제목 추출 실패: {e}")
            
            # 이미지 URL 추출 (다양한 선택자 시도)
            try:
                image_found = False
                
                # 1. 기존 XPath들 시도
                for xpath in self.XPATHS.get('imageurl', []):
                    try:
                        image_element = self.driver.find_element(By.XPATH, xpath)
                        src = image_element.get_attribute('src')
                        if src and 'image.coolblue.nl' in src:
                            result['imageurl'] = src
                            logger.info(f"이미지 URL (XPath): {result['imageurl']}")
                            image_found = True
                            break
                    except:
                        continue
                
                # 2. CSS 선택자들로 재시도
                if not image_found:
                    css_selectors = [
                        'img[src*="image.coolblue.nl"]',  # Coolblue 이미지 도메인 포함
                        'img[data-src*="image.coolblue.nl"]',  # lazy loading
                        'img.product-image',
                        'img.hero-image',
                        '.product-image img',
                        '.gallery img',
                        'img[alt*="product"]',
                        'img[alt*="Product"]'
                    ]
                    
                    for selector in css_selectors:
                        try:
                            images = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            for img in images:
                                src = img.get_attribute('src') or img.get_attribute('data-src')
                                if src and 'image.coolblue.nl' in src:
                                    result['imageurl'] = src
                                    logger.info(f"이미지 URL (CSS): {result['imageurl']}")
                                    image_found = True
                                    break
                            if image_found:
                                break
                        except:
                            continue
                
                # 3. JavaScript로 이미지 찾기 (최후 수단)
                if not image_found:
                    try:
                        script = """
                        var imgs = document.querySelectorAll('img');
                        for (var i = 0; i < imgs.length; i++) {
                            var src = imgs[i].src || imgs[i].getAttribute('data-src');
                            if (src && src.includes('image.coolblue.nl') && src.includes('products')) {
                                return src;
                            }
                        }
                        return null;
                        """
                        js_result = self.driver.execute_script(script)
                        if js_result:
                            result['imageurl'] = js_result
                            logger.info(f"이미지 URL (JS): {result['imageurl']}")
                            image_found = True
                    except:
                        pass
                
                if not image_found:
                    logger.warning("모든 이미지 추출 방법 실패")
                    
            except Exception as e:
                logger.warning(f"이미지 URL 추출 실패: {e}")
            
            # 재고 상태 확인
            stock_flags = ['Out of Stock', 'Sold Out', 'Currently unavailable', 'Temporarily out of stock']
            page_source = self.driver.page_source
            stock_available = not any(flag in page_source for flag in stock_flags)
            
            # VAT 텍스트 확인
            # vat_texts = ['inclusief btw', 'incl. BTW', 'Tax included', 'Inclusive of all taxes']
            # result['vat'] = 'o' if any(text in page_source for text in vat_texts) else 'x'
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 페이지 처리 오류: {e}")
            
            # 재시도 로직
            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 10  # 재시도마다 대기 시간 증가
                logger.info(f"🔄 {wait_time}초 후 재시도합니다... (재시도 {retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                
                # 드라이버 새로고침
                try:
                    self.driver.refresh()
                except:
                    # 드라이버가 죽었으면 재시작
                    logger.info("🔧 드라이버 재시작 중...")
                    self.driver.quit()
                    self.setup_driver()
                    self.initialize_session()
                
                # 재귀 호출로 재시도
                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)
            
            # 최대 재시도 횟수 초과 시 기본값 반환
            logger.error(f"❌ 최대 재시도 횟수 초과: {url}")
            now_time = datetime.now(self.korea_tz)
            crawl_datetime_str = now_time.strftime('%Y-%m-%d %H:%M:%S')
            crawl_strdatetime = now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4]
            
            return {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': row_data.get('country', 'nl'),
                'ships_from': 'NL',
                'channel': row_data.get('channel', 'Online'),
                'retailersku': row_data.get('retailersku', ''),
                'brand': row_data.get('brand', ''),
                'brand_eng': row_data.get('brand_eng', row_data.get('brand', '')),
                'form_factor': row_data.get('form_factor', ''),
                'segment_lv1': row_data.get('seg_lv1', ''),
                'segment_lv2': row_data.get('seg_lv2', ''),
                'segment_lv3': row_data.get('seg_lv3', ''),
                'capacity': row_data.get('capacity', ''),
                'item': row_data.get('item', ''),
                'retailprice': None,
                'sold_by': 'Coolblue',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_str,
                'crawl_strdatetime': crawl_strdatetime,
                'title': None,
                'vat': row_data.get('vat', 'o')
            }
    
    def save_to_db(self, df):
        """DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("⚠️ DB 연결이 없어 DB 저장을 건너뜁니다")
            return False
        
        try:
            # coolblue_price_crawl_tbl_nl 테이블에 저장
            df.to_sql('coolblue_price_crawl_tbl_nl', self.db_engine, if_exists='append', index=False)
            logger.info(f"✅ DB 저장 완료: {len(df)}개 레코드")
            
            # 크롤링 로그를 pandas DataFrame으로 만들어서 한번에 저장
            log_records = []
            for _, row in df.iterrows():
                log_records.append({
                    'country_code': 'nl',
                    'url': row['producturl'],
                    'error_message': None if row['retailprice'] is not None else 'Price not found',
                    'execution_time': random.uniform(3, 10),
                    'retailprice': row['retailprice'],
                    'crawl_datetime': row['crawl_datetime']
                })
            
            if log_records:
                log_df = pd.DataFrame(log_records)
                log_df.to_sql('amazon_crawl_logs', self.db_engine, if_exists='append', index=False)
                logger.info(f"✅ 크롤링 로그 저장 완료: {len(log_records)}개")
            
            # 저장된 데이터 확인
            with self.db_engine.connect() as conn:
                count_query = "SELECT COUNT(*) FROM coolblue_price_crawl_tbl_nl WHERE DATE(crawl_datetime) = CURDATE()"
                result = conn.execute(count_query)
                today_count = result.scalar()
                logger.info(f"📊 오늘 저장된 총 레코드: {today_count}개")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ DB 저장 실패: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def upload_to_file_server(self, local_file_path, remote_filename=None, country_code='nl'):
        """파일서버에 업로드"""
        try:
            # SFTP 연결
            transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
            transport.connect(
                username=FILE_SERVER_CONFIG['username'],
                password=FILE_SERVER_CONFIG['password']
            )
            sftp = paramiko.SFTPClient.from_transport(transport)
            
            # 원격 파일명 설정
            if remote_filename is None:
                remote_filename = os.path.basename(local_file_path)
            
            # 국가별 디렉토리 경로
            country_dir = f"{FILE_SERVER_CONFIG['upload_path']}/{country_code}"
            
            # 디렉토리가 없으면 생성
            try:
                sftp.stat(country_dir)
            except FileNotFoundError:
                logger.info(f"📁 디렉토리 생성: {country_dir}")
                sftp.mkdir(country_dir)
            
            # 업로드 경로
            remote_path = f"{country_dir}/{remote_filename}"
            
            # 파일 업로드
            sftp.put(local_file_path, remote_path)
            logger.info(f"✅ 파일서버 업로드 완료: {remote_path}")
            
            # 연결 종료
            sftp.close()
            transport.close()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 파일서버 업로드 실패: {e}")
            return False
    
    def save_results(self, df, save_db=True, upload_server=True):
        """결과를 DB와 파일서버에 저장"""
        # 새로운 파일명 형식: {수집일자}{수집시간}_{국가코드}_{쇼핑몰}.csv
        now = datetime.now(self.korea_tz)
        date_str = now.strftime("%Y%m%d")  # 수집일자
        time_str = now.strftime("%H%M%S")  # 수집시간
        country_code = "nl"  # 국가코드
        mall_name = "coolblue"  # 쇼핑몰
        
        # 파일명 생성
        base_filename = f"{date_str}{time_str}_{country_code}_{mall_name}"
        
        results = {
            'db_saved': False,
            'server_uploaded': False
        }
        
        # DB 저장
        if save_db:
            results['db_saved'] = self.save_to_db(df)
        
        # 파일서버 업로드
        if upload_server:
            try:
                # CSV 임시 파일
                temp_csv = f'temp_{base_filename}.csv'
                df.to_csv(temp_csv, index=False, encoding='utf-8-sig')
                
                # 파일서버 업로드
                remote_csv_filename = f'{base_filename}.csv'
                if self.upload_to_file_server(temp_csv, remote_csv_filename, country_code):
                    results['server_uploaded'] = True
                
                # Excel 파일도 생성 및 업로드
                temp_excel = f'temp_{base_filename}.xlsx'
                with pd.ExcelWriter(temp_excel, engine='openpyxl') as writer:
                    # 전체 결과
                    df.to_excel(writer, sheet_name='All_Results', index=False)
                    
                    # 가격이 있는 항목만
                    price_df = df[df['retailprice'].notna()]
                    if not price_df.empty:
                        price_df.to_excel(writer, sheet_name='With_Prices', index=False)
                    
                    # 요약 통계
                    summary = pd.DataFrame({
                        'Metric': [
                            'Total Products', 
                            'Products with Price', 
                            'Products without Price', 
                            'Success Rate (%)',                            
                            'Crawl Date',
                            'Country Code',
                            'Mall Name'
                        ],
                        'Value': [
                            len(df),
                            df['retailprice'].notna().sum(),
                            df['retailprice'].isna().sum(),
                            round(df['retailprice'].notna().sum() / len(df) * 100, 2) if len(df) > 0 else 0,
                            now.strftime('%Y-%m-%d %H:%M:%S'),
                            country_code.upper(),
                            mall_name.capitalize()
                        ]
                    })
                    summary.to_excel(writer, sheet_name='Summary', index=False)
                
                # Excel 파일 업로드
                # remote_excel_filename = f'{base_filename}.xlsx'
                # self.upload_to_file_server(temp_excel, remote_excel_filename, country_code)
                
                # 임시 파일 삭제
                os.remove(temp_csv)
                os.remove(temp_excel)
                logger.info("🗑️ 임시 파일 삭제 완료")
                
                # 업로드된 파일명 로그
                logger.info(f"📁 업로드된 파일:")
                logger.info(f"   - CSV: {remote_csv_filename}")
                # logger.info(f"   - Excel: {remote_excel_filename}")
                
            except Exception as e:
                logger.error(f"파일 처리 오류: {e}")
        
        return results
    
    def test_connection(self):
        """연결 테스트 및 세션 초기화"""
        logger.info("=== Coolblue 세션 초기화 및 테스트 ===")
        
        if not self.setup_driver():
            return False
        
        try:
            # 1단계: Google 연결 테스트
            logger.info("1단계: Google 연결 테스트...")
            self.driver.get("https://www.google.com")
            time.sleep(2)
            google_title = self.driver.title
            
            if "Google" in google_title:
                logger.info("✅ Google 접속 성공")
            else:
                logger.warning("⚠️ Google 접속 이상")
            
            # 2단계: Coolblue 세션 초기화
            if not self.initialize_session():
                return False
            
            # 3단계: 테스트 상품 페이지 접속
            logger.info("3단계: 테스트 상품 페이지 접속...")
            test_url = "https://www.coolblue.nl/product/913729/samsung-990-pro-1tb-nvme-ssd.html"
            test_row = {
                'url': test_url,
                'brand': 'Samsung',
                'item': '990 PRO 1TB',
                'country': 'nl'
            }
            
            test_result = self.extract_product_info(test_url, test_row)
            
            logger.info("추출된 정보:")
            logger.info(f"  - 상품명: {test_result['title']}")
            logger.info(f"  - 가격: €{test_result['retailprice']}")
            logger.info(f"  - 이미지: {'추출됨' if test_result['imageurl'] else '없음'}")
            
            # 4단계: 파일서버 연결 테스트
            logger.info("4단계: 파일서버 연결 테스트...")
            try:
                transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
                transport.connect(
                    username=FILE_SERVER_CONFIG['username'],
                    password=FILE_SERVER_CONFIG['password']
                )
                transport.close()
                logger.info("✅ 파일서버 연결 성공")
            except:
                logger.warning("⚠️ 파일서버 연결 실패 - 크롤링은 계속 진행")
            
            if test_result['retailprice'] or test_result['title']:
                logger.info("✅ 정보 추출 성공 - 크롤링 준비 완료!")
                return True
            else:
                logger.warning("⚠️ 정보 추출 부분 실패 - 그래도 계속 진행")
                return True
                
        except Exception as e:
            logger.error(f"❌ 테스트 실패: {e}")
            return False
    
    def scrape_urls(self, urls_data, max_items=None):
        """여러 URL 스크래핑"""
        if max_items:
            urls_data = urls_data[:max_items]
        
        logger.info(f"📊 총 {len(urls_data)}개 제품 처리 시작")
        
        results = []
        failed_urls = []  # 실패한 URL 추적
        
        try:
            for idx, row in enumerate(urls_data):
                logger.info(f"\n{'='*50}")
                logger.info(f"진행률: {idx + 1}/{len(urls_data)} ({(idx + 1)/len(urls_data)*100:.1f}%)")
                
                # URL 추출
                url = row.get('url')
                
                # 제품 정보 추출 (재시도 로직 포함)
                result = self.extract_product_info(url, row)
                
                # 실패한 URL 추적 로직 추가
                if result['retailprice'] is None:
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'brand': row.get('brand', '')
                    })
                    
                results.append(result)
                
                
                # 10개마다 DB에 중간 저장
                if (idx + 1) % 10 == 0:
                    interim_df = pd.DataFrame(results[-10:])
                    if self.db_engine:
                        try:
                            interim_df.to_sql('coolblue_price_crawl_tbl_nl', self.db_engine, 
                                            if_exists='append', index=False)
                            logger.info(f"💾 중간 저장: 10개 레코드 DB 저장")
                        except Exception as e:
                            logger.error(f"중간 저장 실패: {e}")
                
                # 다음 요청 전 대기
                if idx < len(urls_data) - 1:
                    wait_time = random.uniform(2, 5)
                    logger.info(f"⏳ {wait_time:.1f}초 대기 중...")
                    time.sleep(wait_time)
                    
                    # 10개마다 긴 휴식
                    if (idx + 1) % 10 == 0:
                        logger.info("☕ 10개 처리 완료, 30초 휴식...")
                        time.sleep(30)
        
        except Exception as e:
            logger.error(f"❌ 스크래핑 중 오류: {e}")
        
        finally:
            # 실패 URL 로그
            if failed_urls:
                logger.warning(f"\n⚠️ 가격 추출 실패한 URL {len(failed_urls)}개:")
                for fail in failed_urls[:5]:  # 처음 5개만 표시
                    logger.warning(f"  - {fail['brand']} {fail['item']}: {fail['url']}")
                if len(failed_urls) > 5:
                    logger.warning(f"  ... 외 {len(failed_urls) - 5}개")
            
            if self.driver:
                self.driver.quit()
                logger.info("🔧 드라이버 종료")
        
        return pd.DataFrame(results)
    
    def analyze_results(self, df):
        """결과 분석"""
        logger.info("\n📊 === 결과 분석 ===")
        
        total = len(df)
        with_price = df['retailprice'].notna().sum()
        without_price = df['retailprice'].isna().sum()
        success_rate = (with_price / total * 100) if total > 0 else 0
        
        logger.info(f"전체 제품: {total}개")
        logger.info(f"가격 추출 성공: {with_price}개")
        logger.info(f"가격 추출 실패: {without_price}개")
        logger.info(f"성공률: {success_rate:.1f}%")
        
        if with_price > 0:
            price_df = df[df['retailprice'].notna()].copy()
            price_df['numeric_price'] = price_df['retailprice']
            
            logger.info(f"\n💰 가격 통계:")
            logger.info(f"평균가: €{price_df['numeric_price'].mean():.2f}")
            logger.info(f"최저가: €{price_df['numeric_price'].min():.2f}")
            logger.info(f"최고가: €{price_df['numeric_price'].max():.2f}")
            logger.info(f"중간값: €{price_df['numeric_price'].median():.2f}")
            
            # 브랜드별 통계
            if 'brand' in df.columns:
                brand_stats = price_df['brand'].value_counts()
                logger.info(f"\n📈 브랜드별 성공:")
                for brand, count in brand_stats.items():
                    logger.info(f"  {brand}: {count}개")
            
            # 용량별 평균 가격
            if 'capacity' in df.columns:
                capacity_stats = price_df.groupby('capacity')['numeric_price'].agg(['mean', 'count'])
                logger.info(f"\n💾 용량별 평균 가격:")
                for capacity, stats in capacity_stats.iterrows():
                    logger.info(f"  {capacity}: €{stats['mean']:.2f} ({int(stats['count'])}개)")

def get_db_history(engine, days=7):
    """DB에서 최근 기록 조회"""
    try:
        query = f"""
        SELECT DATE(crawl_datetime) as date, 
               COUNT(*) as total_count,
               SUM(CASE WHEN retailprice IS NOT NULL THEN 1 ELSE 0 END) as with_price,
               COUNT(DISTINCT brand) as brands,
               COUNT(DISTINCT item) as items
        FROM coolblue_price_crawl_tbl_nl
        WHERE crawl_datetime >= DATE_SUB(NOW(), INTERVAL {days} DAY)
        GROUP BY DATE(crawl_datetime)
        ORDER BY date DESC
        """
        
        df = pd.read_sql(query, engine)
        logger.info(f"\n📅 최근 {days}일 크롤링 기록:")
        if not df.empty:
            print(df.to_string(index=False))
        else:
            logger.info("최근 크롤링 기록이 없습니다.")
        
    except Exception as e:
        logger.error(f"DB 조회 오류: {e}")

def main():
    """메인 실행 함수"""
    print("\n🚀 Coolblue 가격 추출 시스템 - DB 기반 버전 (이미지 URL 추출 개선)")
    print("="*60)
    
    # 스크래퍼 초기화
    scraper = CoolblueScraper()
    
    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        return
    
    # 최근 크롤링 기록 확인
    get_db_history(scraper.db_engine, 7)
    
    # 테스트 모드 확인
    test_mode = os.getenv("TEST_MODE", "false").lower()
    
    if test_mode in ["true", "1", "yes"]:
        logger.info("🧪 테스트 모드 실행")
        
        # 연결 테스트
        if scraper.test_connection():
            logger.info("✅ 테스트 완료")
            
            # 테스트 결과 업로드
            test_df = pd.DataFrame([{
                'test_type': 'session_test',
                'country_code': 'nl',
                'test_datetime': datetime.now(scraper.korea_tz)
            }])
            
            scraper.save_results(test_df, save_db=False, upload_server=True)
        else:
            logger.error("❌ 테스트 실패")
            
        if scraper.driver:
            scraper.driver.quit()
        return
    
    # 실제 크롤링
    logger.info("\n📊 실제 크롤링 시작")
    
    # 연결 테스트
    if not scraper.test_connection():
        logger.error("연결 테스트 실패로 종료합니다.")
        return
    
    # 크롤링 대상 조회
    urls_data = scraper.get_crawl_targets()
    
    if not urls_data:
        logger.warning("크롤링 대상이 없습니다.")
        return
    
    logger.info(f"✅ 크롤링 대상: {len(urls_data)}개")
    
    # 시작 시간
    start_time = datetime.now(scraper.korea_tz)
    
    # 크롤링 실행
    results_df = scraper.scrape_urls(urls_data)
    
    if results_df is None or results_df.empty:
        logger.error("크롤링 결과가 없습니다.")
        return
    
    # 종료 시간
    end_time = datetime.now(scraper.korea_tz)
    
    # 최종 결과 저장
    logger.info("\n💾 최종 결과 저장")
    
    # 최종 통계
    success_count = results_df['retailprice'].notna().sum()
    failed_count = results_df['retailprice'].isna().sum()
    success_rate = (success_count / len(results_df) * 100) if len(results_df) > 0 else 0
    
    logger.info(f"\n📊 === 최종 결과 ===")
    logger.info(f"전체: {len(results_df)}개")
    logger.info(f"성공: {success_count}개")
    logger.info(f"실패: {failed_count}개")
    logger.info(f"성공률: {success_rate:.1f}%")
    logger.info(f"소요 시간: {round((end_time - start_time).total_seconds() / 60, 2)} 분")
    
    # DB와 파일서버에 최종 결과 저장
    save_results = scraper.save_results(
        results_df,
        save_db=True,
        upload_server=True
    )
    
    # 상세 분석
    scraper.analyze_results(results_df)
    
    # 저장 결과 출력
    logger.info("\n📊 저장 결과:")
    logger.info(f"DB 저장: {'✅ 성공' if save_results['db_saved'] else '❌ 실패'}")
    logger.info(f"파일서버 업로드: {'✅ 성공' if save_results['server_uploaded'] else '❌ 실패'}")
    
    # 실패한 URL 로그
    if failed_count > 0:
        logger.warning(f"\n⚠️ {failed_count}개 URL에서 크롤링 실패")
        failed_items = results_df[results_df['retailprice'].isna()]    # retailprice로 판단
        logger.warning("실패 목록 (상위 5개):")
        for idx, row in failed_items.head().iterrows():
            logger.warning(f"  - {row['brand']} {row['item']}: {row['producturl'][:50]}...")
    
    logger.info("\n✅ 크롤링 프로세스 완료!")
    logger.info(f"📁 모든 결과 파일이 파일서버에 업로드되었습니다.")
    logger.info(f"📍 업로드 위치: {FILE_SERVER_CONFIG['host']}:{FILE_SERVER_CONFIG['upload_path']}/")

if __name__ == "__main__":
    # 필요한 패키지 설치 확인
    required_packages = [
        'undetected-chromedriver',
        'selenium',
        'pandas',
        'pymysql',
        'sqlalchemy',
        'paramiko',
        'openpyxl'
    ]
    
    print("📦 필요한 패키지:")
    print("pip install " + " ".join(required_packages))
    print("\n⚠️ DB 설정을 먼저 확인하세요:")
    print("DB_CONFIG 딕셔너리의 user, password, host 정보를 실제 값으로 변경해야 합니다.")
    print()
    
    main()