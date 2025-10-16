# Samsung DX - Amazon TV Crawler

Amazon TV 제품 정보 수집 크롤러

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Setup database:
```bash
python db_setup.py
python insert_xpaths.py
```

3. Run crawler:
```bash
python amazon_crawler.py
```

## Database Configuration

- Host: samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com
- Port: 5432
- Database: postgres

## Files

- `amazon_crawler.py` - Main crawler script
- `db_setup.py` - Database table creation
- `insert_xpaths.py` - Insert XPath selectors
- `check_tables.py` - Verify database tables
- `requirements.txt` - Python dependencies
