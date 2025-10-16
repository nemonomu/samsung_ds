from lxml import html

# Read first product HTML
with open('first_product.txt', 'r', encoding='utf-8') as f:
    html_content = f.read()

# Parse with lxml
tree = html.fromstring(html_content)

# Test XPath
xpath = './/h2/a/@href'
result = tree.xpath(xpath)

print("="*80)
print("Testing URL Extraction")
print("="*80)
print(f"XPath: {xpath}")
print(f"Result type: {type(result)}")
print(f"Result: {result}")

if result:
    print(f"\nExtracted path: {result[0]}")
    print(f"Is string: {isinstance(result[0], str)}")
    print(f"\nComplete URL: https://www.amazon.com{result[0]}")
else:
    print("\n[ERROR] No result found")

input("\nPress Enter to exit...")
