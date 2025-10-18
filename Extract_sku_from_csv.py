import pandas as pd
import re

def extract_linnworks_skus(csv_file_path):
    # Read the CSV file
    df = pd.read_csv(csv_file_path)
    
    linnworks_skus = []
    
    # Extract linnworks_sku from additional_attributes column
    for index, row in df.iterrows():
        additional_attrs = row.get('additional_attributes', '')
        
        # Use regex to find linnworks_sku value
        match = re.search(r'linnworks_sku=([^,]+)', str(additional_attrs))
        if match:
            linnworks_skus.append(match.group(1))
    
    return linnworks_skus

# Usage - FIXED PATH (choose one option below):

# Option 1: Use raw string (add 'r' before the string)
csv_file = r"C:\Users\shatc\Downloads\export_catalog_product_20251007_121719.csv"

# Option 2: Use forward slashes (works on Windows too)
# csv_file = "C:/Users/shatc/Downloads/export_catalog_product_20251007_121719.csv"

# Option 3: Use double backslashes
# csv_file = "C:\\Users\\shatc\\Downloads\\export_catalog_product_20251007_121719.csv"

skus = extract_linnworks_skus(csv_file)

print(f"Total linnworks_sku values found: {len(skus)}")
print("\nAll linnworks_sku values:")
for i, sku in enumerate(skus, 1):
    print(f"{i}. {sku}")

# Save to a text file
with open('linnworks_skus.txt', 'w') as f:
    for sku in skus:
        f.write(sku + '\n')

print(f"\nSaved {len(skus)} SKUs to 'linnworks_skus.txt'")