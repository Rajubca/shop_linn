import pandas as pd
import re
from io import StringIO

# Read the CSV data
# df = pd.read_csv('ebay_uk_descriptions2.csv')
# df = pd.read_csv('ebay_uk_descriptions_battery_operated.csv')
df = pd.read_csv('ebay_uk_descriptions_Garden.csv')
# df = pd.read_csv('ebay_uk_descriptions_battery_operated.csv')
# df = pd.read_csv('ebay_uk_descriptions_battery_operated.csv')
# df = pd.read_csv('ebay_uk_descriptions_battery_operated.csv')

def extract_specifications(description):
    """Extract specifications from product description and format them line by line"""
    if not isinstance(description, str):
        return ""
    
    # Remove HTML tags but preserve text content
    clean_text = re.sub('<[^<]+?>', ' ', description)
    clean_text = re.sub('\s+', ' ', clean_text).strip()
    
    specs = []
    
    # Look for specification sections and extract key-value pairs
    spec_sections = re.findall(r'Specifications[^:]*:(.*?)(?=Key Features|Features|Description|Note:|$)', clean_text, re.IGNORECASE | re.DOTALL)
    
    if spec_sections:
        # Extract from specification section
        spec_text = spec_sections[0]
        # Look for common specification patterns
        patterns = [
            (r'LED Color:?\s*([^\.\n]+)', 'LED Color'),
            (r'LED Colour:?\s*([^\.\n]+)', 'LED Color'),
            (r'Cable Color:?\s*([^\.\n]+)', 'Cable Color'),
            (r'Cable Colour:?\s*([^\.\n]+)', 'Cable Color'),
            (r'LED Counts?:?\s*([^\.\n]+)', 'LED Count'),
            (r'Number of LEDs:?\s*([^\.\n]+)', 'LED Count'),
            (r'Total Length:?\s*([^\.\n]+)', 'Total Length'),
            (r'Total length:?\s*([^\.\n]+)', 'Total Length'),
            (r'Distance between 2 bulbs:?\s*([^\.\n]+)', 'Bulb Spacing'),
            (r'10cm space between two bulbs', 'Bulb Spacing: 10cm'),
            (r'5cm space between two bulbs', 'Bulb Spacing: 5cm'),
            (r'UK plug-operated', 'Power: UK Plug'),
            (r'UK plug operated', 'Power: UK Plug'),
            (r'LED bulb size[^\.]*?about\s*([^\.\n]+)', 'Bulb Size'),
            (r'Cable material:?\s*([^\.\n]+)', 'Cable Material'),
            (r'IP44 Waterproof', 'Waterproof Rating: IP44'),
            (r'Light Color:?\s*([^\.\n]+)', 'Light Color')
        ]
        
        for pattern, label in patterns:
            matches = re.findall(pattern, spec_text, re.IGNORECASE)
            for match in matches:
                if match:
                    if label in ['Bulb Spacing: 10cm', 'Bulb Spacing: 5cm', 'Power: UK Plug', 'Waterproof Rating: IP44']:
                        specs.append(label)
                    else:
                        specs.append(f"{label}: {match.strip()}")
    
    # If no specification section found, try to extract from the entire text
    if not specs:
        # Look for common specification patterns in the entire text
        patterns = [
            (r'LED Color:?\s*([^\.\n]+)', 'LED Color'),
            (r'LED Colour:?\s*([^\.\n]+)', 'LED Color'),
            (r'Cable Color:?\s*([^\.\n]+)', 'Cable Color'),
            (r'Cable Colour:?\s*([^\.\n]+)', 'Cable Color'),
            (r'LED Counts?:?\s*([^\.\n]+)', 'LED Count'),
            (r'Total Length:?\s*([^\.\n]+)', 'Total Length'),
            (r'UK plug-operated', 'Power: UK Plug'),
            (r'UK plug operated', 'Power: UK Plug')
        ]
        
        for pattern, label in patterns:
            matches = re.findall(pattern, clean_text, re.IGNORECASE)
            for match in matches:
                if match:
                    if label in ['Power: UK Plug']:
                        specs.append(label)
                    else:
                        specs.append(f"{label}: {match.strip()}")
    
    # Remove duplicates while preserving order
    seen = set()
    unique_specs = []
    for spec in specs:
        if spec not in seen:
            seen.add(spec)
            unique_specs.append(spec)
    
    return "\n".join(unique_specs) if unique_specs else "Specifications not found in description"

# Apply the function to create the new column
df['specifications'] = df['ebay_uk_description'].apply(extract_specifications)

# Save the result to a new CSV file
df.to_csv('ebay_uk_descriptions_with_specs.csv', index=False)

# Display sample results
print("Sample extracted specifications:")
print("=" * 50)
for i, row in df.head(10).iterrows():
    print(f"\nSKU: {row['linnworks_sku']}")
    print("Specifications:")
    print(row['specifications'])
    print("-" * 30)

print(f"\nTotal products processed: {len(df)}")