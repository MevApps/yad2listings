import re
import json
import csv
from typing import List, Dict
from datetime import datetime
from bs4 import BeautifulSoup
import os
from pathlib import Path

today = datetime.now().date().strftime("%y_%m_%d")

def extract_json_from_html(html_content: str) -> Dict:
    """Extract JSON data from __NEXT_DATA__ script tag"""
    soup = BeautifulSoup(html_content, 'html.parser')
    script_tag = soup.find('script', id='__NEXT_DATA__')
    
    if script_tag is None:
        raise ValueError("Could not find __NEXT_DATA__ script tag in HTML")
        
    return json.loads(script_tag.string)

def get_month_number(month_text: str) -> int:
    # Hebrew month names to numbers mapping
    month_mapping = {
        'ינואר': 1, 'פברואר': 2, 'מרץ': 3, 'אפריל': 4,
        'מאי': 5, 'יוני': 6, 'יולי': 7, 'אוגוסט': 8,
        'ספטמבר': 9, 'אוקטובר': 10, 'נובמבר': 11, 'דצמבר': 12
    }
    return month_mapping.get(month_text, 1)  # Default to 1 if month not found

def calculate_years_since_production(production_year: int, production_month: int) -> float:
    production_date = datetime(production_year, production_month, 1)
    current_date = datetime.now()
    years = (current_date - production_date).days / 365.25
    return years

def process_vehicle_data(json_list: List[Dict], listing_type: str, output_file: str, mode: str = 'w', debug: bool = False) -> None:
    """Process vehicle data and write to CSV"""
    
    # Debug mode: inspect first item
    if debug and json_list:
        print(f"\n{'='*60}")
        print(f"DEBUG: Inspecting first item in {listing_type} listings")
        print(f"{'='*60}")
        item = json_list[0]
        print(f"All keys: {list(item.keys())}")
        print(f"\nDetailed structure:")
        for key, value in item.items():
            if isinstance(value, dict):
                print(f"  {key}: dict with keys {list(value.keys())}")
            elif isinstance(value, list):
                print(f"  {key}: list with {len(value)} items")
            else:
                print(f"  {key}: {type(value).__name__} = {value if not isinstance(value, str) or len(str(value)) < 50 else str(value)[:50] + '...'}")
        print(f"{'='*60}\n")
    
    # Define the headers we want to extract
    headers = ['adNumber', 'price', 'city', 'adType', 'model', 'subModel', 
              'productionDate', 'km', 'hand', 'createdAt', 'updatedAt', 
              'rebouncedAt', 'listingType', 'number_of_years', 'km_per_year', 'description', 'link', 'make', 'hp']
    
    # Open the CSV file for writing
    with open(output_file, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        if mode == 'w':  # Only write header if we're creating a new file
            writer.writeheader()
        
        processed_count = 0
        # Process each JSON object
        for item in json_list:
            try:
                # Get ad number - use orderId as fallback
                ad_number = item.get('orderId', item.get('token', 'unknown'))
                
                # Skip if price is 0 or missing (likely "contact dealer" listings)
                if 'price' not in item or item['price'] == 0:
                    continue
                
                # Skip if required vehicle fields are missing
                required_fields = ['manufacturer', 'model', 'subModel', 'vehicleDates', 'token']
                if not all(field in item for field in required_fields):
                    continue
                
                # Create date string in YYYY-MM-DD format for production date
                year = item['vehicleDates']['yearOfProduction']
                month_data = item['vehicleDates'].get('monthOfProduction', {"text": "ינואר"})
                month = get_month_number(month_data.get('text', 'ינואר'))
                production_date = f"{year}-{month:02d}-01"
                
                # Calculate years since production
                years_since_production = calculate_years_since_production(year, month)
                
                # Handle missing km field - use 0 as default
                # Note: km field doesn't exist in this data structure
                km = item.get('km', 0)
                
                # Calculate km per year (handle division by zero)
                if years_since_production > 0 and km > 0:
                    km_per_year = round(km / years_since_production, 2)
                else:
                    km_per_year = 0
                
                # Extract HP from subModel text
                hp_match = re.search(r'(\d+)\s*כ״ס', item['subModel']['text'])
                hp = int(hp_match.group(1)) if hp_match else 0
                
                # Get city - handle different address structures
                city = ''
                if 'address' in item:
                    if 'city' in item['address']:
                        city = item['address']['city'].get('text', '')
                    elif 'area' in item['address']:
                        city = item['address']['area'].get('text', '')
                
                # Get description from metaData if available
                description = ''
                if 'metaData' in item and 'description' in item['metaData']:
                    description = item['metaData']['description']
                
                # Use current date as placeholder for missing date fields
                current_date = datetime.now().strftime('%Y-%m-%d')
                
                row = {
                    'adNumber': ad_number,
                    'price': item['price'],
                    'city': city,
                    'adType': item.get('adType', ''),
                    'model': item['model']['text'],
                    'subModel': item['subModel']['text'],
                    'hp': hp,
                    'make': item['manufacturer']['text'],
                    'productionDate': production_date,
                    'km': km,
                    'hand': item.get('hand', {"id": 0})["id"],
                    'createdAt': current_date,  # Not available in this data structure
                    'updatedAt': current_date,  # Not available in this data structure
                    'rebouncedAt': current_date,  # Not available in this data structure
                    'listingType': listing_type,
                    'number_of_years': round(years_since_production, 2),
                    'km_per_year': km_per_year,
                    'description': description,
                    'link': f'https://www.yad2.co.il/vehicles/item/{item["token"]}',
                }
                writer.writerow(row)
                processed_count += 1
            except KeyError as e:
                if debug:
                    print(f"Skipping item {ad_number} due to missing key: {e}")
            except Exception as e:
                if debug:
                    print(f"Error processing item {ad_number}: {e}")
        
        # Report how many items were actually processed
        if processed_count > 0:
            print(f"✓ Successfully processed {processed_count} valid items")

def process_directory(directory_path: str, debug: bool = False) -> None:
    """Process all HTML files in a directory and combine the data"""
    # Get directory name for the output file
    dir_name = Path(directory_path).name
    output_file = f"{dir_name}_summary.csv"
    output_path = os.path.join(directory_path, output_file)
    
    # Process each HTML file in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith('.html') and today in filename:
            file_path = os.path.join(directory_path, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    print(f"Processing {filename}...")
                    html_content = file.read()
                    data = extract_json_from_html(html_content)
                    listings_data = data['props']['pageProps']['dehydratedState']['queries'][0]['state']['data']
                    
                    # Process commercial listings
                    commercial_list = listings_data.get('commercial', [])
                    if commercial_list:
                        mode = 'a' if os.path.exists(output_path) else 'w'
                        process_vehicle_data(commercial_list, 'commercial', output_path, mode, debug=debug)
                        debug = False  # Only debug first batch
                    
                    # Process private listings
                    private_list = listings_data.get('private', [])
                    if private_list:
                        mode = 'a' if os.path.exists(output_path) else 'w'
                        process_vehicle_data(private_list, 'private', output_path, mode)
                    
                    # Process solo listings
                    solo_list = listings_data.get('solo', [])
                    if solo_list:
                        mode = 'a' if os.path.exists(output_path) else 'w'
                        process_vehicle_data(solo_list, 'solo', output_path, mode)
                    
                    # Process platinum listings
                    platinum_list = listings_data.get('platinum', [])
                    if platinum_list:
                        mode = 'a' if os.path.exists(output_path) else 'w'
                        process_vehicle_data(platinum_list, 'platinum', output_path, mode)
                    
            except Exception as e:
                print(f"Error processing {filename}: {e}")
    
    print(f"\n✓ Output saved to: {output_path}")

if __name__ == "__main__":
    directory_path = "scraped_vehicles"
    process_directory(directory_path, debug=True)