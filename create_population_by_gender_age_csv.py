import csv

# Mapping for age categories
AGE_CATEGORIES = [
    ("Women under 18", 0, 1),
    ("Women 18-25", 0, 2),
    ("Women 26-35", 0, 3),
    ("Women 36-45", 0, 4),
    ("Women 46-55", 0, 5),
    ("Women 56+", 0, 6),
    ("Men under 18", 1, 1),
    ("Men 18-25", 1, 2),
    ("Men 26-35", 1, 3),
    ("Men 36-45", 1, 4),
    ("Men 46-55", 1, 5),
    ("Men 56+", 1, 6),
]

# Build region_id -> region_name_en mapping from reference file
def build_region_map(ref_file):
    region_map = {}
    with open(ref_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            region_id = row['region_id']
            region_name_en = row['name_en'].split()[0]  # e.g., "Andijan District" -> "Andijan"
            # Use the first word as region name, but keep the full name for uniqueness
            if region_id not in region_map:
                # Try to extract region name from "District" names
                if 'region' in region_name_en.lower():
                    region_map[region_id] = region_name_en
                else:
                    # Fallback: use the last word as region name
                    region_map[region_id] = region_name_en
    # Manual fixes for known region ids (if needed)
    # region_map['1'] = 'Karakalpakstan'  # Example
    return region_map

def create_population_by_gender_age_csv(merged_file, ref_file, output_file):
    region_map = build_region_map(ref_file)
    with open(merged_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    output_rows = []
    next_id = 1
    for row in rows:
        # Filter for year 2025 only
        if row.get('Year', '') != '2025':
            continue
        district_id = row.get('district_id', '')
        region_id = row.get('ref_region_id', '')
        district_name_en = row.get('ref_name_en', '')
        region_name_en = region_map.get(region_id, '')
        # For each age/gender category
        for col, gender, age_cat in AGE_CATEGORIES:
            pop_count = row.get(col, '').strip()
            if pop_count and pop_count != '':
                try:
                    pop_count_val = int(float(pop_count))
                except Exception:
                    continue
                output_rows.append({
                    'id': next_id,
                    'district_id': district_id,
                    'region_id': region_id,
                    'district_name(en)': district_name_en,
                    'region_name(en)': region_name_en,
                    'gender_category': gender,
                    'age_category': age_cat,
                    'population_count': pop_count_val
                })
                next_id += 1
    # Write output
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'id', 'district_id', 'region_id', 'district_name(en)', 'region_name(en)', 'gender_category', 'age_category', 'population_count'
        ])
        writer.writeheader()
        writer.writerows(output_rows)
    print(f"Created {output_file} with {len(output_rows)} rows.")

if __name__ == "__main__":
    create_population_by_gender_age_csv(
        'merged_district_data_with_id.csv',
        'reference_district_202508071730.csv',
        'district_population_by_gender_age.csv'
    )