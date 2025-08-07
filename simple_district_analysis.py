import csv
from difflib import SequenceMatcher

def load_csv_data(filename):
    """Load CSV data without pandas"""
    data = []
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Skip header
        for row in reader:
            data.append(row)
    return data, headers

def clean_district_name(name):
    """Clean district name for better matching"""
    if not name:
        return ""
    
    # Remove common suffixes and clean up
    name = str(name).strip()
    name = name.replace(' tumani', '').replace(' district', '').replace(' District', '')
    name = name.replace(' shahri', '').replace(' city', '').replace(' City', '')
    name = name.replace('Respublikasi', '').replace('Республика', '').replace('Republic of', '')
    name = name.replace('Ўзбекистон', '').replace('Узбекистан', '').replace('Uzbekistan', '')
    
    return name.strip()

def find_best_match(district_name, reference_names, threshold=0.7):
    """Find the best matching district name using fuzzy matching"""
    cleaned_name = clean_district_name(district_name)
    
    best_match = None
    best_score = 0
    
    for ref_name in reference_names:
        cleaned_ref = clean_district_name(ref_name)
        
        # Calculate similarity score
        score = SequenceMatcher(None, cleaned_name.lower(), cleaned_ref.lower()).ratio()
        
        if score > best_score and score >= threshold:
            best_score = score
            best_match = ref_name
    
    return best_match, best_score

def analyze_district_matching():
    """Analyze district matching between the two files"""
    print("Loading data...")
    
    # Load main data file
    main_data, main_headers = load_csv_data('part-00000-7eaec3f3-77ab-415c-ade1-cd47c9a52da1-c000.csv')
    print(f"Main data file: {len(main_data)} rows")
    
    # Load reference district file
    ref_data, ref_headers = load_csv_data('reference_district_202508071730.csv')
    print(f"Reference district file: {len(ref_data)} rows")
    
    # Find the column indices
    main_klassifikator_idx = main_headers.index('Klassifikator')
    ref_name_uz_idx = ref_headers.index('name_uz')
    
    # Get unique districts from main file
    main_districts = set()
    for row in main_data:
        main_districts.add(row[main_klassifikator_idx])
    
    # Get unique districts from reference file
    ref_districts = set()
    for row in ref_data:
        ref_districts.add(row[ref_name_uz_idx])
    
    print(f"\nUnique districts in main file: {len(main_districts)}")
    print(f"Unique districts in reference file: {len(ref_districts)}")
    
    print("\nFirst 10 districts from main file:")
    for i, district in enumerate(sorted(main_districts)[:10]):
        print(f"  {i+1}. {district}")
    
    print("\nFirst 10 districts from reference file:")
    for i, district in enumerate(sorted(ref_districts)[:10]):
        print(f"  {i+1}. {district}")
    
    # Match districts
    print("\n" + "="*80)
    print("DISTRICT MATCHING ANALYSIS")
    print("="*80)
    
    matches = []
    unmatched_main = []
    
    # Try to match each main district with reference districts
    for main_district in main_districts:
        best_match, score = find_best_match(main_district, ref_districts)
        
        if best_match:
            matches.append({
                'main_district': main_district,
                'reference_district': best_match,
                'match_score': score
            })
        else:
            unmatched_main.append(main_district)
    
    # Find unmatched reference districts
    matched_ref_districts = {m['reference_district'] for m in matches}
    unmatched_ref = ref_districts - matched_ref_districts
    
    # Print results
    print(f"\nMATCHED DISTRICTS ({len(matches)}):")
    print("-" * 80)
    for match in sorted(matches, key=lambda x: x['match_score'], reverse=True):
        print(f"{match['main_district']:<40} -> {match['reference_district']:<40} (Score: {match['match_score']:.3f})")
    
    print(f"\nUNMATCHED MAIN DISTRICTS ({len(unmatched_main)}):")
    print("-" * 80)
    for district in sorted(unmatched_main):
        print(f"  {district}")
    
    print(f"\nUNMATCHED REFERENCE DISTRICTS ({len(unmatched_ref)}):")
    print("-" * 80)
    for district in sorted(unmatched_ref):
        print(f"  {district}")
    
    # Calculate summary statistics
    if matches:
        scores = [m['match_score'] for m in matches]
        avg_score = sum(scores) / len(scores)
        min_score = min(scores)
        max_score = max(scores)
        
        print(f"\nMATCHING SUMMARY:")
        print(f"  Total main districts: {len(main_districts)}")
        print(f"  Total reference districts: {len(ref_districts)}")
        print(f"  Successfully matched: {len(matches)}")
        print(f"  Unmatched main districts: {len(unmatched_main)}")
        print(f"  Unmatched reference districts: {len(unmatched_ref)}")
        print(f"  Average match score: {avg_score:.3f}")
        print(f"  Min match score: {min_score:.3f}")
        print(f"  Max match score: {max_score:.3f}")
        
        # Save matching results
        with open('district_matching_results.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['main_district', 'reference_district', 'match_score'])
            for match in matches:
                writer.writerow([match['main_district'], match['reference_district'], match['match_score']])
        
        print(f"\nMatching results saved to 'district_matching_results.csv'")
    
    return matches, unmatched_main, unmatched_ref

def create_merged_dataset():
    """Create a merged dataset with matched districts"""
    print("\n" + "="*80)
    print("CREATING MERGED DATASET")
    print("="*80)
    
    # Load data
    main_data, main_headers = load_csv_data('part-00000-7eaec3f3-77ab-415c-ade1-cd47c9a52da1-c000.csv')
    ref_data, ref_headers = load_csv_data('reference_district_202508071730.csv')
    
    # Get matches
    matches, _, _ = analyze_district_matching()
    
    if not matches:
        print("No matches found. Cannot create merged dataset.")
        return
    
    # Create a mapping dictionary
    district_mapping = {m['main_district']: m['reference_district'] for m in matches}
    
    # Create reference lookup dictionary
    ref_lookup = {}
    ref_name_uz_idx = ref_headers.index('name_uz')
    ref_name_en_idx = ref_headers.index('name_en')
    ref_name_ru_idx = ref_headers.index('name_ru')
    ref_code_idx = ref_headers.index('code')
    ref_region_id_idx = ref_headers.index('region_id')
    
    for row in ref_data:
        ref_lookup[row[ref_name_uz_idx]] = {
            'name_en': row[ref_name_en_idx],
            'name_ru': row[ref_name_ru_idx],
            'code': row[ref_code_idx],
            'region_id': row[ref_region_id_idx]
        }
    
    # Create merged dataset
    merged_data = []
    main_klassifikator_idx = main_headers.index('Klassifikator')
    
    for row in main_data:
        main_district = row[main_klassifikator_idx]
        ref_district = district_mapping.get(main_district)
        
        if ref_district and ref_district in ref_lookup:
            ref_info = ref_lookup[ref_district]
            new_row = row + [
                ref_district,
                ref_info['name_en'],
                ref_info['name_ru'],
                ref_info['code'],
                ref_info['region_id']
            ]
        else:
            new_row = row + ['', '', '', '', '']
        
        merged_data.append(new_row)
    
    # Create new headers
    new_headers = main_headers + ['ref_name_uz', 'ref_name_en', 'ref_name_ru', 'ref_code', 'ref_region_id']
    
    # Save merged dataset
    with open('merged_district_data.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(new_headers)
        writer.writerows(merged_data)
    
    print(f"Merged dataset shape: {len(merged_data)} rows, {len(new_headers)} columns")
    
    # Count rows with reference data
    rows_with_ref = sum(1 for row in merged_data if row[-5])  # Check if ref_name_uz is not empty
    print(f"Rows with reference data: {rows_with_ref}")
    print(f"Rows without reference data: {len(merged_data) - rows_with_ref}")
    
    print("Merged dataset saved to 'merged_district_data.csv'")
    
    # Show sample of merged data
    print("\nSample of merged data:")
    sample_cols = ['Year', 'Code', 'Klassifikator', 'Value', 'ref_name_uz', 'ref_name_en', 'ref_code', 'ref_region_id']
    sample_indices = [main_headers.index(col) for col in sample_cols[:4]]
    sample_indices.extend([len(main_headers) + i for i in range(4)])  # Add ref columns
    
    print("Year  Code   Klassifikator                    Value  ref_name_uz                    ref_name_en                    ref_code  ref_region_id")
    print("-" * 120)
    for i, row in enumerate(merged_data[:10]):
        sample_values = [row[idx] for idx in sample_indices]
        print(f"{sample_values[0]:<5} {sample_values[1]:<6} {sample_values[2]:<35} {sample_values[3]:<7} {sample_values[4]:<35} {sample_values[5]:<35} {sample_values[6]:<8} {sample_values[7]}")
    
    return merged_data

if __name__ == "__main__":
    print("DISTRICT MATCHING ANALYSIS")
    print("="*80)
    
    # Analyze matching
    matches, unmatched_main, unmatched_ref = analyze_district_matching()
    
    # Create merged dataset
    merged_df = create_merged_dataset()
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)