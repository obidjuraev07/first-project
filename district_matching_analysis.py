import pandas as pd
import numpy as np
from difflib import SequenceMatcher

def load_data():
    """Load both CSV files"""
    # Load the main data file
    main_df = pd.read_csv('part-00000-7eaec3f3-77ab-415c-ade1-cd47c9a52da1-c000.csv')
    
    # Load the reference district file
    ref_df = pd.read_csv('reference_district_202508071730.csv')
    
    return main_df, ref_df

def clean_district_name(name):
    """Clean district name for better matching"""
    if pd.isna(name):
        return ""
    
    # Remove common suffixes and clean up
    name = str(name).strip()
    name = name.replace(' tumani', '').replace(' district', '').replace(' District', '')
    name = name.replace(' shahri', '').replace(' city', '').replace(' City', '')
    name = name.replace('Respublikasi', '').replace('Республика', '').replace('Republic of', '')
    name = name.replace('Ўзбекистон', '').replace('Узбекистан', '').replace('Uzbekistan', '')
    
    return name.strip()

def find_best_match(district_name, reference_names, threshold=0.8):
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
    main_df, ref_df = load_data()
    
    print(f"\nMain data file shape: {main_df.shape}")
    print(f"Reference district file shape: {ref_df.shape}")
    
    # Get unique districts from main file
    main_districts = main_df['Klassifikator'].unique()
    ref_districts = ref_df['name_uz'].unique()
    
    print(f"\nUnique districts in main file: {len(main_districts)}")
    print(f"Unique districts in reference file: {len(ref_districts)}")
    
    print("\nFirst 10 districts from main file:")
    for i, district in enumerate(main_districts[:10]):
        print(f"  {i+1}. {district}")
    
    print("\nFirst 10 districts from reference file:")
    for i, district in enumerate(ref_districts[:10]):
        print(f"  {i+1}. {district}")
    
    # Match districts
    print("\n" + "="*80)
    print("DISTRICT MATCHING ANALYSIS")
    print("="*80)
    
    matches = []
    unmatched_main = []
    unmatched_ref = []
    
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
    matched_ref_districts = [m['reference_district'] for m in matches]
    unmatched_ref = [d for d in ref_districts if d not in matched_ref_districts]
    
    # Print results
    print(f"\nMATCHED DISTRICTS ({len(matches)}):")
    print("-" * 80)
    for match in sorted(matches, key=lambda x: x['match_score'], reverse=True):
        print(f"{match['main_district']:<40} -> {match['reference_district']:<40} (Score: {match['match_score']:.3f})")
    
    print(f"\nUNMATCHED MAIN DISTRICTS ({len(unmatched_main)}):")
    print("-" * 80)
    for district in unmatched_main:
        print(f"  {district}")
    
    print(f"\nUNMATCHED REFERENCE DISTRICTS ({len(unmatched_ref)}):")
    print("-" * 80)
    for district in unmatched_ref:
        print(f"  {district}")
    
    # Create a summary DataFrame for matched districts
    if matches:
        match_df = pd.DataFrame(matches)
        print(f"\nMATCHING SUMMARY:")
        print(f"  Total main districts: {len(main_districts)}")
        print(f"  Total reference districts: {len(ref_districts)}")
        print(f"  Successfully matched: {len(matches)}")
        print(f"  Unmatched main districts: {len(unmatched_main)}")
        print(f"  Unmatched reference districts: {len(unmatched_ref)}")
        print(f"  Average match score: {match_df['match_score'].mean():.3f}")
        print(f"  Min match score: {match_df['match_score'].min():.3f}")
        print(f"  Max match score: {match_df['match_score'].max():.3f}")
        
        # Save matching results
        match_df.to_csv('district_matching_results.csv', index=False)
        print(f"\nMatching results saved to 'district_matching_results.csv'")
    
    return matches, unmatched_main, unmatched_ref

def create_merged_dataset():
    """Create a merged dataset with matched districts"""
    print("\n" + "="*80)
    print("CREATING MERGED DATASET")
    print("="*80)
    
    main_df, ref_df = load_data()
    
    # Get matches
    matches, _, _ = analyze_district_matching()
    
    if not matches:
        print("No matches found. Cannot create merged dataset.")
        return
    
    # Create a mapping dictionary
    district_mapping = {m['main_district']: m['reference_district'] for m in matches}
    
    # Add reference district info to main dataset
    main_df['reference_district'] = main_df['Klassifikator'].map(district_mapping)
    
    # Merge with reference data
    merged_df = main_df.merge(
        ref_df[['name_uz', 'name_en', 'name_ru', 'code', 'region_id']], 
        left_on='reference_district', 
        right_on='name_uz', 
        how='left'
    )
    
    # Rename columns for clarity
    merged_df = merged_df.rename(columns={
        'name_uz': 'ref_name_uz',
        'name_en': 'ref_name_en', 
        'name_ru': 'ref_name_ru',
        'code': 'ref_code',
        'region_id': 'ref_region_id'
    })
    
    print(f"Merged dataset shape: {merged_df.shape}")
    print(f"Rows with reference data: {merged_df['ref_name_uz'].notna().sum()}")
    print(f"Rows without reference data: {merged_df['ref_name_uz'].isna().sum()}")
    
    # Save merged dataset
    merged_df.to_csv('merged_district_data.csv', index=False)
    print("Merged dataset saved to 'merged_district_data.csv'")
    
    # Show sample of merged data
    print("\nSample of merged data:")
    sample_cols = ['Year', 'Code', 'Klassifikator', 'Value', 'ref_name_uz', 'ref_name_en', 'ref_code', 'ref_region_id']
    print(merged_df[sample_cols].head(10).to_string(index=False))
    
    return merged_df

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