# District Matching Analysis Summary

## Overview
This analysis matches districts between two CSV files:
- **Main data file**: `part-00000-7eaec3f3-77ab-415c-ade1-cd47c9a52da1-c000.csv` (3,536 rows)
- **Reference file**: `reference_district_202508071730.csv` (174 rows)

## Key Findings

### Matching Statistics
- **Total main districts**: 221 unique districts
- **Total reference districts**: 174 unique districts
- **Successfully matched**: 192 districts (87% success rate)
- **Unmatched main districts**: 29 districts
- **Unmatched reference districts**: 4 districts
- **Average match score**: 0.951 (95.1% similarity)
- **Match score range**: 0.706 - 1.000

### Perfect Matches (Score = 1.000)
The following districts had perfect matches between the files:
- Amudaryo tumani
- Andijon tumani
- Angor tumani
- Arnasoy tumani
- Asaka tumani
- Baliqchi tumani
- Bandixon tumani
- Bekobod tumani
- Beruniy tumani
- Buxoro tumani
- Chimboy tumani
- Chust tumani
- Guliston tumani
- Karmana tumani
- Kegeyli tumani
- Kogon tumani
- Namangan tumani
- Nukus tumani
- Qarshi tumani
- Samarqand tumani
- Termiz tumani
- And many more...

### High-Quality Matches (Score ≥ 0.9)
Districts with very high similarity scores include:
- Shahrisabz tumani → Shakhrisabz tumani (0.952)
- Mirzo Ulug'bek tumani → Mirzo Ulug'bek tumani (0.929)
- O'rtachirchiq tumani → O'rtachirchiq tumani (0.923)
- Tuproqqal'a tumani → Tuproqqal'a tumani (0.909)
- Pastdarg'om tumani → Pastdarg'om tumani (0.909)

### Unmatched Main Districts (29 districts)
These districts from the main file could not be matched:
- **Regional/Republic level**: O'zbekiston Respublikasi, Qoraqalpog'iston Respublikasi
- **Province level**: Andijon viloyati, Buxoro viloyati, Farg'ona viloyati, etc.
- **Cities**: G'ozg'on shahri, Jizzax shahri, Marg'ilon shahri, Navoiy shahri, etc.
- **Special districts**: Kattaqo'rg'on shahar, O'zbekiston tumani, Sirg'ali tumani

### Unmatched Reference Districts (4 districts)
These districts from the reference file were not found in the main data:
- Kattakurgan tumani
- Sergeli tumani
- Taxtako'pир tumani
- Yangi Namangan tumani

## Data Quality Insights

### Naming Patterns
1. **District suffixes**: Most districts end with "tumani" (district)
2. **City suffixes**: Cities end with "shahri" (city)
3. **Province suffixes**: Provinces end with "viloyati" (province)
4. **Republic level**: "Respublikasi" (Republic)

### Common Variations
- **Apostrophe differences**: O' vs O' (Unicode variations)
- **Spacing differences**: Extra spaces in names
- **Case variations**: Different capitalization
- **Suffix variations**: "tumani" vs "district"

## Merged Dataset Results
- **Total rows**: 3,536
- **Rows with reference data**: 3,072 (87%)
- **Rows without reference data**: 464 (13%)
- **Additional columns added**: 5 reference columns (ref_name_uz, ref_name_en, ref_name_ru, ref_code, ref_region_id)

## Recommendations

### For Data Integration
1. **Use the merged dataset**: `merged_district_data.csv` contains the matched data
2. **Review unmatched districts**: Consider manual review of the 29 unmatched main districts
3. **Standardize naming**: Implement consistent naming conventions for future data

### For Data Quality
1. **Address naming inconsistencies**: Standardize district name formats
2. **Add missing districts**: Consider adding the 4 unmatched reference districts to main data
3. **Validate regional data**: Ensure province and republic level data is properly categorized

### For Analysis
1. **Use match scores**: Consider match quality when analyzing data
2. **Flag low-confidence matches**: Review matches with scores below 0.8
3. **Monitor data updates**: Re-run matching when new data is added

## Files Generated
1. **`district_matching_results.csv`**: Detailed matching results with scores
2. **`merged_district_data.csv`**: Complete merged dataset with reference data
3. **`simple_district_analysis.py`**: Analysis script for future use

## Technical Details
- **Matching algorithm**: Fuzzy string matching using SequenceMatcher
- **Threshold**: 0.7 minimum similarity score
- **Cleaning process**: Removed common suffixes and standardized names
- **Encoding**: UTF-8 to handle Uzbek characters properly

This analysis provides a solid foundation for integrating the two datasets with high confidence in the matching quality.