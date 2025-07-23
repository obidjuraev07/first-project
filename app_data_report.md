# App/Website Data Analysis Report

## Dataset Overview
- **Total entries**: 349 websites/apps
- **Unique domains**: 342
- **Named applications**: 52 (14.9%)
- **Unnamed entries**: 297 (85.1%)

## Category Distribution

The dataset contains 11 different categories with the following distribution:

| Category | Count | Percentage |
|----------|-------|------------|
| Other | 110 | 31.5% |
| Information | 65 | 18.6% |
| Entertainment | 52 | 14.9% |
| Social Network | 33 | 9.5% |
| Finance | 26 | 7.4% |
| Retail | 25 | 7.2% |
| Search | 21 | 6.0% |
| Education | 12 | 3.4% |
| Traveling | 3 | 0.9% |
| Food | 1 | 0.3% |
| Government | 1 | 0.3% |

## Geographic Distribution

Based on top-level domains (TLDs):

| Region/Country | Count | Percentage |
|----------------|-------|------------|
| International (.com, .org, .net, .io) | 167 | 47.9% |
| Russia (.ru) | 63 | 18.1% |
| Uzbekistan (.uz) | 54 | 15.5% |
| Montenegro (.me) | 17 | 4.9% |
| China (.cn) | 5 | 1.4% |
| Others | 43 | 12.1% |

## Key Applications with Multiple Domains

Several major applications/services operate across multiple domains:

- **Google_Common**: 3 domains (google.com, google.cn, google.ru)
- **Amazon**: 6 domains (various subdomains and CDNs)
- **Instagram**: 5 domains (including alternative access domains)
- **Uzum**: 3 domains (main site, CDP, and delivery service)
- **YouTube**: 3 domains (main site and related services)
- **OLX**: 3 domains (different regional versions)

## Data Quality Issues

### Duplicate URLs
- `ddinstagram.com`: appears 4 times
- `puzzlejoytime.com`: appears 3 times
- `google.com`: appears 2 times (with different app names)
- `ikea.com`: appears 2 times

### Missing Information
- 85.1% of entries lack app_name information
- Some entries have empty app_id fields

## Regional Focus

The dataset shows a strong focus on:
1. **Uzbekistan** (15.5% of domains) - indicating local market focus
2. **Russia** (18.1% of domains) - significant Russian web presence
3. **International services** (47.9%) - global platforms and services

## Popular Categories Analysis

1. **"Other" category dominance** (31.5%) suggests need for better categorization
2. **Information websites** are well-represented (18.6%)
3. **Entertainment** platforms make up a significant portion (14.9%)
4. **Financial services** show strong presence (7.4%), particularly Uzbek fintech

## Recommendations

1. **Data Cleaning**: Remove duplicate URLs and standardize app names
2. **Category Refinement**: Break down the "Other" category into more specific subcategories
3. **App Name Completion**: Research and add missing app names for better analysis
4. **URL Standardization**: Ensure consistent URL formatting
5. **Regional Analysis**: Further investigate the high representation of Uzbek and Russian domains

## Technical Notes

- Analysis performed using Python 3 with built-in libraries
- Domain extraction used regex pattern matching
- Geographic classification based on TLD analysis
- Data source: 349 entries from provided CSV dataset