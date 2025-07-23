#!/usr/bin/env python3
"""
Simple App Data Analysis Script
Analyzes the provided app/website dataset using only built-in Python libraries.
"""

import csv
from collections import Counter, defaultdict
import re

def load_data(filename='app_data.csv'):
    """Load the app data from CSV file."""
    data = []
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data

def clean_data(data):
    """Clean and process the data."""
    cleaned = []
    for row in data:
        # Skip rows with empty URLs
        if not row['url'].strip():
            continue
            
        # Extract domain from URL
        domain_match = re.search(r'([^./]+\.[^./]+)/?$', row['url'])
        domain = domain_match.group(1) if domain_match else row['url']
        
        cleaned_row = {
            'app_id': row['app_id'].strip() if row['app_id'].strip() else 'Unknown',
            'app_name': row['app_name'].strip() if row['app_name'].strip() else 'Unknown',
            'url': row['url'].strip(),
            'domain': domain,
            'category': row['category'].strip() if row['category'].strip() else 'uncategorized'
        }
        cleaned.append(cleaned_row)
    
    return cleaned

def analyze_categories(data):
    """Analyze category distribution."""
    categories = Counter(row['category'] for row in data)
    
    print("=== CATEGORY ANALYSIS ===")
    print(f"Total entries: {len(data)}")
    print(f"Unique categories: {len(categories)}")
    print("\nCategory distribution:")
    
    for category, count in categories.most_common():
        percentage = (count / len(data)) * 100
        print(f"  {category}: {count} ({percentage:.1f}%)")
    
    return categories

def analyze_apps(data):
    """Analyze app distribution."""
    apps = defaultdict(list)
    
    for row in data:
        if row['app_name'] != 'Unknown':
            apps[row['app_name']].append(row)
    
    print("\n=== APP ANALYSIS ===")
    print(f"Named apps: {len(apps)}")
    print(f"Unnamed entries: {sum(1 for row in data if row['app_name'] == 'Unknown')}")
    
    print("\nApps with multiple domains:")
    for app_name, entries in apps.items():
        if len(entries) > 1:
            domains = [entry['domain'] for entry in entries]
            print(f"  {app_name}: {len(domains)} domains ({', '.join(domains)})")
    
    return apps

def analyze_domains(data):
    """Analyze domain patterns."""
    domains = Counter(row['domain'] for row in data)
    tlds = Counter(row['domain'].split('.')[-1] for row in data if '.' in row['domain'])
    
    print("\n=== DOMAIN ANALYSIS ===")
    print(f"Unique domains: {len(domains)}")
    
    print("\nTop 10 domains:")
    for domain, count in domains.most_common(10):
        print(f"  {domain}: {count}")
    
    print("\nTop-level domains (TLDs):")
    for tld, count in tlds.most_common():
        percentage = (count / len(data)) * 100
        print(f"  .{tld}: {count} ({percentage:.1f}%)")
    
    return domains, tlds

def analyze_geographic_distribution(data):
    """Analyze geographic distribution based on TLDs."""
    country_tlds = {
        'uz': 'Uzbekistan',
        'ru': 'Russia',
        'cn': 'China',
        'com': 'International',
        'org': 'International',
        'net': 'International',
        'io': 'International'
    }
    
    geo_distribution = defaultdict(int)
    
    for row in data:
        domain = row['domain']
        if '.' in domain:
            tld = domain.split('.')[-1]
            country = country_tlds.get(tld, f'Other (.{tld})')
            geo_distribution[country] += 1
    
    print("\n=== GEOGRAPHIC DISTRIBUTION ===")
    total = sum(geo_distribution.values())
    
    for country, count in sorted(geo_distribution.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total) * 100
        print(f"  {country}: {count} ({percentage:.1f}%)")
    
    return geo_distribution

def find_duplicates(data):
    """Find duplicate entries."""
    url_counts = Counter(row['url'] for row in data)
    duplicates = {url: count for url, count in url_counts.items() if count > 1}
    
    if duplicates:
        print("\n=== DUPLICATE URLS ===")
        for url, count in duplicates.items():
            print(f"  {url}: appears {count} times")
            # Show the different app_names for this URL
            app_names = set(row['app_name'] for row in data if row['url'] == url)
            if len(app_names) > 1:
                print(f"    Different app names: {', '.join(app_names)}")
    else:
        print("\n=== NO DUPLICATE URLS FOUND ===")
    
    return duplicates

def main():
    """Main analysis function."""
    print("Loading and analyzing app data...")
    
    try:
        raw_data = load_data()
        cleaned_data = clean_data(raw_data)
        
        print(f"Loaded {len(raw_data)} raw entries, cleaned to {len(cleaned_data)} valid entries\n")
        
        # Run all analyses
        categories = analyze_categories(cleaned_data)
        apps = analyze_apps(cleaned_data)
        domains, tlds = analyze_domains(cleaned_data)
        geo_dist = analyze_geographic_distribution(cleaned_data)
        duplicates = find_duplicates(cleaned_data)
        
        # Summary insights
        print("\n=== KEY INSIGHTS ===")
        most_common_category = categories.most_common(1)[0]
        print(f"• Most common category: {most_common_category[0]} ({most_common_category[1]} entries)")
        
        uzbek_entries = sum(1 for row in cleaned_data if row['domain'].endswith('.uz'))
        print(f"• Uzbek domains (.uz): {uzbek_entries} entries ({(uzbek_entries/len(cleaned_data)*100):.1f}%)")
        
        named_apps = len([row for row in cleaned_data if row['app_name'] != 'Unknown'])
        print(f"• Named applications: {named_apps} out of {len(cleaned_data)} entries ({(named_apps/len(cleaned_data)*100):.1f}%)")
        
    except FileNotFoundError:
        print("Error: app_data.csv file not found!")
    except Exception as e:
        print(f"Error during analysis: {e}")

if __name__ == "__main__":
    main()