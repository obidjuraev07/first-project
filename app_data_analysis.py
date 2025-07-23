#!/usr/bin/env python3
"""
App Data Analysis Script
Analyzes the provided app/website dataset to generate insights and statistics.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
import numpy as np

def load_and_clean_data(filename='app_data.csv'):
    """Load and clean the app data."""
    df = pd.read_csv(filename)
    
    # Fill empty app_id and app_name with NaN for better handling
    df['app_id'] = df['app_id'].replace('', np.nan)
    df['app_name'] = df['app_name'].replace('', np.nan)
    
    # Extract domain from URL for better analysis
    df['domain'] = df['url'].str.extract(r'([^.]+\.[^.]+)$')
    
    return df

def analyze_categories(df):
    """Analyze category distribution."""
    print("=== CATEGORY ANALYSIS ===")
    category_counts = df['category'].value_counts()
    print("\nCategory Distribution:")
    for category, count in category_counts.items():
        percentage = (count / len(df)) * 100
        print(f"{category:20}: {count:4} ({percentage:5.1f}%)")
    
    return category_counts

def analyze_apps(df):
    """Analyze app distribution."""
    print("\n=== APP ANALYSIS ===")
    
    # Count apps with IDs
    apps_with_ids = df[df['app_id'].notna()]
    unique_apps = apps_with_ids['app_name'].value_counts()
    
    print(f"\nTotal entries: {len(df)}")
    print(f"Entries with app_id: {len(apps_with_ids)}")
    print(f"Entries without app_id: {len(df) - len(apps_with_ids)}")
    print(f"Unique named apps: {len(unique_apps)}")
    
    print("\nTop 10 Apps by URL count:")
    for app, count in unique_apps.head(10).items():
        app_id = apps_with_ids[apps_with_ids['app_name'] == app]['app_id'].iloc[0]
        print(f"{app:20} (ID: {app_id:4}): {count:3} URLs")
    
    return unique_apps

def analyze_domains(df):
    """Analyze domain patterns."""
    print("\n=== DOMAIN ANALYSIS ===")
    
    # Top level domains
    df['tld'] = df['url'].str.extract(r'\.([a-z]+)$')
    tld_counts = df['tld'].value_counts()
    
    print("\nTop 10 TLDs:")
    for tld, count in tld_counts.head(10).items():
        percentage = (count / len(df)) * 100
        print(f".{tld:10}: {count:4} ({percentage:5.1f}%)")
    
    # Uzbekistan domains
    uz_domains = df[df['url'].str.contains('\.uz$', na=False)]
    print(f"\nUzbek domains (.uz): {len(uz_domains)}")
    
    # Russian domains
    ru_domains = df[df['url'].str.contains('\.ru$', na=False)]
    print(f"Russian domains (.ru): {len(ru_domains)}")
    
    return tld_counts

def analyze_uzbekistan_focus(df):
    """Analyze Uzbekistan-specific data."""
    print("\n=== UZBEKISTAN FOCUS ===")
    
    # Uzbek domains
    uz_domains = df[df['url'].str.contains('\.uz$', na=False)]
    
    if len(uz_domains) > 0:
        print(f"\nUzbek domains by category:")
        uz_categories = uz_domains['category'].value_counts()
        for category, count in uz_categories.items():
            percentage = (count / len(uz_domains)) * 100
            print(f"{category:20}: {count:3} ({percentage:5.1f}%)")
        
        print(f"\nTop Uzbek domains:")
        for _, row in uz_domains.head(10).iterrows():
            app_name = row['app_name'] if pd.notna(row['app_name']) else 'Unknown'
            print(f"{row['url']:25} - {app_name:15} ({row['category']})")

def generate_visualizations(df, category_counts, save_plots=False):
    """Generate visualization plots."""
    print("\n=== GENERATING VISUALIZATIONS ===")
    
    # Set up the plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('App Data Analysis Dashboard', fontsize=16, fontweight='bold')
    
    # 1. Category distribution pie chart
    axes[0, 0].pie(category_counts.values, labels=category_counts.index, autopct='%1.1f%%', startangle=90)
    axes[0, 0].set_title('Distribution by Category')
    
    # 2. Top domains bar chart
    tld_counts = df['url'].str.extract(r'\.([a-z]+)$')[0].value_counts().head(10)
    axes[0, 1].bar(range(len(tld_counts)), tld_counts.values)
    axes[0, 1].set_xticks(range(len(tld_counts)))
    axes[0, 1].set_xticklabels([f'.{tld}' for tld in tld_counts.index], rotation=45)
    axes[0, 1].set_title('Top 10 TLDs')
    axes[0, 1].set_ylabel('Count')
    
    # 3. Apps with most URLs
    apps_with_ids = df[df['app_id'].notna()]
    if len(apps_with_ids) > 0:
        top_apps = apps_with_ids['app_name'].value_counts().head(8)
        axes[1, 0].barh(range(len(top_apps)), top_apps.values)
        axes[1, 0].set_yticks(range(len(top_apps)))
        axes[1, 0].set_yticklabels(top_apps.index)
        axes[1, 0].set_title('Apps with Most URLs')
        axes[1, 0].set_xlabel('Number of URLs')
    
    # 4. Regional distribution (uz, ru, com domains)
    regional_data = {
        '.uz (Uzbekistan)': len(df[df['url'].str.contains('\.uz$', na=False)]),
        '.ru (Russia)': len(df[df['url'].str.contains('\.ru$', na=False)]),
        '.com (International)': len(df[df['url'].str.contains('\.com$', na=False)]),
        'Other': len(df) - len(df[df['url'].str.contains('\.(uz|ru|com)$', na=False)])
    }
    
    axes[1, 1].bar(regional_data.keys(), regional_data.values())
    axes[1, 1].set_title('Regional Distribution')
    axes[1, 1].set_ylabel('Count')
    axes[1, 1].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    
    if save_plots:
        plt.savefig('app_data_analysis.png', dpi=300, bbox_inches='tight')
        print("Visualization saved as 'app_data_analysis.png'")
    
    plt.show()

def main():
    """Main analysis function."""
    print("App Data Analysis")
    print("=" * 50)
    
    try:
        # Load data
        df = load_and_clean_data()
        print(f"Loaded {len(df)} records from app_data.csv")
        
        # Perform analyses
        category_counts = analyze_categories(df)
        unique_apps = analyze_apps(df)
        tld_counts = analyze_domains(df)
        analyze_uzbekistan_focus(df)
        
        # Generate summary statistics
        print("\n=== SUMMARY STATISTICS ===")
        print(f"Total URLs: {len(df)}")
        print(f"Unique categories: {df['category'].nunique()}")
        print(f"URLs with app_id: {df['app_id'].notna().sum()}")
        print(f"URLs without app_id: {df['app_id'].isna().sum()}")
        print(f"Most common category: {category_counts.index[0]} ({category_counts.iloc[0]} URLs)")
        
        # Generate visualizations
        try:
            generate_visualizations(df, category_counts, save_plots=True)
        except Exception as e:
            print(f"Could not generate visualizations: {e}")
            print("This might be due to missing matplotlib/seaborn. Install with: pip install matplotlib seaborn")
        
    except FileNotFoundError:
        print("Error: app_data.csv not found. Please ensure the file exists.")
    except Exception as e:
        print(f"Error during analysis: {e}")

if __name__ == "__main__":
    main()