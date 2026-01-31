#!/usr/bin/env python3
"""
Test the improved extraction logic on a small sample of jobs
"""

import sys
sys.path.append('/Users/ricardosimoes/workspace/Avature-ATS-Scraper')

from hybrid_scraper import AvatureMultiStrategyScraper
import json

def test_extraction():
    """Test extraction improvements on Bloomberg and UCLA Health"""
    
    companies = [
        ("Bloomberg", "https://bloomberg.avature.net/careers"),
        ("UCLA Health", "https://uclahealth.avature.net/careers"),
    ]
    
    for company_name, base_url in companies:
        print(f"\n{'='*50}")
        print(f"Testing {company_name}")
        print(f"{'='*50}")
        
        scraper = AvatureMultiStrategyScraper(
            company_name=company_name,
            base_url=base_url,
            max_workers=2  # Reduced for testing
        )
        
        # Get first 3 job URLs from sitemap for testing
        sitemap_url = f"{base_url}/sitemap.xml"
        job_urls = scraper._get_job_urls_from_sitemap(sitemap_url)
        
        if job_urls:
            print(f"Found {len(job_urls)} total jobs, testing first 3...")
            
            test_urls = job_urls[:3]
            
            for i, url in enumerate(test_urls, 1):
                print(f"\n--- Testing Job {i}: {url} ---")
                
                result = scraper._fetch_job_detail_with_retry(url, 'sitemap')
                
                if result and hasattr(result, 'title'):
                    print(f"✓ Title: {result.title}")
                    print(f"✓ Location: {result.location}")
                    print(f"✓ Department: {result.department}")
                    print(f"✓ Employment Type: {result.employment_type}")
                    print(f"✓ Date Posted: {result.date_posted}")
                    print(f"✓ Application URL: {result.application_url}")
                    print(f"✓ Description Length: {len(result.description or '')}")
                    
                    # Check for common issues
                    issues = []
                    if not result.title or 'Home Page' in result.title:
                        issues.append("❌ Invalid title")
                    if not result.location:
                        issues.append("❌ Missing location")
                    if not result.application_url:
                        issues.append("❌ Missing application URL")
                    if not result.description or len(result.description) < 100:
                        issues.append("❌ Poor description")
                    
                    if issues:
                        print("Issues found:")
                        for issue in issues:
                            print(f"  {issue}")
                    else:
                        print("✓ All extraction looks good!")
                        
                else:
                    print(f"❌ Failed to extract job: {result}")
        else:
            print(f"❌ No jobs found in sitemap for {company_name}")

if __name__ == "__main__":
    test_extraction()