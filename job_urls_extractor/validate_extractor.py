"""
Job URL Extractor Validation Script
==================================

Test script to validate the job URL extractor against known career pages
with expected job counts.

Usage:
    python validate_extractor.py <career_url> <expected_count>
    
Example:
    python validate_extractor.py https://bloomberg.avature.net/careers 1200
"""

import sys
import tempfile
from pathlib import Path
from job_url_extractor import AvatureMultiStrategyScraper
import requests
from dataclasses import dataclass
from typing import Optional, List
from enum import Enum

class FailureCategory(Enum):
    LEGITIMATE_NO_JOBS = "legitimate_no_jobs"  # Site exists but has no jobs
    SITE_NOT_FOUND = "site_not_found"          # 404, site doesn't exist
    SITE_MOVED = "site_moved"                  # Redirects to different domain
    ACCESS_DENIED = "access_denied"            # 403, 406, access restrictions
    TECHNICAL_ERROR = "technical_error"        # Network, parsing, or other technical issues
    UNKNOWN = "unknown"                        # Unclassified failure

@dataclass
class ValidationResult:
    company: str
    url: str
    success: bool
    jobs_found: int
    category: FailureCategory
    error_message: Optional[str] = None
    http_status: Optional[int] = None
    redirected_url: Optional[str] = None
    strategy_used: Optional[str] = None

def categorize_failure(url: str, error_msg: str, http_status: Optional[int], redirected_url: Optional[str]) -> FailureCategory:
    """
    Automatically categorize failure types based on response patterns
    """
    if http_status == 404:
        return FailureCategory.SITE_NOT_FOUND
    
    if http_status in [403, 406]:
        return FailureCategory.ACCESS_DENIED
    
    if redirected_url and redirected_url != url:
        # Check if redirected to completely different domain
        from urllib.parse import urlparse
        orig_domain = urlparse(url).netloc
        redir_domain = urlparse(redirected_url).netloc
        if orig_domain != redir_domain and 'avature.net' not in redir_domain:
            return FailureCategory.SITE_MOVED
    
    if "No job listings found" in error_msg or "0 jobs" in error_msg:
        return FailureCategory.LEGITIMATE_NO_JOBS
    
    if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
        return FailureCategory.TECHNICAL_ERROR
    
    return FailureCategory.UNKNOWN

def validate_extraction(career_url: str, expected_count: int) -> ValidationResult:
    """
    Test job URL extraction and categorize failure types.
    
    Args:
        career_url: The career page URL to test
        expected_count: Expected number of job URLs to extract
        
    Returns:
        ValidationResult with detailed categorization
    """
    from urllib.parse import urlparse
    
    print(f"Testing URL extraction from: {career_url}")
    print(f"Expected job count: {expected_count}")
    print("-" * 60)
    
    # Extract company name from URL
    parsed = urlparse(career_url)
    company_name = parsed.netloc.split('.')[0] if parsed.netloc else "unknown"
    
    try:
        # Create extractor instance with conservative settings
        scraper = AvatureMultiStrategyScraper(
            company_name=company_name,
            base_url=career_url,
            max_workers=1
        )
        
        print(f"Company: {company_name}")
        print(f"Original URL: {career_url}")
        print(f"Resolved URL: {scraper.base_url}")
        print()
        
        # Extract job URLs
        job_urls = scraper.scrape_all_job_urls()
        actual_count = len(job_urls)
        
        print(f"✓ Extraction completed via {scraper.strategy_used}")
        print(f"Jobs found: {actual_count}")
        print(f"Expected: {expected_count}")
        
        # Determine success
        tolerance = 0
        success = abs(actual_count - expected_count) <= tolerance
        
        if success:
            print(f"✅ PASSED: Job count within tolerance (±{tolerance})")
            category = FailureCategory.LEGITIMATE_NO_JOBS if actual_count == 0 else None
        else:
            difference = actual_count - expected_count
            print(f"❌ Count mismatch: {difference:+d} jobs")
            
            # Categorize based on results
            if actual_count == 0:
                if scraper.failures:
                    # Analyze failures to categorize
                    error_msgs = [f.error_message for f in scraper.failures]
                    http_statuses = [f.http_status for f in scraper.failures if f.http_status]
                    
                    if http_statuses:
                        category = categorize_failure(
                            career_url, 
                            '; '.join(error_msgs), 
                            http_statuses[0],
                            scraper.base_url if scraper.base_url != career_url else None
                        )
                    else:
                        category = FailureCategory.LEGITIMATE_NO_JOBS
                else:
                    category = FailureCategory.LEGITIMATE_NO_JOBS
            else:
                category = FailureCategory.UNKNOWN
        
        # Show sample URLs if any found
        if job_urls:
            print("\nSample job URLs:")
            for i, job_url in enumerate(job_urls[:3]):
                print(f"  {i+1}. {job_url}")
            if len(job_urls) > 3:
                print(f"  ... and {len(job_urls) - 3} more")
        
        return ValidationResult(
            company=company_name,
            url=career_url,
            success=success,
            jobs_found=actual_count,
            category=category if not success else FailureCategory.LEGITIMATE_NO_JOBS if actual_count == 0 else None,
            redirected_url=scraper.base_url if scraper.base_url != career_url else None,
            strategy_used=scraper.strategy_used
        )
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Network Error: {e}")
        
        # Try to get HTTP status from the exception
        http_status = None
        if hasattr(e, 'response') and e.response:
            http_status = e.response.status_code
        
        category = categorize_failure(career_url, str(e), http_status, None)
        
        return ValidationResult(
            company=company_name,
            url=career_url,
            success=False,
            jobs_found=0,
            category=category,
            error_message=str(e),
            http_status=http_status
        )
    
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        
        return ValidationResult(
            company=company_name,
            url=career_url,
            success=False,
            jobs_found=0,
            category=FailureCategory.TECHNICAL_ERROR,
            error_message=str(e)
        )

def run_validation_suite():
    """Run a suite of validation tests with known career pages"""
    test_cases = [
        # Format: (career_url, expected_count, description)
        ("https://bloomberg.avature.net/careers", 1100, "Bloomberg - Large financial company"),
        ("https://uclahealth.avature.net/careers", 800, "UCLA Health - Healthcare system"),
        ("https://ally.avature.net/careers", 200, "Ally Financial - Mid-size financial"),
        ("https://astellas.avature.net/careers", 400, "Astellas - Pharmaceutical company"),
    ]
    
    print("="*80)
    print("JOB URL EXTRACTOR VALIDATION SUITE")
    print("="*80)
    print(f"Running {len(test_cases)} validation tests...\n")
    
    results = []
    
    for i, (url, expected, description) in enumerate(test_cases, 1):
        print(f"Test {i}/{len(test_cases)}: {description}")
        print("-" * 80)
        
        result = validate_extraction(url, expected)
        results.append((description, result))
        
        print()
        
        # Add delay between tests to avoid rate limiting
        if i < len(test_cases):
            print("Waiting 5 seconds before next test...")
            import time
            time.sleep(5)
            print()
    
    # Summary
    print("="*80)
    print("VALIDATION RESULTS SUMMARY")
    print("="*80)
    
    passed = 0
    failed = 0
    categories = {}
    
    for description, result in results:
        if result.success:
            print(f"✅ PASSED: {description}")
            passed += 1
        else:
            print(f"❌ FAILED: {description} - {result.category.value}")
            failed += 1
            
        # Count categories
        cat = result.category.value if result.category else 'unknown'
        categories[cat] = categories.get(cat, 0) + 1
    
    print(f"\nTotal: {len(test_cases)} tests")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    
    print("\nFailure categories:")
    for cat, count in categories.items():
        print(f"  {cat}: {count}")
    
    if failed == 0:
        print("\n🎉 All tests passed!")
        return True
    else:
        print(f"\n⚠️  {failed} test(s) failed")
        return False

def batch_validate_companies(company_urls: List[str]) -> List[ValidationResult]:
    """Validate multiple companies and return categorized results"""
    results = []
    
    for i, url in enumerate(company_urls, 1):
        print(f"\n{'='*60}")
        print(f"Testing {i}/{len(company_urls)}: {url}")
        print(f"{'='*60}")
        
        result = validate_extraction(url, 0)  # Expect 0 for failing companies
        results.append(result)
        
        # Short delay to avoid rate limiting
        if i < len(company_urls):
            print("\nWaiting 2 seconds...")
            import time
            time.sleep(2)
    
    return results

def main():
    """Main execution function"""
    if len(sys.argv) == 1:
        # No arguments - run validation suite
        success = run_validation_suite()
        sys.exit(0 if success else 1)
    
    elif len(sys.argv) == 2 and sys.argv[1] == '--batch-validate':
        # Batch validate some failing companies for categorization
        failing_urls = [
            "https://cisco2.avature.net/careers",
            "https://ciscotrainingats.avature.net/careers", 
            "https://dell2.avature.net/careers",
            "https://deloittepng.avature.net/careers",
            "https://deltaflightattendants.avature.net/careers",
            "https://docsglobal.avature.net/careers",
            "https://diageocrm.avature.net/careers",
            "https://discovermgs.avature.net/careers",
            "https://dtccpontoon.avature.net/careers",
            "https://customerservice.avature.net/careers"
        ]
        
        print("Batch validating failing companies for categorization...")
        results = batch_validate_companies(failing_urls)
        
        # Summary by category
        print(f"\n{'='*60}")
        print("CATEGORIZATION SUMMARY")
        print(f"{'='*60}")
        
        categories = {}
        for result in results:
            cat = result.category.value if result.category else 'unknown'
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(result.company)
        
        for cat, companies in categories.items():
            print(f"\n{cat.upper().replace('_', ' ')} ({len(companies)}):")
            for company in companies:
                print(f"  - {company}")
        
        sys.exit(0)
    
    elif len(sys.argv) == 3:
        # Individual test
        try:
            career_url = sys.argv[1]
            expected_count = int(sys.argv[2])
        except ValueError:
            print("Error: Expected count must be an integer")
            print("Usage: python validate_extractor.py <career_url> <expected_count>")
            sys.exit(1)
        
        result = validate_extraction(career_url, expected_count)
        
        print(f"\nResult: {result.category.value if result.category else 'success'}")
        if result.error_message:
            print(f"Error: {result.error_message}")
        
        if result.success:
            print("\n🎉 Validation PASSED!")
            sys.exit(0)
        else:
            print("\n❌ Validation FAILED!")
            sys.exit(1)
    
    else:
        print("Usage:")
        print("  python validate_extractor.py                              # Run validation suite")
        print("  python validate_extractor.py --batch-validate             # Categorize failing companies")
        print("  python validate_extractor.py <career_url> <expected_count> # Test single URL")
        print()
        print("Examples:")
        print("  python validate_extractor.py")
        print("  python validate_extractor.py --batch-validate")
        print("  python validate_extractor.py https://bloomberg.avature.net/careers 1200")
        sys.exit(1)

if __name__ == "__main__":
    main()