"""
Example Usage and Testing for Job Details Extractor
Demonstrates various ways to use the extractor system
"""

import json
import logging
from pathlib import Path
from scraper.job_details_extractor import AvatureJobDetailsExtractor
from scraper.url_processor import URLProcessor
from scraper.output_manager import OutputManager
from scraper.retry_manager import RetryManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_sample_urls_file():
    """Create a sample URLs file for testing"""
    sample_urls = [
        "https://advocateaurorahealth.avature.net/careers/JobDetail/Aurora-IL-United-States-Project-Manager-Information-Technology/7856",
        "https://bloomberg.avature.net/careers/JobDetail/New-York-New-York-United-States-Software-Engineer-Terminal/12345",
        "https://ucla.avature.net/careers/JobDetail/Los-Angeles-CA-United-States-Research-Scientist/9876"
    ]
    
    # Create plain text version
    with open('sample_urls.txt', 'w') as f:
        for url in sample_urls:
            f.write(url + '\n')
    
    # Create JSONL version
    with open('sample_urls.jsonl', 'w') as f:
        for i, url in enumerate(sample_urls):
            job_data = {
                'url': url,
                'company': url.split('.')[0].split('://')[-1],
                'job_id': str(7856 + i),
                'discovered_at': '2026-02-08T12:00:00Z'
            }
            f.write(json.dumps(job_data) + '\n')
    
    logger.info("Created sample input files: sample_urls.txt, sample_urls.jsonl")


def example_basic_usage():
    """Example 1: Basic usage with URL list"""
    print("\n" + "="*60)
    print("EXAMPLE 1: Basic Extraction from URL List")
    print("="*60)
    
    # Sample URLs for testing
    job_urls = [
        "https://advocateaurorahealth.avature.net/careers/JobDetail/Aurora-IL-United-States-Project-Manager-Information-Technology/7856"
    ]
    
    # Initialize extractor
    extractor = AvatureJobDetailsExtractor(max_workers=2, timeout=30)
    
    # Extract job details
    jobs, failures = extractor.extract_from_urls(job_urls, "advocateaurorahealth")
    
    # Print results
    print(f"Extracted {len(jobs)} jobs, {len(failures)} failures")
    
    for job in jobs:
        print(f"\nJob: {job.title}")
        print(f"Company: {job.company}")
        print(f"Location: {job.location}")
        print(f"Description length: {len(job.description) if job.description else 0} chars")


def example_file_processing():
    """Example 2: Processing input files with full pipeline"""
    print("\n" + "="*60)
    print("EXAMPLE 2: File Processing Pipeline")
    print("="*60)
    
    # Create sample files if they don't exist
    if not Path('sample_urls.txt').exists():
        create_sample_urls_file()
    
    # Initialize components
    url_processor = URLProcessor()
    output_manager = OutputManager(create_subdirs=True)
    retry_manager = RetryManager()
    
    # Process input file
    urls = url_processor.process_input_file('sample_urls.txt')
    print(f"Processed {len(urls)} URLs from file")
    
    # Show statistics
    stats = url_processor.get_url_statistics(urls)
    print(f"Companies found: {list(stats['by_company'].keys())}")
    
    # Extract job details
    extractor = AvatureJobDetailsExtractor(max_workers=2, timeout=30)
    job_urls = [url_data['url'] for url_data in urls]
    jobs, failures = extractor.extract_from_urls(job_urls)
    
    # Save results
    extraction_metadata = {
        'input_file': 'sample_urls.txt',
        'duration_seconds': 60,
        'settings': {'max_workers': 2, 'timeout': 30}
    }
    
    files_created = output_manager.save_extraction_results(
        jobs, failures, extraction_metadata, "example_extraction"
    )
    
    print(f"\nFiles created:")
    for file_type, file_path in files_created.items():
        print(f"  {file_type}: {file_path}")
    
    # Process retry files if there are failures
    if failures:
        retry_stats = retry_manager.process_failures(failures, "example")
        print(f"\nRetry statistics: {retry_stats}")


def example_retry_handling():
    """Example 3: Retry file processing"""
    print("\n" + "="*60)
    print("EXAMPLE 3: Retry File Handling")
    print("="*60)
    
    retry_manager = RetryManager()
    
    # Get retry statistics
    stats = retry_manager.get_retry_statistics()
    print(f"Retry files in directory: {stats['total_retry_files']}")
    
    if stats['files']:
        print("\nAvailable retry files:")
        for file_info in stats['files'][:3]:  # Show first 3
            print(f"  {file_info['filename']}: {file_info['total_items']} items "
                  f"({'ready' if file_info['ready_for_retry'] else 'not ready'})")


def example_output_analysis():
    """Example 4: Analyzing extraction outputs"""
    print("\n" + "="*60)
    print("EXAMPLE 4: Output Analysis")
    print("="*60)
    
    # Find recent job details files
    job_details_dir = Path('job_details')
    if job_details_dir.exists():
        jsonl_files = list(job_details_dir.glob('*.jsonl'))
        
        if jsonl_files:
            latest_file = max(jsonl_files, key=lambda x: x.stat().st_mtime)
            print(f"Analyzing latest file: {latest_file}")
            
            # Read and analyze jobs
            jobs = []
            with open(latest_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        jobs.append(json.loads(line))
            
            print(f"\nAnalysis of {len(jobs)} jobs:")
            
            # Field completion rates
            fields = ['title', 'location', 'description', 'date_posted', 'department', 'application_url']
            for field in fields:
                count = sum(1 for job in jobs if job.get(field) and str(job[field]).strip())
                rate = count / len(jobs) * 100 if jobs else 0
                print(f"  {field}: {count}/{len(jobs)} ({rate:.1f}%)")
            
            # Company breakdown
            companies = {}
            for job in jobs:
                company = job.get('company', 'unknown')
                companies[company] = companies.get(company, 0) + 1
            
            print(f"\nCompanies:")
            for company, count in companies.items():
                print(f"  {company}: {count} jobs")
        
        else:
            print("No job details files found")
    else:
        print("Job details directory not found")


def main():
    """Run all examples"""
    print("🚀 Job Details Extractor Examples")
    print("This will demonstrate various usage patterns")
    print("Note: Some examples may take time due to actual HTTP requests")
    
    try:
        # Create sample files for testing
        create_sample_urls_file()
        
        # Run examples (comment out expensive ones for quick testing)
        # example_basic_usage()        # Makes actual HTTP requests
        example_file_processing()     # Makes actual HTTP requests  
        example_retry_handling()      # Just file operations
        example_output_analysis()     # Just file operations
        
        print("\n✅ Examples completed!")
        print("\nTo run the full extractor:")
        print("  python extract_job_details.py --input sample_urls.txt --output my_extraction")
        
    except Exception as e:
        logger.error(f"Example failed: {e}")
        raise


if __name__ == '__main__':
    main()