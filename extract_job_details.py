#!/usr/bin/env python3
"""
Job Details Extractor CLI
Command-line interface for extracting job details from Avature ATS URLs
"""

import argparse
import logging
import time
import sys
from pathlib import Path
from typing import List, Dict

# Add scraper directory to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'scraper'))

from scraper.job_details_extractor import AvatureJobDetailsExtractor
from scraper.url_processor import URLProcessor
from scraper.output_manager import OutputManager
from scraper.retry_manager import RetryManager


def setup_logging(verbose: bool = False, log_file: str = None):
    """Setup logging configuration"""
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Create formatters
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Setup root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # Suppress noisy third-party loggers
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)


def validate_input_file(input_file: str) -> bool:
    """Validate input file exists and is readable"""
    if not Path(input_file).exists():
        print(f"❌ Error: Input file not found: {input_file}")
        return False
    
    if not Path(input_file).is_file():
        print(f"❌ Error: Input path is not a file: {input_file}")
        return False
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            f.read(1)  # Try to read first character
        return True
    except Exception as e:
        print(f"❌ Error: Cannot read input file: {e}")
        return False


def print_extraction_progress(current: int, total: int, jobs_extracted: int, failures: int):
    """Print progress information"""
    percentage = (current / total * 100) if total > 0 else 0
    print(f"Progress: {current}/{total} ({percentage:.1f}%) | "
          f"Extracted: {jobs_extracted} | Failed: {failures}")


def main():
    parser = argparse.ArgumentParser(
        description='Extract job details from Avature ATS URLs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract from text file with URLs
  python extract_job_details.py --input job_urls.txt --output company_jobs

  # Extract with custom settings
  python extract_job_details.py --input urls.jsonl --max-workers 3 --timeout 30 --company "Bloomberg"

  # Process retry file
  python extract_job_details.py --input retries/retryable_company_20260208_123456.jsonl --max-workers 2

  # Verbose output with logging
  python extract_job_details.py --input urls.txt --output jobs --verbose --log-file extraction.log

Supported Input Formats:
  - Plain text (.txt): One URL per line
  - JSONL (.jsonl): One JSON object per line with 'url' field
  - JSON (.json): Array of URLs or objects, or retry file format
  - Retry files: Generated from previous extraction failures
        """
    )
    
    # Required arguments
    parser.add_argument('--input', '-i', required=True,
                      help='Input file with job URLs (txt, jsonl, or json)')
    
    # Output configuration
    parser.add_argument('--output', '-o', default='job_details',
                      help='Output file prefix (default: job_details)')
    parser.add_argument('--output-dir', default='.',
                      help='Output directory (default: current directory)')
    
    # Extraction configuration
    parser.add_argument('--max-workers', type=int, default=5,
                      help='Maximum concurrent workers (default: 5)')
    parser.add_argument('--timeout', type=int, default=25,
                      help='Request timeout in seconds (default: 25)')
    parser.add_argument('--max-retries', type=int, default=3,
                      help='Maximum retry attempts per URL (default: 3)')
    parser.add_argument('--delay', type=float, default=0.2,
                      help='Delay between requests in seconds (default: 0.2)')
    
    # Filtering options
    parser.add_argument('--company', 
                      help='Filter URLs for specific company')
    parser.add_argument('--limit', type=int,
                      help='Limit number of URLs to process (for testing)')
    
    # Logging and output options
    parser.add_argument('--verbose', '-v', action='store_true',
                      help='Enable verbose logging')
    parser.add_argument('--log-file',
                      help='Save detailed logs to file')
    parser.add_argument('--quiet', '-q', action='store_true',
                      help='Suppress progress output')
    parser.add_argument('--no-retries', action='store_true',
                      help='Do not generate retry files for failures')
    
    # Advanced options
    parser.add_argument('--check-retry-file', action='store_true',
                      help='Check if retry file is ready for processing')
    parser.add_argument('--stats-only', action='store_true',
                      help='Show URL statistics without extracting')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose, args.log_file)
    logger = logging.getLogger(__name__)
    
    print("🚀 Avature Job Details Extractor")
    print("=" * 50)
    
    # Validate input file
    if not validate_input_file(args.input):
        return 1
    
    try:
        # Initialize components
        url_processor = URLProcessor()
        output_manager = OutputManager(args.output_dir, create_subdirs=True)
        retry_manager = RetryManager(output_manager.retries_dir)
        
        # Check if this is a retry file check
        if args.check_retry_file:
            try:
                readiness = retry_manager.check_retry_readiness(args.input)
                if readiness['ready']:
                    print(f"✅ Retry file is ready for processing")
                    print(f"   Items: {readiness.get('total_items', 'unknown')}")
                    print(f"   Type: {readiness.get('retry_type', 'unknown')}")
                else:
                    print(f"⏰ Retry file not ready: {readiness['reason']}")
                    if 'next_retry_time' in readiness:
                        print(f"   Next retry time: {readiness['next_retry_time']}")
                return 0
            except Exception as e:
                print(f"❌ Error checking retry file: {e}")
                return 1
        
        # Process input URLs
        print(f"📁 Processing input file: {args.input}")
        start_time = time.time()
        
        urls = url_processor.process_input_file(args.input, args.company)
        
        if not urls:
            print("❌ No valid URLs found in input file")
            return 1
        
        # Apply limit if specified
        if args.limit and args.limit < len(urls):
            urls = urls[:args.limit]
            print(f"🔢 Limited to first {args.limit} URLs for testing")
        
        # Show URL statistics
        url_stats = url_processor.get_url_statistics(urls)
        print(f"\n📊 URL Statistics:")
        print(f"   Total URLs: {url_stats['total_urls']}")
        print(f"   Unique URLs: {url_stats['unique_urls']}")
        print(f"   Unique companies: {url_stats['unique_companies']}")
        print(f"   Companies: {', '.join(list(url_stats['by_company'].keys())[:5])}")
        if url_stats['duplicate_urls'] > 0:
            print(f"   ⚠️  Duplicate URLs: {url_stats['duplicate_urls']}")
        
        if args.stats_only:
            print("\n📈 Company breakdown:")
            for company, count in sorted(url_stats['by_company'].items(), key=lambda x: x[1], reverse=True):
                print(f"   {company}: {count} URLs")
            return 0
        
        # Initialize extractor
        extractor = AvatureJobDetailsExtractor(
            max_workers=args.max_workers,
            timeout=args.timeout,
            max_retries=args.max_retries
        )
        extractor.request_delay = args.delay
        
        print(f"\n⚙️  Extraction Configuration:")
        print(f"   Workers: {args.max_workers}")
        print(f"   Timeout: {args.timeout}s")
        print(f"   Max retries: {args.max_retries}")
        print(f"   Request delay: {args.delay}s")
        
        # Provide timing expectations
        estimated_time_minutes = len(urls) * args.timeout / args.max_workers / 60
        print(f"\n⏱️   Timing Estimates:")
        print(f"   Conservative estimate: {estimated_time_minutes:.1f} minutes")
        print(f"   First progress update: ~{min(25, max(10, len(urls) // 100)) * args.timeout / args.max_workers:.0f} seconds")
        if len(urls) > 1000:
            print(f"   ⚠️  Large batch detected - progress updates every 60 seconds minimum")
        
        # Extract job details
        print(f"\n🔍 Starting extraction of {len(urls)} job URLs...")
        print(f"💡 Tip: Progress updates every ~60 seconds or {min(25, max(10, len(urls) // 100))} completions")
        if not args.quiet:
            print(f"📊 Watch for: 📤 Submission → 📊 Progress → 🎉 Completion")
        
        extraction_start = time.time()
        
        # Prepare URLs for extraction
        job_urls = [url_data['url'] for url_data in urls]
        company_name = args.company or (urls[0]['company'] if urls else None)
        
        # Progress tracking
        if not args.quiet:
            print("📊 Extraction Progress:")
        
        jobs, failures = extractor.extract_from_urls(job_urls, company_name)
        
        extraction_duration = time.time() - extraction_start
        
        # Prepare metadata for output
        extraction_metadata = {
            'input_file': args.input,
            'duration_seconds': extraction_duration,
            'settings': {
                'max_workers': args.max_workers,
                'timeout': args.timeout,
                'max_retries': args.max_retries,
                'request_delay': args.delay
            },
            'avg_extraction_time': extraction_duration / len(job_urls) if job_urls else 0,
            'worker_utilization': min(args.max_workers, len(job_urls)),
        }
        
        # Generate outputs
        print(f"\n💾 Saving results...")
        
        files_created = output_manager.save_extraction_results(
            jobs, failures, extraction_metadata, args.output
        )
        
        # Handle retry files
        if not args.no_retries and failures:
            retry_stats = retry_manager.process_failures(failures, args.output)
            print(f"\n🔄 Retry File Statistics:")
            print(f"   Retryable failures: {retry_stats['retryable']}")
            print(f"   Rate-limited failures: {retry_stats['rate_limited']}")
            print(f"   Permanent failures: {retry_stats['permanent']}")
        
        # Print summary report
        print(f"\n" + output_manager.create_extraction_report(jobs, failures, extraction_metadata))
        
        # List generated files
        print(f"\n📄 Generated Files:")
        for file_type, file_path in files_created.items():
            print(f"   {file_type}: {file_path}")
        
        total_time = time.time() - start_time
        print(f"\n✅ Extraction completed in {total_time:.1f} seconds")
        
        # Return appropriate exit code
        success_rate = len(jobs) / (len(jobs) + len(failures)) if (len(jobs) + len(failures)) > 0 else 0
        if success_rate < 0.5:
            print(f"⚠️  Warning: Low success rate ({success_rate:.1%})")
            return 2
        elif failures:
            print(f"⚠️  Completed with {len(failures)} failures")
            return 1
        else:
            print(f"🎉 All {len(jobs)} jobs extracted successfully!")
            return 0
        
    except KeyboardInterrupt:
        print(f"\n❌ Extraction interrupted by user")
        return 130
    
    except Exception as e:
        logger.exception("Unexpected error during extraction")
        print(f"\n❌ Extraction failed: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())