#!/usr/bin/env python3
"""
Avature Job Board Finder

Discovers and validates Avature career sites from tenant names.
Generates candidate URLs and validates them using comprehensive Avature detection.
"""

import sys
import argparse
import logging
import requests
import time
import random
import json
import threading
from datetime import datetime
from typing import List, Set, Tuple, Dict
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import the URL validator
from url_validator import AvatureURLValidator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_tenants(file_path: str) -> List[str]:
    """Load tenant names from file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            tenants = [line.strip() for line in f if line.strip()]
        
        logger.info(f"Loaded {len(tenants):,} tenants from {file_path}")
        return tenants
    
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return []
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return []


class AvatureJobBoardFinder:
    """
    Discovers and validates Avature career sites from tenant names.
    Uses the AvatureURLValidator for comprehensive validation.
    """
    
    def __init__(self, output_dir: str = '.'):
        self.validator = AvatureURLValidator()
        
        # Create timestamped output directory in script location
        script_dir = Path(__file__).parent
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_dir = script_dir / f"results_{timestamp}"
        self.output_dir.mkdir(exist_ok=True)
        
        # Create timestamped output files in the results folder
        self.success_file = self.output_dir / "success_urls.txt"
        self.failure_file = self.output_dir / "failed_tenants.txt"
        self.redirect_file = self.output_dir / "redirected_tenants.txt"
        
        # File lock for thread-safe file operations
        self._file_lock = threading.Lock()
        
        # Initialize output files with headers
        self._init_output_files()
    
    def get_candidate_patterns(self, tenant: str) -> List[str]:
        """
        Get ordered list of candidate URL patterns for a tenant.
        Patterns are ordered by likelihood of success.
        """
        base_url = f"https://{tenant}.avature.net"
        
        # Ordered by likelihood (most common patterns first)
        return [
            f"{base_url}/careers",         # Most common (20 occurrences)
            f"{base_url}/Careers",         # Capital C variant (2 occurrences)
            f"{base_url}/talent",          # AbbVie pattern (9 occurrences)
            f"{base_url}/jobs",            # Alternative (6 occurrences)
            f"{base_url}/SearchJobs",      # Direct to search
            
            # Localized patterns (premium.avature.net style)
            f"{base_url}/en_US/jobs",      # English US (16,657 occurrences)
            f"{base_url}/en_US/careers",   # English US careers
            f"{base_url}/fr_CA/jobs",      # French Canada (16,606 occurrences)
            f"{base_url}/fr_CA/careers",   # French Canada careers
            f"{base_url}/en/careers",      # English
            f"{base_url}/de/careers",      # German
            f"{base_url}/es/careers",      # Spanish
            f"{base_url}/fr/careers",      # French
        ]
    
    def find_valid_url_for_tenant(self, tenant: str) -> Dict:
        """
        Find the first valid URL for a tenant by testing patterns in order.
        Uses the URL validator for comprehensive Avature detection.
        
        Returns:
            {
                'tenant': str,
                'url': str or None,
                'status': str,
                'job_count': int,
                'validation_details': dict
            }
        """
        candidate_urls = self.get_candidate_patterns(tenant)
        
        logger.debug(f"Testing {len(candidate_urls)} patterns for {tenant}")
        
        # Track all attempted URLs and their results for debugging
        attempt_details = []
        
        for url in candidate_urls:
            try:
                result = self.validator._test_url(url)
                attempt_details.append({'url': url, 'result': result})
                
                logger.debug(f"  {tenant}: {url} -> {result.get('status')} ({result.get('reason', 'OK')})")
                
                if result['status'] == 'valid':
                    return {
                        'tenant': tenant,
                        'url': url,
                        'status': 'valid',
                        'job_count': result.get('job_count', 0),
                        'validation_details': result,
                        'attempts': attempt_details
                    }
                elif result['status'] == 'blocked':
                    # Site might be valid but blocking bots - treat as likely valid
                    logger.debug(f"  {tenant}: {url} -> blocked but likely valid")
                    return {
                        'tenant': tenant,
                        'url': url,
                        'status': 'valid_blocked',
                        'job_count': 0,  # Can't get job count due to blocking
                        'validation_details': result,
                        'attempts': attempt_details
                    }
                elif result['status'] == 'redirected':
                    # Still consider as found, but note the redirect
                    return {
                        'tenant': tenant,
                        'url': url,
                        'status': 'redirected',
                        'job_count': 0,
                        'validation_details': result,
                        'attempts': attempt_details
                    }
            
            except Exception as e:
                logger.debug(f"  {tenant}: {url} -> Error: {e}")
                attempt_details.append({'url': url, 'result': {'status': 'error', 'reason': str(e)}})
        
        return {
            'tenant': tenant,
            'url': None,
            'status': 'no_valid_urls',
            'job_count': 0,
            'validation_details': {
                'reason': f'No valid URLs found after testing {len(candidate_urls)} patterns',
                'attempts': attempt_details
            }
        }
    
    def discover_job_boards(self, tenants: List[str], input_file: str, max_workers: int = 3) -> Dict:
        """
        Discover and validate job boards for multiple tenants.
        
        Returns:
            {
                'valid': [...],          # Successfully found job boards
                'redirected': [...],     # URLs that redirect to other systems
                'failed': [...],         # No valid URLs found
                'total_jobs': {...},     # Job counts per valid site
                'discovery_details': {}  # Detailed results per tenant
            }
        """
        results = {
            'valid': [],
            'redirected': [],
            'failed': [],
            'total_jobs': {},
            'discovery_details': {}
        }
        
        logger.info(f"Discovering job boards for {len(tenants):,} tenants with {max_workers} workers...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_tenant = {
                executor.submit(self.find_valid_url_for_tenant, tenant): tenant 
                for tenant in tenants
            }
            
            for i, future in enumerate(as_completed(future_to_tenant), 1):
                try:
                    tenant_result = future.result()
                    tenant = tenant_result['tenant']
                    
                    # Store detailed results
                    results['discovery_details'][tenant] = tenant_result
                    
                    if tenant_result['status'] == 'valid':
                        url = tenant_result['url']
                        job_count = tenant_result['job_count']
                        results['valid'].append(url)
                        results['total_jobs'][url] = job_count
                        
                        # Immediately append to success file
                        self._append_success(tenant, url, job_count)
                        logger.info(f"✓ {tenant}: {url} ({job_count} jobs)")
                    
                    elif tenant_result['status'] == 'valid_blocked':
                        url = tenant_result['url']
                        results['valid'].append(url)
                        results['total_jobs'][url] = 0  # Unknown job count due to blocking
                        
                        # Immediately append to success file
                        self._append_success(tenant, url, 0)
                        logger.info(f"✓ {tenant}: {url} (blocked but likely valid)")
                    
                    elif tenant_result['status'] == 'redirected':
                        redirect_url = tenant_result['validation_details'].get('redirect_url', 'Unknown')
                        results['redirected'].append({
                            'tenant': tenant,
                            'original': tenant_result['url'],
                            'redirect_info': tenant_result['validation_details']
                        })
                        
                        # Immediately append to redirect file
                        self._append_redirect(tenant, redirect_url)
                        logger.info(f"⚠ {tenant}: redirected to different system")
                    
                    else:
                        results['failed'].append(tenant)
                        reason = tenant_result['validation_details'].get('reason', 'Unknown')
                        
                        # Immediately append to failure file
                        self._append_failure(tenant, reason)
                        logger.info(f"✗ {tenant}: {reason}")
                    
                    # Remove processed tenant from input file
                    self._remove_from_input_file(input_file, tenant)
                    
                    # Progress indicator
                    if i % 10 == 0:
                        valid_count = len(results['valid'])
                        logger.info(f"Progress: {i:,}/{len(tenants):,} tenants - {valid_count:,} valid job boards")
                
                except Exception as e:
                    tenant = future_to_tenant[future]
                    results['failed'].append(tenant)
                    logger.error(f"✗ {tenant}: Error during discovery: {e}")
        
        # Log final summary
        total_jobs = sum(results['total_jobs'].values())
        logger.info(f"Discovery complete:")
        logger.info(f"  Valid job boards: {len(results['valid']):,}")
        logger.info(f"  Redirected sites: {len(results['redirected']):,}")
        logger.info(f"  Failed tenants: {len(results['failed']):,}")
        logger.info(f"  Total jobs found: {total_jobs:,}")
        
        # Add completion markers to files
        with self._file_lock:
            # Don't add completion markers to success file to keep it clean
            pass
        
        # Store file paths in results for reporting
        results['output_files'] = {
            'success': str(self.success_file),
            'failures': str(self.failure_file),
            'redirected': str(self.redirect_file)
        }
        
        return results
    
    def _init_output_files(self):
        """Initialize output files with headers"""
        with self._file_lock:
            # Success URLs file
            with open(self.success_file, 'w', encoding='utf-8') as f:
                pass  # Empty file, no headers
            
            # Failed tenants file
            with open(self.failure_file, 'w', encoding='utf-8') as f:
                pass  # Empty file, no headers
            
            # Redirected tenants file
            with open(self.redirect_file, 'w', encoding='utf-8') as f:
                pass  # Empty file, no headers
    
    def _append_success(self, tenant: str, url: str, job_count: int):
        """Append successful result to success file"""
        with self._file_lock:
            with open(self.success_file, 'a', encoding='utf-8') as f:
                f.write(f"{url}\n")
    
    def _append_failure(self, tenant: str, reason: str):
        """Append failed result to failure file"""
        with self._file_lock:
            with open(self.failure_file, 'a', encoding='utf-8') as f:
                f.write(f"{tenant}\n")
    
    def _append_redirect(self, tenant: str, redirect_url: str):
        """Append redirected result to redirect file"""
        with self._file_lock:
            with open(self.redirect_file, 'a', encoding='utf-8') as f:
                f.write(f"{tenant} -> {redirect_url}\n")
    
    def _remove_from_input_file(self, input_file: str, tenant: str):
        """Remove processed tenant from original input file"""
        try:
            with self._file_lock:
                # Read all lines
                with open(input_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                
                # Filter out the processed tenant (handle comments and whitespace)
                remaining_lines = []
                for line in lines:
                    stripped = line.strip()
                    if stripped and not stripped.startswith('#'):
                        if stripped != tenant:
                            remaining_lines.append(line)
                    else:
                        # Keep comments and empty lines
                        remaining_lines.append(line)
                
                # Write back the remaining tenants
                with open(input_file, 'w', encoding='utf-8') as f:
                    f.writelines(remaining_lines)
                    
                logger.debug(f"Removed {tenant} from {input_file}")
        
        except Exception as e:
            logger.warning(f"Could not remove {tenant} from {input_file}: {e}")

def save_results(results: Dict, output_dir: Path) -> Dict[str, str]:
    """Save discovery results to various files in the timestamped folder."""
    files_created = {}
    
    # 1. Valid URLs file (for scraper input)
    if results['valid']:
        valid_urls_file = output_dir / "valid_urls_summary.txt"
        with open(valid_urls_file, 'w', encoding='utf-8') as f:
            f.write("# Valid Avature Job Board URLs\n")
            f.write(f"# Generated by job board finder on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total job boards: {len(results['valid'])}\n")
            f.write(f"# Total jobs: {sum(results['total_jobs'].values())}\n\n")
            
            for url in results['valid']:
                job_count = results['total_jobs'].get(url, 0)
                f.write(f"{url}  # {job_count} jobs\n")
        
        files_created['valid_urls'] = str(valid_urls_file)
        logger.info(f"✓ Saved {len(results['valid'])} valid URLs to {valid_urls_file}")
    
    # 2. Detailed JSON results
    json_file = output_dir / "discovery_results.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    files_created['json_results'] = str(json_file)
    logger.info(f"✓ Saved detailed results to {json_file}")
    
    # 3. Failed tenants (for retry or analysis)
    if results['failed']:
        failed_file = output_dir / "failed_tenants_summary.txt"
        with open(failed_file, 'w', encoding='utf-8') as f:
            f.write("# Tenants with no valid job board URLs found\n")
            f.write(f"# Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Failed count: {len(results['failed'])}\n\n")
            
            for tenant in results['failed']:
                # Get failure reason from details
                details = results['discovery_details'].get(tenant, {})
                reason = details.get('validation_details', {}).get('reason', 'Unknown')
                f.write(f"{tenant}  # {reason}\n")
        
        files_created['failed_tenants'] = str(failed_file)
        logger.info(f"✓ Saved {len(results['failed'])} failed tenants to {failed_file}")
    
    # 4. Redirected sites report
    if results['redirected']:
        redirected_file = output_dir / "redirected_sites_summary.txt"
        with open(redirected_file, 'w', encoding='utf-8') as f:
            f.write("# Sites that redirected to different ATS systems\n")
            f.write(f"# Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Redirected count: {len(results['redirected'])}\n\n")
            
            for item in results['redirected']:
                redirect_url = item['redirect_info'].get('redirect_url', 'Unknown')
                f.write(f"{item['tenant']} -> {redirect_url}\n")
        
        files_created['redirected_sites'] = str(redirected_file)
        logger.info(f"✓ Saved {len(results['redirected'])} redirected sites to {redirected_file}")
    
    return files_created


def print_summary(results: Dict):
    """Print a comprehensive summary of discovery results."""
    print("\n" + "="*80)
    print("JOB BOARD DISCOVERY SUMMARY")
    print("="*80)
    
    total_tenants = len(results['valid']) + len(results['failed']) + len(results['redirected'])
    total_jobs = sum(results['total_jobs'].values())
    
    print(f"\nTenants processed: {total_tenants:,}")
    print(f"✓ Valid job boards: {len(results['valid']):,}")
    print(f"⚠ Redirected sites: {len(results['redirected']):,}")
    print(f"✗ Failed tenants: {len(results['failed']):,}")
    print(f"📊 Total jobs available: {total_jobs:,}")
    
    if results['valid']:
        success_rate = len(results['valid']) / total_tenants * 100
        print(f"🎯 Success rate: {success_rate:.1f}%")
    
    # Top job boards by job count
    if results['total_jobs']:
        print(f"\n{'='*80}")
        print("TOP JOB BOARDS BY JOB COUNT")
        print("="*80)
        
        sorted_jobs = sorted(results['total_jobs'].items(), key=lambda x: x[1], reverse=True)
        
        for i, (url, count) in enumerate(sorted_jobs[:10], 1):
            tenant = url.replace('https://', '').split('.')[0]
            print(f"{i:2d}. {tenant:20} | {count:5,} jobs | {url}")
    
    # Sample of failed tenants
    if results['failed']:
        print(f"\n{'='*80}")
        print("SAMPLE FAILED TENANTS")
        print("="*80)
        
        for tenant in results['failed'][:5]:
            details = results['discovery_details'].get(tenant, {})
            reason = details.get('validation_details', {}).get('reason', 'Unknown')[:50]
            print(f"  ✗ {tenant}: {reason}")
        
        if len(results['failed']) > 5:
            print(f"  ... and {len(results['failed']) - 5} more")
    
    print("="*80)


def main():
    """Discover and validate Avature job boards from tenant list."""
    parser = argparse.ArgumentParser(
        description='Discover and validate Avature career sites from tenant names',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python job_board_finder.py tenants.txt
  python job_board_finder.py tenants.txt --workers 5 --verbose
  
This tool will:
  1. Generate candidate URLs for each tenant
  2. Validate each URL using comprehensive Avature detection
  3. Categorize sites as valid, redirected, or failed
  4. Extract job counts for valid sites
  5. Save results in multiple formats for analysis
        """
    )
    
    parser.add_argument('tenants_file', help='Path to tenants file (one tenant per line)')
    parser.add_argument('--workers', '-w', type=int, default=3, help='Max concurrent workers (default: 3)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set up logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Check input file
    if not Path(args.tenants_file).exists():
        logger.error(f"Input file not found: {args.tenants_file}")
        sys.exit(1)
    
    logger.info("Starting Avature job board discovery...")
    logger.info(f"Input file: {args.tenants_file}")
    logger.info(f"Workers: {args.workers}")
    
    # Load tenants
    tenants = load_tenants(args.tenants_file)
    if not tenants:
        logger.error("No tenants loaded!")
        sys.exit(1)
    
    # Initialize finder (creates timestamped folder automatically)
    finder = AvatureJobBoardFinder()
    logger.info(f"Output directory: {finder.output_dir}")
    
    results = finder.discover_job_boards(tenants, args.tenants_file, args.workers)
    
    # Save results to files in the same timestamped folder
    files_created = save_results(results, finder.output_dir)
    
    # Print comprehensive summary
    print_summary(results)
    
    # Show files created (including incremental files)
    print(f"\n{'='*80}")
    print("FILES CREATED")
    print("="*80)
    
    # Show incremental files first
    output_files = results.get('output_files', {})
    if output_files:
        print("  Incremental Processing Files:")
        for file_type, filepath in output_files.items():
            print(f"    {file_type:12} | {filepath}")
        print()
    
    # Show summary files
    if files_created:
        print("  Summary Files:")
        for file_type, filepath in files_created.items():
            print(f"    {file_type:12} | {filepath}")
    
    # Usage suggestions
    print(f"\n{'='*80}")
    print("NEXT STEPS")
    print("="*80)
    
    # Show incremental files usage
    output_files = results.get('output_files', {})
    success_file = output_files.get('success')
    
    if success_file and results['valid']:
        print(f"✓ Use the success file for immediate scraping:")
        print(f"  python ../hybrid_scraper.py {success_file}")
        print(f"\n✓ Or validate URLs further with:")
        print(f"  python url_validator.py {success_file}")
    
    # Show resumption info
    remaining_tenants = len(load_tenants(args.tenants_file))
    if remaining_tenants > 0:
        print(f"\n⚠ Process can be resumed:")
        print(f"  {remaining_tenants} tenants remaining in {args.tenants_file}")
        print(f"  Run the same command again to continue processing")
    else:
        print(f"\n✓ All tenants processed!")
        print(f"  Input file {args.tenants_file} is now empty")


if __name__ == "__main__":
    main()