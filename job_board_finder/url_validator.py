"""
Avature URL Validator
Tests a list of URLs to determine which are valid, active Avature sites
"""

import requests
from bs4 import BeautifulSoup
import json
from typing import List, Dict
import time
from pathlib import Path
import re

class AvatureURLValidator:
    """Validates and categorizes potential Avature career sites"""
    
    def __init__(self):
        self.session = requests.Session()
        # Use comprehensive browser headers to avoid 406 errors
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'DNT': '1'
        })
    
    def validate_urls(self, urls: List[str]) -> Dict:
        """
        Test a list of URLs and categorize them
        
        Returns:
            {
                'valid': [...],      # Active Avature sites
                'invalid': [...],    # Not Avature or not accessible
                'redirected': [...], # Redirected to different system
                'total_jobs': {...}  # Job counts per valid site
            }
        """
        results = {
            'valid': [],
            'invalid': [],
            'redirected': [],
            'total_jobs': {},
            'validation_details': {}
        }
        
        print(f"Validating {len(urls)} URLs...")
        print("=" * 60)
        
        for i, url in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] Testing: {url}")
            
            result = self._test_url(url)
            
            if result['status'] == 'valid':
                results['valid'].append(url)
                results['total_jobs'][url] = result.get('job_count', 0)
                print(f"  ✓ VALID - {result.get('job_count', 0)} jobs")
            
            elif result['status'] == 'redirected':
                results['redirected'].append({
                    'original': url,
                    'redirected_to': result.get('redirect_url'),
                    'reason': result.get('reason')
                })
                print(f"  ⚠ REDIRECTED to {result.get('redirect_url')}")
            
            else:
                results['invalid'].append(url)
                print(f"  ✗ INVALID - {result.get('reason')}")
            
            results['validation_details'][url] = result
        
        return results
    
    def _test_url(self, url: str) -> Dict:
        """Test a single URL with retry logic for 406 errors"""
        # Try with different user agents if we get 406 errors
        user_agents = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]
        
        for attempt, user_agent in enumerate(user_agents):
            try:
                # Update user agent for this attempt
                headers = self.session.headers.copy()
                headers['User-Agent'] = user_agent
                
                # Try to access the URL
                resp = self.session.get(url, timeout=15, allow_redirects=True, headers=headers)
                
                # Check if redirected to a different domain
                if resp.url != url and not self._is_same_domain(url, resp.url):
                    return {
                        'status': 'redirected',
                        'redirect_url': resp.url,
                        'reason': 'Redirected to different domain (likely changed ATS)'
                    }
                
                # Check if it's accessible
                if resp.status_code == 406:
                    if attempt < len(user_agents) - 1:
                        continue  # Try next user agent
                    else:
                        # Special handling for 406 - might still be valid but blocking automated access
                        return {
                            'status': 'blocked',
                            'reason': f'HTTP 406 (blocked after {len(user_agents)} attempts - site may be valid but blocking bots)',
                            'http_code': 406
                        }
                
                if resp.status_code != 200:
                    return {
                        'status': 'invalid',
                        'reason': f'HTTP {resp.status_code}'
                    }
                
                # Check if it's actually Avature
                if not self._is_avature_site(resp.text, resp.url):
                    return {
                        'status': 'invalid',
                        'reason': 'Not an Avature site (different ATS detected)'
                    }
                
                # Try to get job count
                job_count = self._get_job_count(resp.text)
                
                # For successful validation, accept sites even with 0 jobs
                # (they might have jobs but we can't detect them properly)
                return {
                    'status': 'valid',
                    'job_count': job_count,
                    'has_sitemap': self._check_sitemap(url),
                    'has_rss': self._check_rss(url),
                    'user_agent_used': user_agent
                }
            
            except requests.exceptions.Timeout:
                if attempt < len(user_agents) - 1:
                    continue  # Try next user agent
                return {
                    'status': 'invalid',
                    'reason': 'Timeout (site not responding)'
                }
            
            except requests.exceptions.ConnectionError:
                if attempt < len(user_agents) - 1:
                    continue  # Try next user agent
                return {
                    'status': 'invalid',
                    'reason': 'Connection error (site not accessible)'
                }
            
            except Exception as e:
                if attempt < len(user_agents) - 1:
                    continue  # Try next user agent
                return {
                    'status': 'invalid',
                    'reason': f'Error: {str(e)}'
                }
        
        # If we get here, all attempts failed
        return {
            'status': 'invalid',
            'reason': f'All {len(user_agents)} attempts failed'
        }
    
    def _is_same_domain(self, url1: str, url2: str) -> bool:
        """Check if two URLs are from the same domain"""
        from urllib.parse import urlparse
        domain1 = urlparse(url1).netloc
        domain2 = urlparse(url2).netloc
        return domain1 == domain2
    
    def _is_avature_site(self, html: str, url: str) -> bool:
        """Check if this is an Avature site"""
        # Check 1: URL contains 'avature.net'
        if 'avature.net' in url:
            return True
        
        # Check 2: HTML contains Avature signatures
        avature_signatures = [
            'avature',
            'portal/jquery',
            '/ASSET/portal/',
            'EventManager.getInstance()',
            'wizard/portal/',
        ]
        
        html_lower = html.lower()
        matches = sum(1 for sig in avature_signatures if sig.lower() in html_lower)
        
        # If 3+ signatures found, likely Avature
        return matches >= 3
    
    def _get_job_count(self, html: str) -> int:
        """Extract total job count from page"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for "X of Y results" pattern
            legend = soup.find('div', class_='list-controls__text__legend')
            if legend:
                match = re.search(r'of\s+(\d+)', legend.text)
                if match:
                    return int(match.group(1))
            
            # Look in any text
            match = re.search(r'of\s+(\d+)\s+result', html)
            if match:
                return int(match.group(1))
            
            # Count job articles as fallback
            articles = soup.find_all('article', class_='article--result')
            if articles:
                return len(articles)  # At least this many
            
            return 0
        
        except:
            return 0
    
    def _check_sitemap(self, base_url: str) -> bool:
        """Check if sitemap exists"""
        sitemap_url = f"{base_url}/sitemap.xml"
        try:
            resp = self.session.get(sitemap_url, timeout=5)
            return resp.status_code == 200
        except:
            return False
    
    def _check_rss(self, base_url: str) -> bool:
        """Check if RSS feed exists"""
        rss_url = f"{base_url}/SearchJobs/feed/"
        try:
            resp = self.session.get(rss_url, timeout=5)
            return resp.status_code == 200 and 'xml' in resp.headers.get('Content-Type', '')
        except:
            return False


def load_urls_from_file(filepath: str) -> List[str]:
    """Load URLs from a text file (one per line)"""
    urls = []
    with open(filepath, 'r') as f:
        for line in f:
            url = line.strip()
            if url and not url.startswith('#'):  # Skip empty lines and comments
                urls.append(url)
    return urls


def save_results(results: Dict, output_file: str):
    """Save validation results to JSON"""
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"Results saved to: {output_file}")


def print_summary(results: Dict):
    """Print validation summary"""
    print("\n" + "="*60)
    print("VALIDATION SUMMARY")
    print("="*60)
    
    total = len(results['valid']) + len(results['invalid']) + len(results['redirected'])
    
    print(f"\nTotal URLs tested: {total}")
    print(f"✓ Valid Avature sites: {len(results['valid'])}")
    print(f"✗ Invalid/Inactive: {len(results['invalid'])}")
    print(f"⚠ Redirected: {len(results['redirected'])}")
    
    if results['valid']:
        print(f"\n{'='*60}")
        print("VALID SITES")
        print("="*60)
        
        total_jobs = 0
        for url in results['valid']:
            job_count = results['total_jobs'].get(url, 0)
            total_jobs += job_count
            print(f"  ✓ {url}")
            print(f"    Jobs: {job_count}")
        
        print(f"\nTotal jobs available: {total_jobs:,}")
    
    if results['redirected']:
        print(f"\n{'='*60}")
        print("REDIRECTED SITES (Changed ATS)")
        print("="*60)
        for item in results['redirected']:
            print(f"  {item['original']}")
            print(f"    → {item['redirected_to']}")
            print(f"    Reason: {item['reason']}")
    
    print("="*60)


def create_input_file(urls: List[str], output_file: str = 'validated_urls.txt'):
    """Create input file for the scraper with valid URLs"""
    with open(output_file, 'w') as f:
        f.write("# Validated Avature URLs\n")
        f.write("# Generated by URL validator\n")
        f.write(f"# Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        for url in urls:
            f.write(f"{url}\n")
    
    print(f"\n✓ Created input file: {output_file}")
    print(f"  Ready to use with scraper!")


def main():
    """Main execution"""
    import sys
    
    # Check if input file provided
    if len(sys.argv) < 2:
        print("Usage: python url_validator.py <input_file.txt>")
        print("\nInput file should contain one URL per line:")
        print("  https://company1.avature.net/careers")
        print("  https://company2.avature.net/careers")
        print("  ...")
        
        # Create example file
        example_urls = [
            "https://bloomberg.avature.net/careers",
            "https://uclahealth.avature.net/careers",
            "https://cbs.avature.net/careers",  # Will be marked as invalid/redirected
        ]
        
        print("\nExample validation:")
        validator = AvatureURLValidator()
        results = validator.validate_urls(example_urls)
        print_summary(results)
        return
    
    input_file = sys.argv[1]
    
    # Load URLs from file
    print(f"Loading URLs from: {input_file}")
    urls = load_urls_from_file(input_file)
    print(f"Found {len(urls)} URLs to validate\n")
    
    # Validate
    validator = AvatureURLValidator()
    results = validator.validate_urls(urls)
    
    # Save results
    output_file = 'validation_results.json'
    save_results(results, output_file)
    
    # Print summary
    print_summary(results)
    
    # Create input file for scraper with valid URLs only
    if results['valid']:
        create_input_file(results['valid'], 'validated_urls.txt')


if __name__ == "__main__":
    main()