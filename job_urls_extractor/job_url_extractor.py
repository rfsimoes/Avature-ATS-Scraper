"""
Avature ATS Scraper - Full Multi-Strategy Implementation
Demonstrates comprehensive discovery and implements all fallback strategies:
1. Sitemap (primary - fastest, includes historical jobs)
2. RSS Feed (secondary - limited but structured)
3. HTML Pagination (fallback - always works)
"""

import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import time
import json
import re
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from urllib.parse import urljoin, urlparse
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logger.warning("Playwright not available. Install with: pip install playwright")


@dataclass
class URLFailure:
    """Failed URL collection record"""
    url: str
    company: str
    source: str  # 'sitemap' or 'html'
    error_type: str
    error_message: str
    http_status: Optional[int] = None
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat()


@dataclass 
class URLSuccess:
    """Successful URL collection record"""
    url: str
    job_id: str
    company: str
    source: str  # 'sitemap' or 'html'
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat()


@dataclass
class JobFailure:
    """Failed job extraction record"""
    url: str
    job_id: str
    company: str
    error_type: str
    error_message: str
    http_status: Optional[int] = None
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat()


@dataclass
class Job:
    """Job posting data model"""
    job_id: str
    title: str
    url: str
    location: str
    company: str
    source_method: str  # 'sitemap', 'rss', or 'html'
    description: Optional[str] = None
    date_posted: Optional[str] = None
    department: Optional[str] = None
    employment_type: Optional[str] = None
    application_url: Optional[str] = None
    scraped_at: str = None
    
    def __post_init__(self):
        if self.scraped_at is None:
            self.scraped_at = datetime.utcnow().isoformat()


class AvatureMultiStrategyScraper:
    """
    Multi-strategy scraper that tries all discovered methods with intelligent fallbacks
    
    Discovery findings:
    - Sitemaps: Contain MORE jobs than website (includes recently closed, ~1165 total)
    - RSS Feeds: Limited to 20 most recent items (not useful for bulk scraping)
    - HTML Pagination: Always works but slower (different page sizes per site)
    
    Strategy order:
    1. Try Sitemap first (fastest, most complete)
    2. If sitemap incomplete/missing, supplement with HTML pagination
    3. Track RSS feed availability for documentation purposes
    """
    
    def __init__(self, company_name: str, base_url: str, max_workers: int = 5):
        self.company_name = company_name
        self.original_base_url = base_url.rstrip('/')
        self.max_workers = max_workers  # Reduced from 10 to 5 for better rate limiting
        
        parsed = urlparse(base_url)
        self.domain = f"{parsed.scheme}://{parsed.netloc}"
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # URL collection tracking
        self.successes: List[URLSuccess] = []
        self.retries: List[URLFailure] = []
        self.failures: List[URLFailure] = []
        
        # Strategy tracking
        self.strategy_used = None
        self.rss_available = False
        
        # Resolve actual base URL by following redirects
        self.base_url = self._resolve_base_url()
        
        # Create output directories
        self.failures_dir = Path('failures')
        self.failures_dir.mkdir(exist_ok=True)
        self.retries_dir = Path('retries')
        self.retries_dir.mkdir(exist_ok=True)
    
    def _is_avature_domain(self, url: str) -> bool:
        """
        Check if a URL is hosted by Avature
        Avature sites should have 'avature.net' as the domain or subdomain
        Secure check to prevent subdomain attacks like 'avature.net.evil.com'
        """
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        # Check if domain ends with '.avature.net' or is exactly 'avature.net'
        return domain == 'avature.net' or domain.endswith('.avature.net')
    
    def _is_aws_waf_protected(self, url: str) -> bool:
        """Check if a site is protected by AWS WAF by looking for challenge responses"""
        try:
            resp = self.session.get(url, timeout=10)
            return resp.status_code == 202 and 'x-amzn-waf-action' in resp.headers
        except:
            return False
    
    def _resolve_base_url(self) -> str:
        """
        Resolve the actual base URL by following redirects
        Many Avature sites redirect to locale-specific URLs like /en_US/careers
        Raises an exception if redirected to a non-Avature domain
        """
        try:
            # Check if site needs AWS WAF authentication
            aws_waf_protected_domains = ['koch', 'sandboxlululemoninc']
            needs_waf_auth = any(domain in self.original_base_url.lower() for domain in aws_waf_protected_domains)
            
            if needs_waf_auth or self._is_aws_waf_protected(self.original_base_url):
                self._get_aws_waf_token()
                
            # For Koch and other AWS WAF-protected sites, set up authentication first
            if 'koch' in self.original_base_url.lower():
                # Koch specifically redirects to /en_US/careers
                return "https://koch.avature.net/en_US/careers"
            
            # Try the original URL first for other sites
            resp = self.session.get(self.original_base_url, timeout=10, allow_redirects=True)
            
            # Check if we were redirected to a non-Avature domain regardless of status code
            final_url = resp.url.rstrip('/')
            if final_url != self.original_base_url:
                logger.info(f"Detected redirect: {self.original_base_url} → {final_url}")
                
                # Check if the final URL is still an Avature domain
                if not self._is_avature_domain(final_url):
                    error_msg = f"Site redirected to non-Avature domain: {final_url}"
                    logger.error(error_msg)
                    raise Exception(f"external_redirect: {error_msg}")
            
            if resp.status_code in [200, 202]:  # Accept both 200 and 202 status codes
                return final_url
            else:
                # Even if we get an error status, if we're still on Avature domain, continue
                if self._is_avature_domain(final_url):
                    logger.warning(f"HTTP {resp.status_code} for {final_url}, but staying on Avature domain")
                    return final_url
                else:
                    # Already handled non-Avature redirects above
                    logger.warning(f"Could not resolve base URL {self.original_base_url}, using as-is")
                    return self.original_base_url
        except Exception as e:
            # If it's our external redirect exception, re-raise it
            if "external_redirect:" in str(e):
                raise
            logger.warning(f"Error resolving base URL {self.original_base_url}: {e}, using as-is")
            return self.original_base_url
    
    def scrape_all_job_urls(self) -> List[str]:
        """
        Multi-strategy URL collection with intelligent fallbacks
        Returns empty list if site redirects to non-Avature domain
        """
        try:
            # This will raise an exception if redirected to non-Avature domain
            resolved_url = self.base_url
        except Exception as e:
            if "external_redirect:" in str(e):
                # Log the redirect failure
                failure = URLFailure(
                    url=self.original_base_url,
                    company=self.company_name,
                    source='redirect_check',
                    error_type='external_redirect',
                    error_message=str(e).replace("external_redirect: ", "")
                )
                self.failures.append(failure)
                self._save_failures()
                
                logger.error(f"{'='*60}")
                logger.error(f"REDIRECT FAILURE: {self.company_name}")
                logger.error(f"Original URL: {self.original_base_url}")
                logger.error(f"Reason: {failure.error_message}")
                logger.error(f"{'='*60}")
                return []
            else:
                # Re-raise other exceptions
                raise
        
        logger.info(f"{'='*60}")
        logger.info(f"Starting multi-strategy scrape for {self.company_name}")
        logger.info(f"Base URL: {self.base_url}")
        logger.info(f"{'='*60}\n")
        
        # Step 1: Check all available methods
        logger.info("DISCOVERY: Checking available data sources...")
        logger.info("-" * 60)
        
        total_jobs_html = self._get_total_job_count()
        logger.info(f"Website reports: {total_jobs_html} jobs")
        
        # Check RSS (for documentation, not scraping)
        rss_count = self._check_rss_availability()
        if rss_count:
            logger.info(f"RSS feed available: {rss_count} items (limited, not used for scraping)")
            self.rss_available = True
        else:
            logger.info(f"RSS feed: Not available")
        
        logger.info("")
        
        # Step 2: Try sitemap first (primary strategy)
        sitemap_urls, sitemap_job_ids = self._try_sitemap_strategy()
        
        # Step 3: Smart gap detection
        all_urls = sitemap_urls
        
        if not sitemap_urls:
            # No sitemap or completely failed - use HTML only
            logger.info("⚠️  Sitemap strategy failed, using HTML pagination")
            all_urls = self._scrape_via_html_pagination(set(), total_jobs_html)
            self.strategy_used = "html_only"
        
        else:
            # Sitemap worked - check if HTML has any jobs we don't have
            logger.info("VERIFICATION: Checking for URLs not in sitemap...")
            logger.info("-" * 60)
            
            # Sample first 3 pages of HTML to detect gaps
            sample_urls = self._check_html_sample(sitemap_job_ids, pages=3)
            
            if sample_urls:
                logger.info(f"✓ Found {len(sample_urls)} URLs in HTML not in sitemap")
                logger.info(f"Running full HTML pagination to capture all missing URLs...\n")
                
                # Do full HTML scrape
                html_urls = self._scrape_via_html_pagination(sitemap_job_ids, total_jobs_html)
                
                # Combine and deduplicate all URLs by job ID
                all_urls = self._deduplicate_urls(sitemap_urls + html_urls)
                self.strategy_used = "sitemap_plus_html"
            else:
                logger.info(f"✓ No gaps detected - sitemap appears complete\n")
                self.strategy_used = "sitemap_only"
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Strategy used: {self.strategy_used}")
        logger.info(f"Final count: {len(all_urls)} URLs for {self.company_name}")
        logger.info(f"{'='*60}\n")
        
        return all_urls
    
    def _check_rss_availability(self) -> Optional[int]:
        """Check if RSS feed exists and how many items it has"""
        rss_urls = [
            f"{self.base_url}/SearchJobs/feed/",
            f"{self.base_url}/feed/",
        ]
        
        for rss_url in rss_urls:
            try:
                resp = self.session.get(rss_url, timeout=10)
                if resp.status_code == 200 and 'xml' in resp.headers.get('Content-Type', '').lower():
                    soup = BeautifulSoup(resp.content, 'xml')
                    items = soup.find_all('item')
                    if items:
                        return len(items)
            except:
                continue
        
        return None
    
    def _try_sitemap_strategy(self) -> Tuple[List[str], Set[str]]:
        """
        Try to collect URLs from sitemap
        Returns: (urls_list, job_ids_set)
        """
        logger.info("STRATEGY 1: Sitemap Extraction")
        logger.info("-" * 60)
        
        sitemap_url = f"{self.base_url}/sitemap.xml"
        job_urls = self._get_job_urls_from_sitemap(sitemap_url)
        
        if not job_urls:
            logger.info("✗ Sitemap not available or contains no job URLs\n")
            return [], set()
        
        logger.info(f"✓ Found {len(job_urls)} job URLs in sitemap")
        logger.info(f"Fetching job details with {self.max_workers} workers and retry logic...\n")

                
        logger.info(f"✓ Found {len(job_urls)} job URLs in sitemap")
        logger.info(f"Processing URLs and extracting job IDs...\n")
        
        job_ids = set()
        
        # Process each URL from sitemap
        for url in job_urls:
            try:
                job_id = self._extract_job_id(url)
                
                # Record successful URL collection
                success = URLSuccess(
                    url=url,
                    job_id=job_id,
                    company=self.company_name,
                    source='sitemap'
                )
                self.successes.append(success)
                job_ids.add(job_id)
                
            except Exception as e:
                logger.debug(f"Error processing sitemap URL {url}: {e}")
                failure = URLFailure(
                    url=url,
                    company=self.company_name,
                    source='sitemap',
                    error_type='parse_error',
                    error_message=f'Failed to extract job ID: {str(e)}'
                )
                self.failures.append(failure)
        
        success_urls = [s.url for s in self.successes if s.source == 'sitemap']
        logger.info(f"Sitemap processing: {len(success_urls)} URLs collected, {len([f for f in self.failures if f.source == 'sitemap'])} failures\n")
        
        return success_urls, job_ids
    
    def _check_html_sample(self, existing_job_ids: Set[str], pages: int = 3) -> List[str]:
        """
        Check first N pages of HTML for URLs not in sitemap
        This detects if there are new URLs posted after sitemap was generated
        """
        new_urls = []
        page_size = self._detect_page_size()
        search_url = f"{self.base_url}/SearchJobs/"
        
        for page_num in range(1, pages + 1):
            try:
                offset = (page_num - 1) * page_size
                params = self._get_pagination_params(page_size, offset)
                resp = self.session.get(search_url, params=params, timeout=10)
                
                # Handle HTTP errors for sample check
                if resp.status_code == 429:
                    logger.debug(f"Rate limited during sample check on page {page_num}")
                    break
                    
                if resp.status_code not in [200, 202]:
                    logger.debug(f"HTTP {resp.status_code} during sample check on page {page_num}")
                    break
                
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                # Find job containers - try multiple structures
                articles = soup.find_all('article', class_='article--result')
                
                # If no standard articles, try alternatives
                if not articles:
                    # Try li elements that actually contain job links (most reliable)
                    all_lis = soup.find_all('li')
                    list_items = [li for li in all_lis if li.find('a', href=re.compile(r'/(JobDetail|FolderDetail|PipelineDetail)/'))]
                    
                    if list_items:
                        logger.debug(f"Found {len(list_items)} jobs using list structure")
                        articles = list_items
                    else:
                        # Try table rows with specific class first (DeloitteBE style)
                        table_rows = soup.find_all('tr', class_='card--box')
                        if table_rows:
                            articles = table_rows
                        else:
                            # Try any table rows that contain job links (sandboxbnc style)
                            all_trs = soup.find_all('tr')
                            table_job_rows = [tr for tr in all_trs if tr.find('a', href=re.compile(r'/(JobDetail|FolderDetail|PipelineDetail)/'))]
                            if table_job_rows:
                                logger.debug(f"Found {len(table_job_rows)} jobs using table row structure")
                                articles = table_job_rows
                            else:
                                div_jobs = soup.find_all('div', class_=lambda x: x and 'job' in x.lower())
                                if div_jobs:
                                    articles = div_jobs
                
                if not articles:
                    break
                
                for article in articles:
                    try:
                        link = article.find('a', href=re.compile(r'/(JobDetail|FolderDetail|PipelineDetail)/'))
                        if not link:
                            continue
                        
                        job_url = link.get('href')
                        if not job_url.startswith('http'):
                            job_url = urljoin(self.domain, job_url)
                        
                        job_id = self._extract_job_id(job_url)
                        
                        # Check if this is a new job not in sitemap
                        if job_id not in existing_job_ids:
                            new_urls.append(job_url)
                            
                    except Exception as e:
                        logger.debug(f"Error processing sample article: {e}")
                        continue
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.debug(f"Error checking HTML sample page {page_num}: {e}")
                break
        
        logger.info(f"Checked {pages} pages, found {len(new_urls)} URL(s) not in sitemap")
        
        return new_urls
    
    def _scrape_via_html_pagination(self, existing_job_ids: Set[str], total_expected: Optional[int]) -> List[str]:
        """
        Collect job URLs using HTML pagination
        Only collects URLs not already in existing_job_ids
        """
        logger.info("STRATEGY 2: HTML Pagination")
        logger.info("-" * 60)
        
        # For AWS WAF-protected sites, get auth token first
        aws_waf_protected_domains = ['koch', 'sandboxlululemoninc']
        if any(domain in self.base_url.lower() for domain in aws_waf_protected_domains):
            self._get_aws_waf_token()
        
        urls = []
        page_num = 1
        offset = 0
        retry_attempt = 0
        
        # Detect page size
        page_size = self._detect_page_size()
        logger.info(f"Detected page size: {page_size} jobs per page")
        
        # Use Koch-specific URL format if needed
        if 'koch' in self.base_url.lower():
            search_url = "https://koch.avature.net/en_US/careers/SearchJobs/"
            referer = "https://koch.avature.net/en_US/careers"
        else:
            search_url = f"{self.base_url}/SearchJobs/"
            referer = self.base_url
        
        # Add headers that match working curl pattern
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9', 
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': referer,
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate', 
            'Sec-Fetch-Site': 'same-origin',
        }
        
        if total_expected:
            estimated_pages = (total_expected + page_size - 1) // page_size
            logger.info(f"Estimated pages needed: {estimated_pages}\n")
        
        while True:
            try:
                params = self._get_pagination_params(page_size, offset)
                # Add listFilterMode parameter like in the working curl
                params['listFilterMode'] = 1
                
                resp = self.session.get(search_url, params=params, headers=headers, timeout=15)
                
                # Handle HTTP errors (group 406 and 429 together)
                if resp.status_code in [406, 429]:
                    retry_attempt += 1
                    if retry_attempt > 5:  # Max 5 retries per page
                        logger.error(f"Max retries exceeded for page {page_num}, moving to next")
                        break
                    delay = min(60 * (2 ** (retry_attempt - 1)), 300)  # 60s, 120s, 240s, max 300s
                    error_name = "HTTP 406" if resp.status_code == 406 else "Rate limit (429)"
                    logger.warning(f"{error_name} on page {page_num} (attempt {retry_attempt}), waiting {delay}s...")
                    time.sleep(delay)
                    continue  # Retry the same page
                
                if resp.status_code not in [200, 202]:
                    logger.warning(f"HTTP {resp.status_code} on page {page_num}")
                    failure = URLFailure(
                        url=f"{search_url}?{params}",
                        company=self.company_name,
                        source='html',
                        error_type='http_error',
                        error_message=f'HTTP {resp.status_code}',
                        http_status=resp.status_code
                    )
                    self.failures.append(failure)
                    break
                
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                # Extract job URLs from this page - try multiple HTML structures
                articles = soup.find_all('article', class_='article--result')
                
                # If no standard articles found, try alternative structures
                if not articles:
                    # First try to find li elements that actually contain job links
                    all_lis = soup.find_all('li')
                    list_items = [li for li in all_lis if li.find('a', href=re.compile(r'/(JobDetail|FolderDetail|PipelineDetail)/'))]
                    
                    if list_items:
                        logger.debug(f"Found {len(list_items)} jobs using list structure")
                        articles = list_items
                    else:
                        # Try table rows with specific class first (DeloitteBE style)
                        table_rows = soup.find_all('tr', class_='card--box')
                        if table_rows:
                            logger.debug(f"Found {len(table_rows)} jobs using table structure (DeloitteBE style)")
                            articles = table_rows
                        else:
                            # Try any table rows that contain job links (sandboxbnc style)
                            all_trs = soup.find_all('tr')
                            table_job_rows = [tr for tr in all_trs if tr.find('a', href=re.compile(r'/(JobDetail|FolderDetail|PipelineDetail)/'))]
                            if table_job_rows:
                                logger.debug(f"Found {len(table_job_rows)} jobs using table row structure (sandboxbnc style)")
                                articles = table_job_rows
                            else:
                                # Other possible alternative structures
                                div_jobs = soup.find_all('div', class_=lambda x: x and 'job' in x.lower())
                                if div_jobs:
                                    logger.debug(f"Found {len(div_jobs)} jobs using div structure")
                                    articles = div_jobs
                
                if not articles:
                    logger.info(f"No more jobs found on page {page_num}")
                    break
                
                logger.info(f"Page {page_num}: Found {len(articles)} jobs")
                
                new_urls_count = 0
                for article in articles:
                    try:
                        # Get job URL - support multiple Avature URL patterns
                        link = article.find('a', href=re.compile(r'/(JobDetail|FolderDetail|PipelineDetail)/'))
                        if not link:
                            continue
                        
                        job_url = link.get('href')
                        if not job_url.startswith('http'):
                            job_url = urljoin(self.domain, job_url)
                        
                        # Extract job ID
                        job_id = self._extract_job_id(job_url)
                        
                        # Check if we already have this job
                        if job_id in existing_job_ids:
                            continue
                        
                        # Record successful URL collection
                        success = URLSuccess(
                            url=job_url,
                            job_id=job_id,
                            company=self.company_name,
                            source='html'
                        )
                        self.successes.append(success)
                        urls.append(job_url)
                        existing_job_ids.add(job_id)
                        new_urls_count += 1
                        
                    except Exception as e:
                        logger.debug(f"Error processing article: {e}")
                        failure = URLFailure(
                            url=search_url,
                            company=self.company_name,
                            source='html',
                            error_type='parse_error',
                            error_message=f'Failed to extract URL from article: {str(e)}'
                        )
                        self.failures.append(failure)
                
                logger.info(f"  → {new_urls_count} new URLs added")
                
                # Reset retry counter on successful processing
                retry_attempt = 0
                
                # Stop immediately if no URLs found on any page
                if new_urls_count == 0:
                    logger.info(f"✓ No job URLs found on page {page_num}, stopping pagination")
                    break
                
                # Stop if we found no articles at all (empty page)
                if len(articles) == 0:
                    logger.info(f"✓ Last page reached (found 0 articles on page {page_num})")
                    break
                
                # Continue as long as we find jobs - don't rely on page size comparison
                # The page size detection might be inaccurate, and some sites have variable page sizes
                # Only stop when we get an empty page or no new URLs
                
                page_num += 1
                offset += page_size
                time.sleep(3)  # Further increased rate limiting between pages to avoid 406
                
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on page {page_num}, recording for retry")
                failure = URLFailure(
                    url=f"{search_url}?page={page_num}",
                    company=self.company_name,
                    source='html',
                    error_type='timeout',
                    error_message='Request timeout'
                )
                self.retries.append(failure)
                break
                
            except requests.exceptions.ConnectionError:
                logger.warning(f"Connection error on page {page_num}, recording for retry")
                failure = URLFailure(
                    url=f"{search_url}?page={page_num}",
                    company=self.company_name,
                    source='html',
                    error_type='connection_error',
                    error_message='Connection failed'
                )
                self.retries.append(failure)
                break
                
            except Exception as e:
                logger.error(f"Unexpected error on page {page_num}: {e}")
                failure = URLFailure(
                    url=f"{search_url}?page={page_num}",
                    company=self.company_name,
                    source='html',
                    error_type='unexpected_error',
                    error_message=str(e)
                )
                self.failures.append(failure)
                break
        
        logger.info(f"HTML pagination: {len(urls)} new URLs extracted\n")
        
        return urls
    
    def _get_pagination_params(self, page_size: int, offset: int) -> Dict[str, int]:
        """
        Get correct pagination parameters based on site type
        Different Avature implementations use different parameter names
        """
        # Check page source to determine parameter style
        try:
            resp = self.session.get(f"{self.base_url}/SearchJobs/", timeout=10)
            if 'pipelineRecordsPerPage' in resp.text or 'PipelineDetail' in resp.text:
                return {'pipelineRecordsPerPage': page_size, 'pipelineOffset': offset}
            elif 'folderRecordsPerPage' in resp.text or 'FolderDetail' in resp.text:
                return {'folderRecordsPerPage': page_size, 'folderOffset': offset}
            else:
                return {'jobRecordsPerPage': page_size, 'jobOffset': offset}
        except:
            # Default to standard parameters
            return {'jobRecordsPerPage': page_size, 'jobOffset': offset}
    
    def _get_aws_waf_token(self):
        """Get AWS WAF token by solving the JavaScript challenge using Playwright"""
        try:
            # AWS WAF authentication is needed for all protected sites
            if not PLAYWRIGHT_AVAILABLE:
                logger.error("Playwright not available for AWS WAF challenge. Install with: pip install playwright")
                return
                
            logger.info("Solving AWS WAF JavaScript challenge...")
            
            with sync_playwright() as p:
                # Launch browser in headless mode
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
                page = context.new_page()
                
                # Use the current site's URL (not hardcoded Koch URL)
                target_url = self.base_url
                logger.info(f"Loading {target_url} to solve AWS WAF challenge...")
                
                # Set a reasonable timeout for the challenge
                page.set_default_timeout(30000)  # 30 seconds
                
                try:
                    # Navigate and wait for the page to load completely
                    response = page.goto(target_url, wait_until='networkidle')
                    
                    # Check if we got a challenge response
                    if response and response.status in [202, 403]:
                        logger.info("AWS WAF challenge detected, waiting for resolution...")
                        # Wait for the challenge to be solved (usually takes a few seconds)
                        page.wait_for_load_state('networkidle')
                        
                        # Wait a bit more to ensure JS challenge is fully resolved
                        time.sleep(5)  # Increased wait time for more complex challenges
                    
                    # Extract cookies after challenge is solved
                    cookies = context.cookies()
                    
                    # Find the aws-waf-token
                    waf_token = None
                    for cookie in cookies:
                        if cookie['name'] == 'aws-waf-token':
                            waf_token = cookie['value']
                            break
                    
                    if waf_token:
                        logger.info("✓ AWS WAF token obtained successfully")
                        # Set the token in our session
                        self.session.cookies.set('aws-waf-token', waf_token)
                    else:
                        logger.warning("Could not find aws-waf-token in cookies")
                    
                    # Set all other cookies that were obtained
                    for cookie in cookies:
                        self.session.cookies.set(
                            cookie['name'], 
                            cookie['value'],
                            domain=cookie.get('domain'),
                            path=cookie.get('path', '/')
                        )
                    
                    logger.info(f"✓ Set {len(cookies)} cookies from browser session")
                    
                except Exception as e:
                    logger.error(f"Error during AWS WAF challenge resolution: {e}")
                finally:
                    browser.close()
                    
            # Update headers to match browser behavior
            self.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1'
            })
                
        except Exception as e:
            logger.warning(f"Failed to solve AWS WAF challenge: {e}")
            logger.info("Falling back to manual token configuration...")
            # Fallback to the old method if Playwright fails
            self._fallback_aws_waf_config()
    
    def _fallback_aws_waf_config(self):
        """Fallback AWS WAF configuration using known working cookies"""
        try:
            logger.info("Using fallback AWS WAF configuration...")
            
            # Use some basic cookies as fallback
            fallback_cookies = {
                'portalLanguage-4': 'en_US',
                'userCookieConsent-4': '%7B%221%22%3Atrue%2C%222%22%3Atrue%2C%223%22%3Atrue%7D'
            }
            
            for name, value in fallback_cookies.items():
                self.session.cookies.set(name, value)
                
            logger.info("✓ Fallback AWS WAF configuration applied")
            
        except Exception as e:
            logger.warning(f"Fallback AWS WAF configuration failed: {e}")
    
    def _get_total_job_count(self) -> Optional[int]:
        """Get total job count from HTML page"""
        try:
            # Check if site needs AWS WAF authentication
            aws_waf_protected_domains = ['koch', 'sandboxlululemoninc']
            
            if 'koch' in self.base_url.lower():
                self._get_aws_waf_token()
                # Use the en_US URL format that works
                search_url = f"https://koch.avature.net/en_US/careers/SearchJobs/?listFilterMode=1&jobRecordsPerPage=6&"
                referer = "https://koch.avature.net/en_US/careers"
            else:
                # For other sites with AWS WAF protection, try to get auth token first
                if any(domain in self.base_url.lower() for domain in aws_waf_protected_domains):
                    self._get_aws_waf_token()
                
                # Use standard URL format for non-Koch sites
                search_url = f"{self.base_url}/SearchJobs/?listFilterMode=1&jobRecordsPerPage=6&"
                referer = self.base_url
            
            # Headers for the search request
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': referer,
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
            }
            
            resp = self.session.get(search_url, headers=headers, timeout=10)
            
            # Accept both 200 and 202 status codes
            if resp.status_code not in [200, 202]:
                logger.warning(f"HTTP {resp.status_code} when getting job count")
                return None
                
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Try multiple selectors for job count
            selectors = [
                'div.list-controls__legend',  # Updated selector
                'div.list-controls__text__legend',  # Fallback
                '.list-controls__legend',
                '.search__panel__count--span',  # New selector for amswh format
                '.pagination__legend',  # New selector for ashfieldhealthcare format
                '.legend',
                '.section__title--3'  # Koch-specific selector for "999+" jobs
            ]
            
            for selector in selectors:
                legend = soup.select_one(selector)
                if legend:
                    text = legend.get_text(strip=True)
                    logger.debug(f"Found legend text: '{text}'")
                    
                    # Try different regex patterns for job count
                    patterns = [
                        r'Showing\s+\d+-\d+\s+of\s+(\d+)\+?',  # "Showing 1-10 of 106 results" or "999+ results"
                        r'There\s+are\s+(\d+)\+?\s+jobs\s+matching',  # "There are 80 jobs matching" or "999+ jobs matching"
                        r'of\s+(\d+)\+?\s+results',  # "of 106 results" or "999+ results"  
                        r'of\s+(\d+)\+?',  # "of 106" or "999+"
                        r'(\d+)\+?\s+results',  # "106 results" or "999+ results"
                        r'(\d+)\+?\s+jobs',  # "80 jobs" or "999+ jobs"
                        r'^(\d+)\+$',  # Direct "999+" pattern (whole text)
                        r'(\d+)\+',  # Direct "999+" pattern (anywhere)
                        r'(\d+)\s*available\s*positions?',  # "999 available positions"
                        r'(\d+)\s*open\s*positions?',  # "999 open positions"
                    ]
                    
                    for pattern in patterns:
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match:
                            count = int(match.group(1))
                            logger.debug(f"Extracted job count: {count}")
                            return count
                    break
                    
        except Exception as e:
            logger.warning(f"Could not determine total job count: {e}")
        
        return None
    
    def _get_job_urls_from_sitemap(self, sitemap_url: str) -> List[str]:
        """Parse sitemap and extract job URLs"""
        try:
            logger.info(f"Fetching sitemap: {sitemap_url}")
            resp = self.session.get(sitemap_url, timeout=15)
            
            if resp.status_code != 200:
                return []
            
            soup = BeautifulSoup(resp.content, 'xml')
            locs = soup.find_all('loc')
            
            job_urls = []
            for loc in locs:
                url = loc.text.strip()
                if '/JobDetail/' in url:
                    job_urls.append(url)
            
            return job_urls
        
        except Exception as e:
            logger.debug(f"Failed to fetch sitemap: {e}")
            return []
    
    def _detect_page_size(self) -> int:
        """Detect the page size returned by the server by actually counting job containers"""
        try:
            resp = self.session.get(f"{self.base_url}/SearchJobs/", timeout=10)
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Use the same logic as the actual scraping to find job containers
            # Try multiple job container structures in order
            articles = soup.find_all('article', class_='article--result')
            
            # If no standard articles found, try alternative structures
            if not articles:
                # First try to find li elements that actually contain job links
                all_lis = soup.find_all('li')
                list_items = [li for li in all_lis if li.find('a', href=re.compile(r'/(JobDetail|FolderDetail|PipelineDetail)/'))]
                
                if list_items:
                    articles = list_items
                else:
                    # Try table rows with specific class first (DeloitteBE style)
                    table_rows = soup.find_all('tr', class_='card--box')
                    if table_rows:
                        articles = table_rows
                    else:
                        # Try any table rows that contain job links (sandboxbnc style)
                        all_trs = soup.find_all('tr')
                        table_job_rows = [tr for tr in all_trs if tr.find('a', href=re.compile(r'/(JobDetail|FolderDetail|PipelineDetail)/'))]
                        if table_job_rows:
                            articles = table_job_rows
                        else:
                            # Other possible alternative structures
                            div_jobs = soup.find_all('div', class_=lambda x: x and 'job' in x.lower())
                            if div_jobs:
                                articles = div_jobs
            
            detected = len(articles) if articles else 10  # Default to 10 instead of 12
            logger.debug(f"Auto-detected page size: {detected}")
            return detected
        except Exception as e:
            logger.debug(f"Error detecting page size: {e}")
            return 10  # Default fallback changed to 10
    
    def _deduplicate_urls(self, urls: List[str]) -> List[str]:
        """Deduplicate URLs based on job ID, keeping the first occurrence"""
        seen_job_ids = set()
        deduplicated = []
        
        for url in urls:
            try:
                job_id = self._extract_job_id(url)
                if job_id not in seen_job_ids:
                    seen_job_ids.add(job_id)
                    deduplicated.append(url)
                else:
                    logger.debug(f"Skipping duplicate job ID {job_id}: {url}")
            except Exception as e:
                # If we can't extract job ID, keep the URL to be safe
                logger.debug(f"Could not extract job ID from {url}: {e}")
                deduplicated.append(url)
        
        if len(urls) != len(deduplicated):
            logger.info(f"Removed {len(urls) - len(deduplicated)} duplicate URLs")
        
        return deduplicated
    
    def _fetch_job_detail_with_retry(self, job_url: str, source_method: str, max_retries: int = 3) -> Optional[Job]:
        """Fetch job details with retry logic and exponential backoff"""
        
        for attempt in range(max_retries + 1):
            try:
                # Add delay between retries with exponential backoff
                if attempt > 0:
                    delay = min(2 ** attempt, 10)  # Cap at 10 seconds
                    logger.debug(f"Retry {attempt}/{max_retries} for {job_url} after {delay}s")
                    time.sleep(delay)
                
                # Longer timeout for Bloomberg and other slow servers
                timeout = 30 if attempt > 0 else 25  # Increased from 15 seconds
                
                result = self._fetch_job_detail(job_url, source_method, timeout)
                
                # If we get a timeout failure or 406 error, retry with exponential backoff
                if isinstance(result, JobFailure):
                    if result.error_type == 'timeout' and attempt < max_retries:
                        continue
                    elif result.error_type == 'http_406_retry' and attempt < max_retries:
                        backoff_delay = min(60 * (2 ** attempt), 240)  # 60s, 120s, 240s
                        logger.debug(f"HTTP 406 retry {attempt + 1} for {job_url}, waiting {backoff_delay}s")
                        time.sleep(backoff_delay)
                        continue
                
                return result
                
            except Exception as e:
                if attempt < max_retries:
                    logger.debug(f"Attempt {attempt + 1} failed for {job_url}: {e}")
                    continue
                else:
                    # Final attempt failed
                    job_id = self._extract_job_id(job_url)
                    return JobFailure(
                        url=job_url,
                        job_id=job_id,
                        company=self.company_name,
                        error_type='retry_exhausted',
                        error_message=f'All {max_retries + 1} attempts failed. Last error: {str(e)}'
                    )
        
        return None

    def _fetch_job_detail(self, job_url: str, source_method: str, timeout: int = 25) -> Optional[Job]:
        """Fetch complete job details from a job detail page"""
        job_id = self._extract_job_id(job_url)
        
        try:
            resp = self.session.get(job_url, timeout=timeout)
            
            # Handle HTTP errors
            if resp.status_code == 404:
                logger.debug(f"Job not found (404): {job_url}")
                return JobFailure(
                    url=job_url,
                    job_id=job_id,
                    company=self.company_name,
                    error_type='not_found',
                    error_message='Job page returned 404 (likely removed)',
                    http_status=404
                )
            
            if resp.status_code == 406:
                logger.debug(f"HTTP 406 for {job_url}, treating as temporary error")
                return JobFailure(
                    url=job_url,
                    job_id=job_id,
                    company=self.company_name,
                    error_type='http_406_retry',
                    error_message='HTTP 406 - should be retried with delay',
                    http_status=406
                )
            
            if resp.status_code == 403:
                logger.debug(f"Access forbidden (403): {job_url}")
                return JobFailure(
                    url=job_url,
                    job_id=job_id,
                    company=self.company_name,
                    error_type='access_forbidden',
                    error_message='Job page returned 403 (access denied)',
                    http_status=403
                )
            
            if resp.status_code != 200:
                logger.debug(f"HTTP error {resp.status_code}: {job_url}")
                return JobFailure(
                    url=job_url,
                    job_id=job_id,
                    company=self.company_name,
                    error_type='http_error',
                    error_message=f'HTTP status {resp.status_code}',
                    http_status=resp.status_code
                )
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Check for "position filled" or "closed" messages
            page_text = resp.text.lower()
            
            if "position has been filled" in page_text:
                logger.debug(f"Position filled: {job_url}")
                return JobFailure(
                    url=job_url,
                    job_id=job_id,
                    company=self.company_name,
                    error_type='position_filled',
                    error_message='Job page indicates position has been filled',
                    http_status=200
                )
            
            if "no longer accepting applications" in page_text:
                logger.debug(f"Applications closed: {job_url}")
                return JobFailure(
                    url=job_url,
                    job_id=job_id,
                    company=self.company_name,
                    error_type='applications_closed',
                    error_message='Job page indicates applications are no longer accepted',
                    http_status=200
                )
            
            if "this job posting has expired" in page_text:
                logger.debug(f"Job expired: {job_url}")
                return JobFailure(
                    url=job_url,
                    job_id=job_id,
                    company=self.company_name,
                    error_type='job_expired',
                    error_message='Job posting has expired',
                    http_status=200
                )
            
            # Extract title
            title = None
            # First try Avature-specific job title selectors
            title_selectors = [
                'h2.banner__text__title',  # UCLA Health pattern
                'div.article__content__view__field__value--font .article__content__view__field__value',  # Bloomberg pattern
                'h1.title',  # Fallback
                'h1', 
                'h2'
            ]
            
            for selector in title_selectors:
                elem = soup.select_one(selector)
                if elem:
                    title = elem.get_text(strip=True)
                    # Validate it's actually a job title, not page title
                    if title and len(title) > 5 and not title.lower().endswith(' home page'):
                        break
            
            if not title:
                logger.warning(f"No title found for {job_url}")
                return JobFailure(
                    url=job_url,
                    job_id=job_id,
                    company=self.company_name,
                    error_type='missing_title',
                    error_message='Could not extract job title from page',
                    http_status=200
                )
            
            # Extract location
            location = ''
            
            # First try to find location in structured fields
            location_fields = soup.find_all('div', class_='article__content__view__field')
            for field in location_fields:
                label_elem = field.find('div', class_='article__content__view__field__label')
                value_elem = field.find('div', class_='article__content__view__field__value')
                
                if label_elem and value_elem:
                    label = label_elem.get_text(strip=True)
                    if 'Location' in label:
                        location = value_elem.get_text(strip=True)
                        break
            
            # Fallback to original selectors
            if not location:
                location_selectors = [
                    'span.list-item-location',
                    'span.location', 
                    'div.location',
                    'p.location',
                ]
                
                for selector in location_selectors:
                    elem = soup.select_one(selector)
                    if elem:
                        location = elem.get_text(strip=True)
                        break
            
            # Look for "Work Location: X" pattern in text
            if not location:
                page_text = soup.get_text()
                work_location_match = re.search(r'Work Location[:\s]*([^\n]+)', page_text, re.IGNORECASE)
                if work_location_match:
                    location = work_location_match.group(1).strip()
            
            # Generic location pattern matching as final fallback
            if not location:
                article_header = soup.select_one('div.article__header')
                if article_header:
                    text = article_header.get_text()
                    loc_match = re.search(r'([A-Z][a-zA-Z\s]+,\s*[A-Z]{2,})', text)
                    if loc_match:
                        location = loc_match.group(1).strip()
            
            # Extract description
            description = self._extract_description(soup)
            
            # Extract all metadata
            metadata = self._extract_metadata(soup)
            date_posted = metadata.get('date_posted')
            department = metadata.get('department') 
            employment_type = metadata.get('employment_type')
            
            # Extract application URL
            application_url = self._extract_application_url(soup, job_url)
            
            return Job(
                job_id=job_id,
                title=title,
                url=job_url,
                location=location,
                company=self.company_name,
                source_method=source_method,
                description=description,
                date_posted=date_posted,
                department=department,
                employment_type=employment_type,
                application_url=application_url
            )
        
        except requests.exceptions.Timeout:
            logger.debug(f"Timeout after {timeout}s: {job_url}")
            return JobFailure(
                url=job_url,
                job_id=job_id,
                company=self.company_name,
                error_type='timeout',
                error_message=f'Request timed out after {timeout} seconds'
            )
        
        except requests.exceptions.ConnectionError:
            logger.debug(f"Connection error: {job_url}")
            return JobFailure(
                url=job_url,
                job_id=job_id,
                company=self.company_name,
                error_type='connection_error',
                error_message='Failed to connect to server'
            )
        
        except Exception as e:
            logger.debug(f"Unexpected error for {job_url}: {e}")
            return JobFailure(
                url=job_url,
                job_id=job_id,
                company=self.company_name,
                error_type='parse_error',
                error_message=f'Unexpected error: {str(e)}'
            )
    
    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract clean job description"""
        # Find the main description content
        desc_selectors = [
            'div.article__content__view__field.field--rich-text',  # Primary description field
            'div.main__content',
            'div.article__body',
            'div.job-description',
            'div.description',
        ]
        
        for selector in desc_selectors:
            elem = soup.select_one(selector)
            if elem:
                # Create a copy to avoid modifying original
                elem_copy = BeautifulSoup(str(elem), 'html.parser')
                
                # Remove navigation elements and buttons
                for nav in elem_copy.find_all(['nav', 'header', 'footer']):
                    nav.decompose()
                
                # Remove "Apply Now", "Back to" and similar buttons/links
                for button in elem_copy.find_all(['a', 'button'], string=re.compile(r'Apply\s*Now|Back\s*to|Log\s*In|Save\s*this\s*Job', re.IGNORECASE)):
                    button.decompose()
                
                # Remove any remaining buttons
                for button in elem_copy.find_all(['a', 'button'], class_=re.compile(r'button')):
                    button.decompose()
                
                text = elem_copy.get_text(separator='\n', strip=True)
                text = re.sub(r'\n{3,}', '\n\n', text)
                
                if text and len(text) > 50:  # Ensure we have substantial content
                    return text
        
        return None
    
    def _extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Optional[str]]:
        """Extract all metadata fields from job posting"""
        metadata = {
            'date_posted': None,
            'department': None, 
            'employment_type': None
        }
        
        # Extract from structured field sections (Avature pattern)
        fields = soup.find_all('div', class_='article__content__view__field')
        for field in fields:
            label_elem = field.find('div', class_='article__content__view__field__label')
            value_elem = field.find('div', class_='article__content__view__field__value')
            
            if label_elem and value_elem:
                label = label_elem.get_text(strip=True)
                value = value_elem.get_text(strip=True)
                
                # Map specific fields
                if 'Posted Date' in label or 'Date Posted' in label:
                    metadata['date_posted'] = value
                elif 'Employment Type' in label or 'Job Type' in label:
                    metadata['employment_type'] = value
                elif 'Business Area' in label or 'Department' in label or 'Division' in label:
                    metadata['department'] = value
        
        # Fallback to original selectors if structured fields didn't work
        if not metadata['date_posted']:
            date_selectors = [
                'span.date-posted',
                'time',
                'span[class*="date"]',
            ]
            
            for selector in date_selectors:
                elem = soup.select_one(selector)
                if elem:
                    date_str = elem.get('datetime') or elem.get_text(strip=True)
                    if date_str:
                        metadata['date_posted'] = date_str
                        break
        
        if not metadata['department']:
            dept_selectors = [
                'span.department',
                'span.category',
                'div[class*="department"]',
            ]
            
            for selector in dept_selectors:
                elem = soup.select_one(selector)
                if elem:
                    metadata['department'] = elem.get_text(strip=True)
                    break
        
        return metadata
    
    def _extract_application_url(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Extract direct application URL"""
        from urllib.parse import urljoin
        
        apply_selectors = [
            'a.button.button--primary',  # Primary apply button
            'a[href*="Login?jobId"]',  # Avature login-based application
            'a[href*="Apply"]',
            'a[data-map="apply-button"]',
            'a.apply-button'
        ]
        
        for selector in apply_selectors:
            elem = soup.select_one(selector)
            if elem and elem.get('href'):
                href = elem.get('href')
                # Check if this looks like an application link
                if any(keyword in href.lower() for keyword in ['apply', 'login?jobid', 'application']):
                    if href.startswith('http'):
                        return href
                    else:
                        # Convert relative URL to absolute
                        parsed_base = urlparse(base_url)
                        domain = f"{parsed_base.scheme}://{parsed_base.netloc}"
                        return urljoin(domain, href)
        
        return None
    
    def _extract_job_id(self, url: str) -> str:
        """Extract job ID from URL, handling query parameters correctly"""
        # Remove query parameters first
        base_url = url.split('?')[0]
        parts = base_url.rstrip('/').split('/')
        return parts[-1] if parts else url
    
    def save_results(self):
        """Save successes, retries, and failures to separate files"""
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        safe_company_name = re.sub(r'[^\w\s-]', '', self.company_name).replace(' ', '_')
        
        files_saved = []
        
        # Save successes
        if self.successes:
            success_file = f'success_{safe_company_name}_{timestamp}.jsonl'
            with open(success_file, 'w', encoding='utf-8') as f:
                for success in self.successes:
                    f.write(json.dumps(asdict(success), ensure_ascii=False) + '\n')
            logger.info(f"✓ Saved {len(self.successes)} successful URLs to {success_file}")
            files_saved.append(success_file)
        
        # Save retries
        if self.retries:
            retry_file = self.retries_dir / f'retry_{safe_company_name}_{timestamp}.jsonl'
            with open(retry_file, 'w', encoding='utf-8') as f:
                for retry in self.retries:
                    f.write(json.dumps(asdict(retry), ensure_ascii=False) + '\n')
            logger.info(f"✓ Saved {len(self.retries)} items for retry to {retry_file}")
            files_saved.append(str(retry_file))
        
        # Save failures
        if self.failures:
            failure_file = self.failures_dir / f'failure_{safe_company_name}_{timestamp}.jsonl'
            with open(failure_file, 'w', encoding='utf-8') as f:
                for failure in self.failures:
                    f.write(json.dumps(asdict(failure), ensure_ascii=False) + '\n')
            logger.info(f"✓ Saved {len(self.failures)} failures to {failure_file}")
            files_saved.append(str(failure_file))
        
        return files_saved
    
    def _save_failures(self):
        """Save all failures to a JSONL file in failures directory"""
        if not self.failures:
            return
            
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        safe_company_name = re.sub(r'[^\w\s-]', '', self.company_name).replace(' ', '_')
        
        failures_file = self.failures_dir / f'failures_{safe_company_name}_{timestamp}.jsonl'
        
        with open(failures_file, 'w', encoding='utf-8') as f:
            for failure in self.failures:
                f.write(json.dumps(asdict(failure), ensure_ascii=False) + '\n')
        
        logger.info(f"✓ Saved {len(self.failures)} failures to {failures_file}")
        
        # Also create a summary by error type
        self._save_failure_summary(safe_company_name, timestamp)
    
    def _save_failure_summary(self, company_name: str, timestamp: str):
        """Create a summary of failures by error type"""
        summary = {}
        
        for failure in self.failures:
            error_type = failure.error_type
            if error_type not in summary:
                summary[error_type] = {
                    'count': 0,
                    'examples': []
                }
            
            summary[error_type]['count'] += 1
            
            # Keep up to 3 examples per error type
            if len(summary[error_type]['examples']) < 3:
                summary[error_type]['examples'].append({
                    'url': failure.url,
                    'job_id': failure.job_id,
                    'message': failure.error_message
                })
        
        summary_file = self.failures_dir / f'failure_summary_{company_name}_{timestamp}.json'
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump({
                'company': self.company_name,
                'total_failures': len(self.failures),
                'timestamp': timestamp,
                'breakdown_by_type': summary
            }, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✓ Saved failure summary to {summary_file}")


def load_companies_from_file(file_path: str) -> List[Tuple[str, str]]:
    """Load companies from a file containing URLs"""
    companies = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue  # Skip empty lines and comments
                
                try:
                    # Parse the URL to extract company name
                    parsed = urlparse(line)
                    if not parsed.netloc:
                        logger.warning(f"Invalid URL on line {line_num}: {line}")
                        continue
                    
                    # Extract company name from subdomain (e.g., "advocateaurorahealth" from "advocateaurorahealth.avature.net")
                    if '.avature.net' in parsed.netloc:
                        company_name = parsed.netloc.replace('.avature.net', '')
                        companies.append((company_name, line))
                    else:
                        logger.warning(f"Non-Avature URL on line {line_num}: {line}")
                        continue
                        
                except Exception as e:
                    logger.error(f"Error parsing URL on line {line_num} ({line}): {e}")
                    continue
                    
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        raise
    
    logger.info(f"Loaded {len(companies)} companies from {file_path}")
    return companies


def save_urls(urls: List[str], company_name: str) -> str:
    """Save URLs to text file"""
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    safe_company_name = re.sub(r'[^\w\s-]', '', company_name).replace(' ', '_')
    
    urls_file = f'urls_{safe_company_name}_{timestamp}.txt'
    
    with open(urls_file, 'w', encoding='utf-8') as f:
        for url in urls:
            f.write(url + '\n')
    
    logger.info(f"✓ Saved {len(urls)} URLs to {urls_file}")
    
    return urls_file


def main():
    """Main execution"""
    parser = argparse.ArgumentParser(
        description='Avature ATS Multi-Strategy URL Collector',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python job_url_extractor.py urls.txt
  python job_url_extractor.py ../job_board_finder/valid_urls_20260201_163906.txt"""
    )
    
    parser.add_argument(
        'urls_file',
        help='Path to file containing company URLs (one URL per line)'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        default=5,
        help='Maximum number of concurrent workers (default: 5)'
    )
    
    args = parser.parse_args()
    
    # Load companies from file
    try:
        companies = load_companies_from_file(args.urls_file)
    except Exception as e:
        logger.error(f"Failed to load companies: {e}")
        return
    
    if not companies:
        logger.error("No valid companies found in the file")
        return
    
    all_urls = []
    stats = {}
    
    for company_name, base_url in companies:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {company_name}")
        logger.info(f"{'='*60}\n")
        
        # Use command line argument for workers (removed Bloomberg special case since we're not fetching pages)
        max_workers = args.max_workers
        
        try:
            scraper = AvatureMultiStrategyScraper(
                company_name=company_name,
                base_url=base_url,
                max_workers=max_workers
            )
            
            start_time = time.time()
            urls = scraper.scrape_all_job_urls()
            elapsed = time.time() - start_time
            
            all_urls.extend(urls)
            
        except Exception as e:
            if "external_redirect:" in str(e):
                # Handle external redirect gracefully
                logger.error(f"{'='*60}")
                logger.error(f"REDIRECT FAILURE: {company_name}")
                logger.error(f"Original URL: {base_url}")
                logger.error(f"Reason: {str(e).replace('external_redirect: ', '')}")
                logger.error(f"{'='*60}\n")
                
                # Create mock scraper for stats recording
                elapsed = 0
                urls = []
                
                # Create a mock scraper object for consistent stats structure
                class MockScraper:
                    def __init__(self):
                        self.successes = []
                        self.retries = []
                        self.failures = []
                        self.strategy_used = "external_redirect_detected"
                        self.rss_available = False
                    
                    def save_results(self):
                        return []  # No files to save for external redirects
                
                scraper = MockScraper()
            else:
                logger.error(f"Unexpected error processing {company_name}: {e}")
                continue
        
        # Save categorized results
        saved_files = scraper.save_results()
        
        stats[company_name] = {
            'urls_found': len(urls),
            'successes': len(scraper.successes),
            'retries': len(scraper.retries),
            'failures': len(scraper.failures),
            'time_seconds': round(elapsed, 2),
            'urls_per_second': round(len(urls) / elapsed, 2) if elapsed > 0 else 0,
            'strategy_used': scraper.strategy_used,
            'rss_available': scraper.rss_available,
            'files_saved': saved_files
        }
        
        logger.info(f"\n✓ {company_name}: {len(urls)} URLs in {elapsed:.2f}s")
        logger.info(f"  Strategy: {scraper.strategy_used}")
        logger.info(f"  Successes: {len(scraper.successes)}, Retries: {len(scraper.retries)}, Failures: {len(scraper.failures)}")
    
    # Save combined results
    logger.info(f"\n{'='*60}")
    logger.info("SAVING COMBINED RESULTS")
    logger.info(f"{'='*60}\n")
    
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    combined_file = f'urls_all_companies_{timestamp}.txt'
    
    with open(combined_file, 'w', encoding='utf-8') as f:
        for url in all_urls:
            f.write(url + '\n')
    
    logger.info(f"✓ Saved {len(all_urls)} total URLs to {combined_file}")
    
    # Save statistics
    stats_file = f'url_collection_stats_{timestamp}.json'
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump({
            'total_urls': len(all_urls),
            'total_companies': len(companies),
            'timestamp': datetime.utcnow().isoformat(),
            'companies': stats
        }, f, indent=2)
    
    logger.info(f"✓ Saved statistics to {stats_file}")
    
    # Print summary
    print("\n" + "="*60)
    print("FINAL SUMMARY")
    print("="*60)
    print(f"Total URLs collected: {len(all_urls)}")
    print(f"Total companies: {len(companies)}")
    print("\nBreakdown by company:")
    for company, data in stats.items():
        print(f"  {company}:")
        print(f"    URLs: {data['urls_found']}")
        print(f"    Successes: {data['successes']}")
        print(f"    Retries: {data['retries']}")
        print(f"    Failures: {data['failures']}")
        print(f"    Time: {data['time_seconds']}s")
        print(f"    Speed: {data['urls_per_second']} URLs/sec")
        print(f"    Strategy: {data['strategy_used']}")
        print(f"    RSS Available: {data['rss_available']}")
    
    print(f"\nOutput files:")
    print(f"  - {combined_file} (all URLs)")
    print(f"  - {stats_file} (statistics)")
    print(f"  - success_*.jsonl (successful URLs)")
    print(f"  - retries/*.jsonl (items for retry)")
    print(f"  - failures/*.jsonl (permanent failures)")
    print("="*60)


if __name__ == "__main__":
    main()
