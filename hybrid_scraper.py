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
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from urllib.parse import urljoin, urlparse
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
        self.base_url = base_url.rstrip('/')
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
        
        self.jobs_scraped = 0
        self.failures: List[JobFailure] = []
        
        # Strategy tracking
        self.strategy_used = None
        self.rss_available = False
        
        # Create failures directory
        self.failures_dir = Path('failures')
        self.failures_dir.mkdir(exist_ok=True)
    
    def scrape_all_jobs(self) -> List[Job]:
        """
        Multi-strategy scraping with intelligent fallbacks
        """
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
        sitemap_jobs, sitemap_job_ids = self._try_sitemap_strategy()
        
        # Step 3: Smart gap detection
        all_jobs = sitemap_jobs
        
        if not sitemap_jobs:
            # No sitemap or completely failed - use HTML only
            logger.info("⚠️  Sitemap strategy failed, using HTML pagination")
            all_jobs = self._scrape_via_html_pagination(set(), total_jobs_html)
            self.strategy_used = "html_only"
        
        else:
            # Sitemap worked - check if HTML has any jobs we don't have
            logger.info("VERIFICATION: Checking for jobs not in sitemap...")
            logger.info("-" * 60)
            
            # Sample first 3 pages of HTML to detect gaps
            sample_jobs = self._check_html_sample(sitemap_job_ids, pages=3)
            
            if sample_jobs:
                logger.info(f"✓ Found {len(sample_jobs)} jobs in HTML not in sitemap")
                logger.info(f"Running full HTML pagination to capture all missing jobs...\n")
                
                # Do full HTML scrape
                html_jobs = self._scrape_via_html_pagination(sitemap_job_ids, total_jobs_html)
                all_jobs = sitemap_jobs + html_jobs
                self.strategy_used = "sitemap_plus_html"
            else:
                logger.info(f"✓ No gaps detected - sitemap appears complete\n")
                self.strategy_used = "sitemap_only"
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Strategy used: {self.strategy_used}")
        logger.info(f"Final count: {len(all_jobs)} jobs for {self.company_name}")
        logger.info(f"{'='*60}\n")
        
        return all_jobs
    
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
    
    def _try_sitemap_strategy(self) -> Tuple[List[Job], Set[str]]:
        """
        Try to scrape from sitemap
        Returns: (jobs_list, job_ids_set)
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
        
        jobs = []
        job_ids = set()
        failed = 0
        
        # Add rate limiting between job requests
        request_delay = 0.2  # 200ms between requests to avoid overwhelming server
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {}
            
            # Submit jobs with staggered timing
            for i, url in enumerate(job_urls):
                # Add small delay between submissions to spread out requests
                if i > 0 and i % self.max_workers == 0:
                    time.sleep(request_delay * self.max_workers)
                
                future = executor.submit(self._fetch_job_detail_with_retry, url, 'sitemap')
                future_to_url[future] = url
            
            for i, future in enumerate(as_completed(future_to_url), 1):
                url = future_to_url[future]
                try:
                    result = future.result()
                    if result:
                        if isinstance(result, Job):
                            jobs.append(result)
                            job_ids.add(result.job_id)
                        elif isinstance(result, JobFailure):
                            self.failures.append(result)
                            failed += 1
                    else:
                        failed += 1
                except Exception as e:
                    logger.warning(f"Error processing {url}: {e}")
                    job_id = self._extract_job_id(url)
                    failure = JobFailure(
                        url=url,
                        job_id=job_id,
                        company=self.company_name,
                        error_type='exception',
                        error_message=str(e)
                    )
                    self.failures.append(failure)
                    failed += 1
                
                if i % 25 == 0:  # Report progress less frequently
                    logger.info(f"Progress: {i}/{len(job_urls)} ({len(jobs)} successful, {failed} failed)")
                
                # Small delay between processing results
                time.sleep(0.05)
        
        logger.info(f"Sitemap extraction: {len(jobs)} jobs, {failed} failures\n")
        
        return jobs, job_ids
    
    def _check_html_sample(self, existing_job_ids: Set[str], pages: int = 3) -> List[Job]:
        """
        Check first N pages of HTML for jobs not in sitemap
        This detects if there are new jobs posted after sitemap was generated
        """
        new_jobs = []
        page_size = self._detect_page_size()
        search_url = f"{self.base_url}/SearchJobs/"
        
        for page_num in range(1, pages + 1):
            try:
                offset = (page_num - 1) * page_size
                params = {'jobRecordsPerPage': page_size, 'jobOffset': offset}
                resp = self.session.get(search_url, params=params, timeout=10)
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                articles = soup.find_all('article', class_='article--result')
                if not articles:
                    break
                
                for article in articles:
                    link = article.find('a', href=re.compile(r'/JobDetail/'))
                    if not link:
                        continue
                    
                    job_url = link.get('href')
                    if not job_url.startswith('http'):
                        job_url = urljoin(self.domain, job_url)
                    
                    job_id = self._extract_job_id(job_url)
                    
                    # Check if this is a new job not in sitemap
                    if job_id not in existing_job_ids:
                        new_jobs.append(job_id)
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.debug(f"Error checking HTML sample page {page_num}: {e}")
                break
        
        logger.info(f"Checked {pages} pages, found {len(new_jobs)} job(s) not in sitemap")
        
        return new_jobs
    
    def _scrape_via_html_pagination(self, existing_job_ids: Set[str], total_expected: Optional[int]) -> List[Job]:
        """
        Scrape jobs using HTML pagination
        Only scrapes jobs not already in existing_job_ids
        """
        logger.info("STRATEGY 2: HTML Pagination")
        logger.info("-" * 60)
        
        jobs = []
        page_num = 1
        offset = 0
        
        # Detect page size
        page_size = self._detect_page_size()
        logger.info(f"Detected page size: {page_size} jobs per page")
        
        search_url = f"{self.base_url}/SearchJobs/"
        
        if total_expected:
            estimated_pages = (total_expected + page_size - 1) // page_size
            logger.info(f"Estimated pages needed: {estimated_pages}\n")
        
        while True:
            try:
                params = {'jobRecordsPerPage': page_size, 'jobOffset': offset}
                resp = self.session.get(search_url, params=params, timeout=15)
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                # Extract job URLs from this page
                articles = soup.find_all('article', class_='article--result')
                if not articles:
                    logger.info(f"No more jobs found on page {page_num}")
                    break
                
                logger.info(f"Page {page_num}: Found {len(articles)} jobs")
                
                new_jobs_count = 0
                for article in articles:
                    # Get job URL
                    link = article.find('a', href=re.compile(r'/JobDetail/'))
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
                    
                    # Fetch full details with retry
                    job = self._fetch_job_detail_with_retry(job_url, 'html')
                    if job:
                        if isinstance(job, Job):
                            jobs.append(job)
                            existing_job_ids.add(job_id)
                            new_jobs_count += 1
                        elif isinstance(job, JobFailure):
                            self.failures.append(job)
                    
                    time.sleep(0.3)  # Increased rate limiting within page
                
                logger.info(f"  → {new_jobs_count} new jobs added")
                
                # Check if we should continue
                if total_expected and len(existing_job_ids) >= total_expected:
                    logger.info(f"✓ Reached expected total of {total_expected} jobs")
                    break
                
                if len(articles) < page_size:
                    logger.info(f"✓ Last page reached (got {len(articles)} < {page_size})")
                    break
                
                page_num += 1
                offset += page_size
                time.sleep(2)  # Increased rate limiting between pages
                
            except Exception as e:
                logger.error(f"Error on page {page_num}: {e}")
                break
        
        logger.info(f"HTML pagination: {len(jobs)} new jobs extracted\n")
        
        return jobs
    
    def _get_total_job_count(self) -> Optional[int]:
        """Get total job count from HTML page"""
        try:
            resp = self.session.get(f"{self.base_url}/SearchJobs/", timeout=10)
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            legend = soup.find('div', class_='list-controls__text__legend')
            if legend:
                match = re.search(r'of\s+(\d+)', legend.text)
                if match:
                    return int(match.group(1))
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
        """Detect the page size returned by the server"""
        try:
            resp = self.session.get(f"{self.base_url}/SearchJobs/", timeout=10)
            soup = BeautifulSoup(resp.text, 'html.parser')
            articles = soup.find_all('article', class_='article--result')
            detected = len(articles) if articles else 12
            logger.debug(f"Auto-detected page size: {detected}")
            return detected
        except:
            return 12  # Default fallback
    
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
                
                # If we get a timeout failure, retry
                if isinstance(result, JobFailure) and result.error_type == 'timeout':
                    if attempt < max_retries:
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
        """Extract job ID from URL"""
        parts = url.rstrip('/').split('/')
        return parts[-1] if parts else url
    
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


def save_results(jobs: List[Job], company_name: str) -> str:
    """Save jobs to JSONL"""
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    safe_company_name = re.sub(r'[^\w\s-]', '', company_name).replace(' ', '_')
    
    jobs_file = f'jobs_{safe_company_name}_{timestamp}.jsonl'
    
    with open(jobs_file, 'w', encoding='utf-8') as f:
        for job in jobs:
            f.write(json.dumps(asdict(job), ensure_ascii=False) + '\n')
    
    logger.info(f"✓ Saved {len(jobs)} jobs to {jobs_file}")
    
    return jobs_file


def main():
    """Main execution"""
    
    companies = [
        ("Bloomberg", "https://bloomberg.avature.net/careers"),
        ("UCLA Health", "https://uclahealth.avature.net/careers"),
    ]
    
    all_jobs = []
    stats = {}
    
    for company_name, base_url in companies:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {company_name}")
        logger.info(f"{'='*60}\n")
        
        # Reduced workers for Bloomberg specifically
        max_workers = 3 if 'bloomberg' in base_url.lower() else 5
        
        scraper = AvatureMultiStrategyScraper(
            company_name=company_name,
            base_url=base_url,
            max_workers=max_workers
        )
        
        start_time = time.time()
        jobs = scraper.scrape_all_jobs()
        elapsed = time.time() - start_time
        
        all_jobs.extend(jobs)
        
        # Save individual company results
        if jobs:
            save_results(jobs, company_name)
        
        # Save failures
        if scraper.failures:
            scraper._save_failures()
        
        # Count jobs by source method
        source_breakdown = {}
        for job in jobs:
            method = job.source_method
            source_breakdown[method] = source_breakdown.get(method, 0) + 1
        
        stats[company_name] = {
            'jobs_found': len(jobs),
            'time_seconds': round(elapsed, 2),
            'failures': len(scraper.failures),
            'jobs_per_second': round(len(jobs) / elapsed, 2) if elapsed > 0 else 0,
            'strategy_used': scraper.strategy_used,
            'rss_available': scraper.rss_available,
            'source_breakdown': source_breakdown
        }
        
        logger.info(f"\n✓ {company_name}: {len(jobs)} jobs in {elapsed:.2f}s")
        logger.info(f"  Strategy: {scraper.strategy_used}")
        logger.info(f"  Source breakdown: {source_breakdown}")
    
    # Save combined results
    logger.info(f"\n{'='*60}")
    logger.info("SAVING COMBINED RESULTS")
    logger.info(f"{'='*60}\n")
    
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    combined_file = f'jobs_all_companies_{timestamp}.jsonl'
    
    with open(combined_file, 'w', encoding='utf-8') as f:
        for job in all_jobs:
            f.write(json.dumps(asdict(job), ensure_ascii=False) + '\n')
    
    logger.info(f"✓ Saved {len(all_jobs)} total jobs to {combined_file}")
    
    # Save statistics
    stats_file = f'scrape_stats_{timestamp}.json'
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump({
            'total_jobs': len(all_jobs),
            'total_companies': len(companies),
            'timestamp': datetime.utcnow().isoformat(),
            'companies': stats
        }, f, indent=2)
    
    logger.info(f"✓ Saved statistics to {stats_file}")
    
    # Print summary
    print("\n" + "="*60)
    print("FINAL SUMMARY")
    print("="*60)
    print(f"Total jobs scraped: {len(all_jobs)}")
    print(f"Total companies: {len(companies)}")
    print("\nBreakdown by company:")
    for company, data in stats.items():
        print(f"  {company}:")
        print(f"    Jobs: {data['jobs_found']}")
        print(f"    Time: {data['time_seconds']}s")
        print(f"    Speed: {data['jobs_per_second']} jobs/sec")
        print(f"    Strategy: {data['strategy_used']}")
        print(f"    RSS Available: {data['rss_available']}")
        print(f"    Sources: {data['source_breakdown']}")
        if data['failures'] > 0:
            print(f"    Failures: {data['failures']}")
    
    print(f"\nOutput files:")
    print(f"  - {combined_file} (all jobs)")
    print(f"  - {stats_file} (statistics)")
    print(f"  - failures/ (detailed failure logs)")
    print("="*60)


if __name__ == "__main__":
    main()
