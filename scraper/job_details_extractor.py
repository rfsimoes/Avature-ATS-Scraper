"""
Avature Job Details Extractor
Standalone component to extract job details from job URLs
Based on patterns from hybrid_scraper.py
"""

import requests
import json
import time
import logging
import re
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Set, Tuple, Union
from dataclasses import dataclass, asdict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class Job:
    """Job posting data model - reused from hybrid_scraper.py"""
    job_id: str
    title: str
    url: str
    location: str
    company: str
    source_method: str = "url_list"  # Default for this extractor
    description: Optional[str] = None
    date_posted: Optional[str] = None
    department: Optional[str] = None
    employment_type: Optional[str] = None
    application_url: Optional[str] = None
    scraped_at: str = None
    
    def __post_init__(self):
        if self.scraped_at is None:
            self.scraped_at = datetime.utcnow().isoformat()


@dataclass
class JobFailure:
    """Failed job extraction record - reused from hybrid_scraper.py"""
    url: str
    job_id: str
    company: str
    error_type: str
    error_message: str
    http_status: Optional[int] = None
    timestamp: str = None
    retry_count: int = 0
    is_retryable: bool = False
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat()
        
        # Always determine if failure is retryable based on current data
        self.is_retryable = self._determine_retryable()
    
    def _determine_retryable(self) -> bool:
        """Determine if this failure type should be retried"""
        retryable_types = {
            'timeout', 'connection_error', 'rate_limited', 
            'server_error', 'temporary_error'
        }
        retryable_status_codes = {406, 429, 500, 502, 503, 504}
        
        error_type_check = self.error_type in retryable_types
        status_code_check = self.http_status in retryable_status_codes if self.http_status else False
        
        return error_type_check or status_code_check


class AvatureJobDetailsExtractor:
    """
    Standalone job details extractor for Avature ATS
    Extracts comprehensive job information from job detail URLs
    """
    
    def __init__(self, max_workers: int = 5, timeout: int = 25, max_retries: int = 3):
        self.max_workers = max_workers
        self.timeout = timeout
        self.max_retries = max_retries
        
        # Session setup - reused from hybrid_scraper.py
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Tracking
        self.jobs_extracted = 0
        self.failures: List[JobFailure] = []
        self.retryable_failures: List[JobFailure] = []
        self.permanent_failures: List[JobFailure] = []
        
        # Enhanced rate limiting for 406 prevention
        self.request_delay = 0.5  # 500ms between requests (increased from 200ms)
        self.adaptive_delay = 0.5  # Base adaptive delay
        self.max_adaptive_delay = 5.0  # Maximum adaptive delay
        self.recent_406_count = 0  # Track recent 406 errors
        self.last_request_time = 0  # Track timing for rate limiting
    
    def extract_from_urls(self, job_urls: List[str], company_name: str = None) -> Tuple[List[Job], List[JobFailure]]:
        """
        Extract job details from a list of URLs
        Returns: (successful_jobs, all_failures)
        """
        start_time = time.time()
        logger.info(f"Starting extraction for {len(job_urls)} job URLs")
        logger.info(f"Workers: {self.max_workers}, Timeout: {self.timeout}s, Max retries: {self.max_retries}")
        logger.info(f"Estimated time (conservative): {len(job_urls) * self.timeout / self.max_workers / 60:.1f} minutes")
        
        jobs = []
        failed = 0
        last_progress_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {}
            
            logger.info(f"📤 Submitting {len(job_urls)} jobs to {self.max_workers} workers...")
            
            # Submit jobs with enhanced staggered timing to prevent 406 errors
            for i, url in enumerate(job_urls):
                if i > 0 and i % self.max_workers == 0:
                    # Longer delay between batches to reduce server load
                    batch_delay = 0.2 + (self.adaptive_delay * 0.5)  # Scale with adaptive delay
                    time.sleep(batch_delay)
                
                # Progress during submission for large batches
                if i > 0 and i % 1000 == 0:
                    logger.info(f"📤 Submitted {i}/{len(job_urls)} jobs to workers...")
                
                # Extract company name from URL if not provided
                if not company_name:
                    company_name = self._extract_company_from_url(url)
                
                future = executor.submit(self._fetch_job_detail_with_retry, url, company_name)
                future_to_url[future] = url
            
            logger.info(f"✅ All {len(job_urls)} jobs submitted. Workers are processing...")
            
            # Process results
            for i, future in enumerate(as_completed(future_to_url), 1):
                url = future_to_url[future]
                try:
                    result = future.result()
                    if isinstance(result, Job):
                        jobs.append(result)
                        self.jobs_extracted += 1
                        self._reset_adaptive_delay_if_needed()  # Reset 406 count periodically
                    elif isinstance(result, JobFailure):
                        self.failures.append(result)
                        if result.is_retryable:
                            self.retryable_failures.append(result)
                        else:
                            self.permanent_failures.append(result)
                        failed += 1
                    
                except Exception as e:
                    logger.error(f"Unexpected error processing {url}: {e}")
                    failure = JobFailure(
                        url=url,
                        job_id=self._extract_job_id(url),
                        company=company_name or "unknown",
                        error_type='processing_error',
                        error_message=f'Unexpected processing error: {str(e)}'
                    )
                    self.failures.append(failure)
                    failed += 1
                
                # Enhanced progress logging
                current_time = time.time()
                
                # More frequent updates for large batches
                progress_interval = min(25, max(10, len(job_urls) // 100))  # Adaptive interval
                
                if i % progress_interval == 0 or (current_time - last_progress_time) >= 60:  # Every interval or 60 seconds
                    elapsed = current_time - start_time
                    rate = i / elapsed if elapsed > 0 else 0
                    eta_seconds = (len(job_urls) - i) / rate if rate > 0 else 0
                    eta_minutes = eta_seconds / 60
                    
                    logger.info(
                        f"📊 Progress: {i}/{len(job_urls)} ({i/len(job_urls)*100:.1f}%) | "
                        f"✅ {len(jobs)} successful, ❌ {failed} failed | "
                        f"⚡ {rate:.1f}/sec | ETA: {eta_minutes:.1f}min"
                    )
                    last_progress_time = current_time
                
                # Enhanced rate limiting to prevent 406 errors
                processing_delay = max(0.1, self.request_delay * 0.5)  # Minimum 100ms, scale with request delay
                time.sleep(processing_delay)
        
        total_time = time.time() - start_time
        success_rate = len(jobs) / (len(jobs) + failed) * 100 if (len(jobs) + failed) > 0 else 0
        throughput = len(job_urls) / total_time if total_time > 0 else 0
        
        logger.info(f"🎉 Extraction complete in {total_time:.1f}s: {len(jobs)} jobs, {failed} failures")
        logger.info(f"📈 Success rate: {success_rate:.1f}%, Throughput: {throughput:.2f} jobs/sec")
        logger.info(f"🔄 Retryable failures: {len(self.retryable_failures)}")
        logger.info(f"🚫 Permanent failures: {len(self.permanent_failures)}")
        
        return jobs, self.failures
    
    def _fetch_job_detail_with_retry(self, job_url: str, company_name: str) -> Union[Job, JobFailure]:
        """
        Fetch job details with retry logic and exponential backoff
        Based on hybrid_scraper.py implementation
        """
        job_id = self._extract_job_id(job_url)
        
        for attempt in range(self.max_retries + 1):
            try:
                # Add delay between retries with enhanced exponential backoff
                if attempt > 0:
                    base_delay = 3 ** attempt  # More aggressive backoff (3^n vs 2^n)
                    # Longer delays for rate-limiting errors (406, 429)
                    if hasattr(self, 'recent_406_count') and self.recent_406_count > 0:
                        base_delay *= 2  # Double delay when we've had recent 406s
                    delay = min(base_delay, 30)  # Cap at 30 seconds (increased from 10)
                    logger.debug(f"Retry {attempt} for {job_url} after {delay}s (recent 406s: {getattr(self, 'recent_406_count', 0)})")
                    time.sleep(delay)
                
                # Longer timeout for retries
                timeout = self.timeout + (10 * attempt)
                
                result = self._fetch_job_detail(job_url, company_name, timeout)
                
                # If we get a retryable failure, continue trying
                if isinstance(result, JobFailure) and result.is_retryable and attempt < self.max_retries:
                    result.retry_count = attempt + 1
                    continue
                
                return result
                
            except Exception as e:
                if attempt < self.max_retries:
                    logger.debug(f"Attempt {attempt + 1} failed for {job_url}: {e}")
                else:
                    return JobFailure(
                        url=job_url,
                        job_id=job_id,
                        company=company_name,
                        error_type='retry_exhausted',
                        error_message=f'All {self.max_retries + 1} attempts failed. Last error: {str(e)}',
                        retry_count=self.max_retries
                    )
        
        return JobFailure(
            url=job_url,
            job_id=job_id,
            company=company_name,
            error_type='retry_exhausted',
            error_message=f'Failed after {self.max_retries + 1} attempts'
        )

    def _fetch_job_detail(self, job_url: str, company_name: str, timeout: int) -> Union[Job, JobFailure]:
        """
        Fetch complete job details from a job detail page
        Based on hybrid_scraper.py implementation with enhancements
        """
        job_id = self._extract_job_id(job_url)
        
        try:
            resp = self._rate_limited_request(job_url, timeout)
            
            # Handle HTTP errors - based on hybrid_scraper.py
            if resp.status_code == 404:
                return JobFailure(
                    url=job_url, job_id=job_id, company=company_name,
                    error_type='not_found', error_message='Job page returned 404 (likely removed)',
                    http_status=404
                )
            
            if resp.status_code == 403:
                return JobFailure(
                    url=job_url, job_id=job_id, company=company_name,
                    error_type='access_forbidden', error_message='Job page returned 403 (access denied)',
                    http_status=403
                )
            
            if resp.status_code == 406:
                # Track 406 errors for adaptive rate limiting
                self.recent_406_count += 1
                self._adjust_adaptive_delay()
                return JobFailure(
                    url=job_url, job_id=job_id, company=company_name,
                    error_type='rate_limited', error_message='Rate limited (406) - not acceptable/too many requests',
                    http_status=406
                )
            
            if resp.status_code == 429:
                return JobFailure(
                    url=job_url, job_id=job_id, company=company_name,
                    error_type='rate_limited', error_message='Rate limited (429) - too many requests',
                    http_status=429
                )
            
            if resp.status_code >= 500:
                return JobFailure(
                    url=job_url, job_id=job_id, company=company_name,
                    error_type='server_error', error_message=f'Server error {resp.status_code}',
                    http_status=resp.status_code
                )
            
            if resp.status_code != 200:
                return JobFailure(
                    url=job_url, job_id=job_id, company=company_name,
                    error_type='http_error', error_message=f'HTTP status {resp.status_code}',
                    http_status=resp.status_code
                )
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Check for content state indicators - based on hybrid_scraper.py
            page_text = resp.text.lower()
            
            if "position has been filled" in page_text:
                return JobFailure(
                    url=job_url, job_id=job_id, company=company_name,
                    error_type='position_filled', error_message='Job page indicates position has been filled',
                    http_status=200
                )
            
            if "no longer accepting applications" in page_text:
                return JobFailure(
                    url=job_url, job_id=job_id, company=company_name,
                    error_type='applications_closed', error_message='Job page indicates applications are no longer accepted',
                    http_status=200
                )
            
            if "this job posting has expired" in page_text:
                return JobFailure(
                    url=job_url, job_id=job_id, company=company_name,
                    error_type='job_expired', error_message='Job posting has expired',
                    http_status=200
                )
            
            # Extract job fields
            title = self._extract_title(soup)
            if not title:
                return JobFailure(
                    url=job_url, job_id=job_id, company=company_name,
                    error_type='missing_title', error_message='Could not extract job title from page',
                    http_status=200
                )
            
            location = self._extract_location(soup)
            description = self._extract_description(soup)
            metadata = self._extract_metadata(soup)
            application_url = self._extract_application_url(soup, job_url)
            
            return Job(
                job_id=job_id,
                title=title,
                url=job_url,
                location=location,
                company=company_name,
                description=description,
                date_posted=metadata.get('date_posted'),
                department=metadata.get('department'),
                employment_type=metadata.get('employment_type'),
                application_url=application_url
            )
        
        except requests.exceptions.Timeout:
            return JobFailure(
                url=job_url, job_id=job_id, company=company_name,
                error_type='timeout', error_message=f'Request timed out after {timeout} seconds'
            )
        
        except requests.exceptions.ConnectionError:
            return JobFailure(
                url=job_url, job_id=job_id, company=company_name,
                error_type='connection_error', error_message='Failed to connect to server'
            )
        
        except Exception as e:
            return JobFailure(
                url=job_url, job_id=job_id, company=company_name,
                error_type='parse_error', error_message=f'Unexpected error: {str(e)}'
            )

    def _adjust_adaptive_delay(self):
        """Adjust adaptive delay based on 406 error frequency"""
        if self.recent_406_count > 0:
            # Increase delay progressively with more 406 errors
            multiplier = min(self.recent_406_count * 0.5, 4.0)  # Cap at 4x
            self.adaptive_delay = min(self.request_delay * (1 + multiplier), self.max_adaptive_delay)
            logger.warning(f"⚠️ Increased adaptive delay to {self.adaptive_delay:.2f}s due to {self.recent_406_count} 406 errors")

    def _rate_limited_request(self, job_url: str, timeout: int):
        """Make a rate-limited request with adaptive delays"""
        current_time = time.time()
        
        # Apply base delay plus adaptive delay
        total_delay = self.request_delay + self.adaptive_delay
        
        # Ensure minimum time between requests
        if self.last_request_time > 0:
            time_since_last = current_time - self.last_request_time
            if time_since_last < total_delay:
                sleep_time = total_delay - time_since_last
                logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s before request")
                time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        return self.session.get(job_url, timeout=timeout)

    def _reset_adaptive_delay_if_needed(self):
        """Reset 406 count periodically to avoid permanent rate limiting"""
        if self.jobs_extracted > 0 and self.jobs_extracted % 100 == 0:  # Every 100 successful jobs
            if self.recent_406_count > 0:
                logger.info(f"💫 Resetting 406 count (was {self.recent_406_count}) after {self.jobs_extracted} successful extractions")
                self.recent_406_count = max(0, self.recent_406_count - 2)  # Reduce gradually
                self._adjust_adaptive_delay()

    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract job title - based on hybrid_scraper.py patterns"""
        title_selectors = [
            'h2.banner__text__title',  # UCLA Health pattern
            'div.article__content__view__field__value--font .article__content__view__field__value',  # Bloomberg pattern
            'h1.title',  # Fallback
            'h1', 'h2'
        ]
        
        for selector in title_selectors:
            elem = soup.select_one(selector)
            if elem:
                title = elem.get_text(strip=True)
                if title and len(title) > 2:
                    return title
        
        return None

    def _extract_location(self, soup: BeautifulSoup) -> str:
        """Extract job location - enhanced from hybrid_scraper.py"""
        location = ''
        
        # First try structured fields
        location_fields = soup.find_all('div', class_='article__content__view__field')
        for field in location_fields:
            label_elem = field.find('div', class_='article__content__view__field__label')
            value_elem = field.find('div', class_='article__content__view__field__value')
            
            if label_elem and value_elem:
                label_text = label_elem.get_text(strip=True).lower()
                if any(keyword in label_text for keyword in ['location', 'work location', 'office location']):
                    location = value_elem.get_text(strip=True)
                    break
        
        # Fallback selectors
        if not location:
            location_selectors = [
                'span.list-item-location', 'span.location', 'div.location', 'p.location'
            ]
            
            for selector in location_selectors:
                elem = soup.select_one(selector)
                if elem:
                    location = elem.get_text(strip=True)
                    break
        
        # Pattern matching fallback
        if not location:
            page_text = soup.get_text()
            work_location_match = re.search(r'Work Location[:\s]*([^\n]+)', page_text, re.IGNORECASE)
            if work_location_match:
                location = work_location_match.group(1).strip()
        
        return location or 'Not specified'

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract job description - based on hybrid_scraper.py"""
        desc_selectors = [
            'div.article__content__view__field.field--rich-text',
            'div.main__content',
            'div.article__body',
            'div.job-description',
            'div.description'
        ]
        
        for selector in desc_selectors:
            elem = soup.select_one(selector)
            if elem:
                # Create copy to avoid modifying original
                elem_copy = BeautifulSoup(str(elem), 'html.parser')
                
                # Remove navigation and buttons
                for nav in elem_copy.find_all(['nav', 'header', 'footer']):
                    nav.decompose()
                
                for button in elem_copy.find_all(['a', 'button'], 
                    string=re.compile(r'Apply\s*Now|Back\s*to|Log\s*In|Save\s*this\s*Job', re.IGNORECASE)):
                    button.decompose()
                
                for button in elem_copy.find_all(['a', 'button'], class_=re.compile(r'button')):
                    button.decompose()
                
                text = elem_copy.get_text(separator='\n', strip=True)
                text = re.sub(r'\n{3,}', '\n\n', text)
                
                if text and len(text) > 50:
                    return text
        
        return None

    def _extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Optional[str]]:
        """Extract metadata fields - based on hybrid_scraper.py"""
        metadata = {
            'date_posted': None,
            'department': None,
            'employment_type': None
        }
        
        # Structured field extraction
        fields = soup.find_all('div', class_='article__content__view__field')
        for field in fields:
            label_elem = field.find('div', class_='article__content__view__field__label')
            value_elem = field.find('div', class_='article__content__view__field__value')
            
            if label_elem and value_elem:
                label = label_elem.get_text(strip=True).lower()
                value = value_elem.get_text(strip=True)
                
                if any(keyword in label for keyword in ['posted date', 'date posted']):
                    metadata['date_posted'] = value
                elif any(keyword in label for keyword in ['employment type', 'job type']):
                    metadata['employment_type'] = value
                elif any(keyword in label for keyword in ['business area', 'department', 'division']):
                    metadata['department'] = value
        
        return metadata

    def _extract_application_url(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Extract application URL - based on hybrid_scraper.py"""
        apply_selectors = [
            'a.button.button--primary',
            'a[href*="Login?jobId"]',
            'a[href*="Apply"]',
            'a[data-map="apply-button"]',
            'a.apply-button'
        ]
        
        for selector in apply_selectors:
            elem = soup.select_one(selector)
            if elem and elem.get('href'):
                href = elem.get('href')
                if any(keyword in href.lower() for keyword in ['apply', 'login?jobid', 'application']):
                    return urljoin(base_url, href)
        
        return None

    def _extract_job_id(self, url: str) -> str:
        """Extract job ID from URL - reused from hybrid_scraper.py"""
        parts = url.rstrip('/').split('/')
        return parts[-1] if parts else url

    def _extract_company_from_url(self, url: str) -> str:
        """Extract company name from Avature URL"""
        try:
            parsed = urlparse(url)
            # For Avature URLs like "company.avature.net"
            hostname_parts = parsed.hostname.split('.')
            if 'avature' in hostname_parts:
                # Get the subdomain before 'avature'
                avature_index = hostname_parts.index('avature')
                if avature_index > 0:
                    return hostname_parts[avature_index - 1]
            
            # Fallback to first part of hostname
            return hostname_parts[0] if hostname_parts else 'unknown'
        except:
            return 'unknown'