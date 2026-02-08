"""
URL Processor for Job Details Extractor
Handles multiple input formats and URL validation
"""

import json
import logging
from typing import List, Dict, Optional, Union, Iterator
from pathlib import Path
from urllib.parse import urlparse
import re

logger = logging.getLogger(__name__)


class URLProcessor:
    """
    Processes various input formats for job URLs
    Supports: plain text, JSONL from url extractor, retry files
    """
    
    def __init__(self):
        self.supported_formats = ['txt', 'jsonl', 'json']
        self.avature_patterns = [
            r'\.avature\.net',
            r'/careers',
            r'/JobDetail/',
            r'/SearchJobs/'
        ]
    
    def process_input_file(self, input_file: str, company_filter: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Process input file and return list of job URLs with metadata
        Returns: [{'url': str, 'company': str, 'source': str, 'metadata': dict}, ...]
        """
        input_path = Path(input_file)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        file_extension = input_path.suffix.lower().lstrip('.')
        
        if file_extension == 'txt':
            urls = self._process_txt_file(input_path)
        elif file_extension in ['jsonl', 'json']:
            urls = self._process_json_file(input_path)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
        
        # Filter by company if specified
        if company_filter:
            urls = [url for url in urls if company_filter.lower() in url['company'].lower()]
            logger.info(f"Filtered to {len(urls)} URLs for company: {company_filter}")
        
        # Validate URLs
        valid_urls = self._validate_urls(urls)
        
        logger.info(f"Processed {len(valid_urls)} valid URLs from {input_file}")
        
        return valid_urls
    
    def _process_txt_file(self, file_path: Path) -> List[Dict[str, str]]:
        """Process plain text file with URLs (one per line)"""
        urls = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                
                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    continue
                
                # Extract company name from URL
                company = self._extract_company_from_url(line)
                
                urls.append({
                    'url': line,
                    'company': company,
                    'source': 'txt_file',
                    'metadata': {
                        'line_number': line_num,
                        'file': str(file_path)
                    }
                })
        
        logger.info(f"Read {len(urls)} URLs from text file: {file_path}")
        return urls
    
    def _process_json_file(self, file_path: Path) -> List[Dict[str, str]]:
        """Process JSONL or JSON file"""
        urls = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            
            # Try to detect format
            if content.startswith('[') or content.startswith('{') and '"failures"' in content:
                # JSON format (possibly retry file)
                data = json.loads(content)
                
                if isinstance(data, list):
                    # Simple JSON array
                    urls = self._process_json_array(data, file_path)
                elif 'failures' in data:
                    # Retry file format
                    urls = self._process_retry_file(data, file_path)
                else:
                    # Single JSON object - try to extract URLs
                    urls = self._extract_urls_from_object(data, file_path)
            
            else:
                # JSONL format
                urls = self._process_jsonl_file(file_path)
        
        logger.info(f"Read {len(urls)} URLs from JSON file: {file_path}")
        return urls
    
    def _process_jsonl_file(self, file_path: Path) -> List[Dict[str, str]]:
        """Process JSONL file (one JSON object per line)"""
        urls = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    data = json.loads(line)
                    url_data = self._extract_url_from_json_object(data, line_num, file_path)
                    if url_data:
                        urls.append(url_data)
                
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON on line {line_num} in {file_path}: {e}")
                    continue
        
        return urls
    
    def _process_json_array(self, data: List, file_path: Path) -> List[Dict[str, str]]:
        """Process JSON array of URLs or objects"""
        urls = []
        
        for i, item in enumerate(data):
            if isinstance(item, str):
                # Simple URL string
                company = self._extract_company_from_url(item)
                urls.append({
                    'url': item,
                    'company': company,
                    'source': 'json_array',
                    'metadata': {
                        'index': i,
                        'file': str(file_path)
                    }
                })
            elif isinstance(item, dict):
                # Object with URL
                url_data = self._extract_url_from_json_object(item, i, file_path)
                if url_data:
                    url_data['source'] = 'json_array'
                    urls.append(url_data)
        
        return urls
    
    def _process_retry_file(self, data: Dict, file_path: Path) -> List[Dict[str, str]]:
        """Process retry file format with metadata"""
        urls = []
        failures = data.get('failures', [])
        
        for i, failure in enumerate(failures):
            url = failure.get('url')
            if not url:
                continue
            
            company = failure.get('company', self._extract_company_from_url(url))
            
            # Extract retry metadata
            retry_metadata = failure.get('retry_metadata', {})
            
            urls.append({
                'url': url,
                'company': company,
                'source': 'retry_file',
                'metadata': {
                    'original_error': failure.get('error_type'),
                    'retry_count': failure.get('retry_count', 0),
                    'retry_attempt': retry_metadata.get('retry_attempt', 1),
                    'max_retries': retry_metadata.get('max_retries', 5),
                    'file': str(file_path),
                    'index': i
                }
            })
        
        return urls
    
    def _extract_url_from_json_object(self, obj: Dict, index: int, file_path: Path) -> Optional[Dict[str, str]]:
        """Extract URL and metadata from JSON object"""
        # Try common URL field names
        url_fields = ['url', 'job_url', 'link', 'href', 'job_link']
        url = None
        
        for field in url_fields:
            if field in obj and obj[field]:
                url = obj[field]
                break
        
        if not url:
            logger.debug(f"No URL found in object at index {index} in {file_path}")
            return None
        
        # Extract company name
        company = obj.get('company')
        if not company:
            company = obj.get('company_name', self._extract_company_from_url(url))
        
        # Determine source
        source = 'json_object'
        if 'job_id' in obj and 'title' in obj:
            source = 'job_extractor_output'
        
        return {
            'url': url,
            'company': company,
            'source': source,
            'metadata': {
                **{k: v for k, v in obj.items() if k not in ['url', 'company']},
                'index': index,
                'file': str(file_path)
            }
        }
    
    def _extract_urls_from_object(self, obj: Dict, file_path: Path) -> List[Dict[str, str]]:
        """Extract URLs from a single JSON object (not array)"""
        urls = []
        
        # Look for URL fields in the object
        for key, value in obj.items():
            if isinstance(value, list):
                # Check if it's a list of URLs
                for i, item in enumerate(value):
                    if isinstance(item, str) and self._is_valid_url(item):
                        company = self._extract_company_from_url(item)
                        urls.append({
                            'url': item,
                            'company': company,
                            'source': f'json_field_{key}',
                            'metadata': {
                                'field': key,
                                'index': i,
                                'file': str(file_path)
                            }
                        })
                    elif isinstance(item, dict):
                        url_data = self._extract_url_from_json_object(item, i, file_path)
                        if url_data:
                            url_data['source'] = f'json_field_{key}'
                            urls.append(url_data)
            
            elif isinstance(value, str) and self._is_valid_url(value):
                company = self._extract_company_from_url(value)
                urls.append({
                    'url': value,
                    'company': company,
                    'source': f'json_field_{key}',
                    'metadata': {
                        'field': key,
                        'file': str(file_path)
                    }
                })
        
        return urls
    
    def _validate_urls(self, urls: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Validate and filter URLs"""
        valid_urls = []
        invalid_count = 0
        
        for url_data in urls:
            url = url_data['url']
            
            if not self._is_valid_url(url):
                logger.debug(f"Invalid URL format: {url}")
                invalid_count += 1
                continue
            
            if not self._is_avature_url(url):
                logger.debug(f"Not an Avature URL: {url}")
                invalid_count += 1
                continue
            
            if not self._is_job_detail_url(url):
                logger.debug(f"Not a job detail URL: {url}")
                invalid_count += 1
                continue
            
            valid_urls.append(url_data)
        
        if invalid_count > 0:
            logger.info(f"Filtered out {invalid_count} invalid URLs")
        
        return valid_urls
    
    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is properly formatted"""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False
    
    def _is_avature_url(self, url: str) -> bool:
        """Check if URL is from Avature platform"""
        return any(re.search(pattern, url, re.IGNORECASE) for pattern in self.avature_patterns)
    
    def _is_job_detail_url(self, url: str) -> bool:
        """Check if URL is a job detail page"""
        job_detail_patterns = [
            r'/JobDetail/',
            r'/jobs/\d+',
            r'/job/[a-zA-Z0-9-]+',
            r'jobId=\d+'
        ]
        
        return any(re.search(pattern, url, re.IGNORECASE) for pattern in job_detail_patterns)
    
    def _extract_company_from_url(self, url: str) -> str:
        """Extract company name from Avature URL"""
        try:
            parsed = urlparse(url)
            hostname_parts = parsed.hostname.split('.')
            
            # For Avature URLs like "company.avature.net"
            if 'avature' in hostname_parts:
                avature_index = hostname_parts.index('avature')
                if avature_index > 0:
                    return hostname_parts[avature_index - 1]
            
            # Fallback to first part of hostname
            return hostname_parts[0] if hostname_parts else 'unknown'
        except:
            return 'unknown'
    
    def get_url_statistics(self, urls: List[Dict[str, str]]) -> Dict[str, any]:
        """Get statistics about processed URLs"""
        stats = {
            'total_urls': len(urls),
            'by_company': {},
            'by_source': {},
            'unique_companies': set(),
            'duplicate_urls': 0
        }
        
        url_set = set()
        
        for url_data in urls:
            url = url_data['url']
            company = url_data['company']
            source = url_data['source']
            
            # Count duplicates
            if url in url_set:
                stats['duplicate_urls'] += 1
            else:
                url_set.add(url)
            
            # Count by company
            stats['by_company'][company] = stats['by_company'].get(company, 0) + 1
            stats['unique_companies'].add(company)
            
            # Count by source
            stats['by_source'][source] = stats['by_source'].get(source, 0) + 1
        
        stats['unique_companies'] = len(stats['unique_companies'])
        stats['unique_urls'] = len(url_set)
        
        return stats
    
    def save_processed_urls(self, urls: List[Dict[str, str]], output_file: str):
        """Save processed URLs to JSONL file for debugging"""
        output_path = Path(output_file)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            for url_data in urls:
                f.write(json.dumps(url_data, ensure_ascii=False) + '\n')
        
        logger.info(f"Saved {len(urls)} processed URLs to {output_file}")