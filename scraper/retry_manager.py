"""
Retry Manager for Job Details Extractor
Handles intelligent retry queue management with backoff strategies
"""

import json
import logging
import time
from typing import List, Dict, Optional, Set
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path

from .job_details_extractor import JobFailure

logger = logging.getLogger(__name__)


class RetryManager:
    """
    Manages retry queues for failed job extractions
    Separates retryable failures from permanent failures
    """
    
    def __init__(self, retries_dir: str = "retries"):
        self.retries_dir = Path(retries_dir)
        self.retries_dir.mkdir(exist_ok=True)
        
        # Retry configuration
        self.max_retry_attempts = 5
        self.retry_delays = [300, 600, 1200, 2400, 4800]  # 5min, 10min, 20min, 40min, 80min
        self.rate_limit_cooldown = 1800  # 30 minutes for rate limit errors
        
    def process_failures(self, failures: List[JobFailure], output_prefix: str) -> Dict[str, int]:
        """
        Process failures and create appropriate retry files
        Returns statistics about failure categorization
        """
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        
        # Categorize failures
        retryable_failures = []
        rate_limited_failures = []
        permanent_failures = []
        
        for failure in failures:
            if failure.error_type == 'rate_limited':
                rate_limited_failures.append(failure)
            elif failure.is_retryable and failure.retry_count < self.max_retry_attempts:
                retryable_failures.append(failure)
            else:
                permanent_failures.append(failure)
        
        stats = {
            'total_failures': len(failures),
            'retryable': len(retryable_failures),
            'rate_limited': len(rate_limited_failures),
            'permanent': len(permanent_failures)
        }
        
        # Save retry files
        if retryable_failures:
            retry_file = self.retries_dir / f"retryable_{output_prefix}_{timestamp}.jsonl"
            self._save_retry_queue(retryable_failures, retry_file, "general")
            logger.info(f"✓ Saved {len(retryable_failures)} retryable failures to {retry_file}")
        
        if rate_limited_failures:
            rate_limit_file = self.retries_dir / f"rate_limited_{output_prefix}_{timestamp}.jsonl"
            self._save_retry_queue(rate_limited_failures, rate_limit_file, "rate_limited")
            logger.info(f"✓ Saved {len(rate_limited_failures)} rate-limited failures to {rate_limit_file}")
        
        # Save permanent failures for analysis (not retryable)
        if permanent_failures:
            permanent_file = self.retries_dir / f"permanent_failures_{output_prefix}_{timestamp}.jsonl"
            self._save_failure_analysis(permanent_failures, permanent_file)
            logger.info(f"✓ Saved {len(permanent_failures)} permanent failures to {permanent_file}")
        
        return stats
    
    def _save_retry_queue(self, failures: List[JobFailure], file_path: Path, retry_type: str):
        """Save failures to retry queue with metadata"""
        retry_queue = {
            'metadata': {
                'created_at': datetime.utcnow().isoformat(),
                'retry_type': retry_type,
                'total_items': len(failures),
                'next_retry_time': self._calculate_next_retry_time(retry_type),
                'retry_instructions': self._get_retry_instructions(retry_type)
            },
            'failures': []
        }
        
        for failure in failures:
            retry_item = {
                **asdict(failure),
                'retry_metadata': {
                    'original_failure_time': failure.timestamp,
                    'retry_attempt': failure.retry_count + 1,
                    'max_retries': self.max_retry_attempts,
                    'recommended_delay': self._get_retry_delay(failure.retry_count, retry_type),
                    'retry_strategy': self._get_retry_strategy(failure.error_type)
                }
            }
            retry_queue['failures'].append(retry_item)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(retry_queue, f, indent=2, ensure_ascii=False)
    
    def _save_failure_analysis(self, failures: List[JobFailure], file_path: Path):
        """Save permanent failures with analysis"""
        analysis = {
            'metadata': {
                'created_at': datetime.utcnow().isoformat(),
                'total_failures': len(failures),
                'failure_summary': self._analyze_failure_patterns(failures)
            },
            'failures': [asdict(failure) for failure in failures]
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=2, ensure_ascii=False)
    
    def _analyze_failure_patterns(self, failures: List[JobFailure]) -> Dict:
        """Analyze patterns in permanent failures"""
        patterns = {
            'by_error_type': {},
            'by_http_status': {},
            'by_company': {},
            'common_patterns': []
        }
        
        for failure in failures:
            # Count by error type
            error_type = failure.error_type
            patterns['by_error_type'][error_type] = patterns['by_error_type'].get(error_type, 0) + 1
            
            # Count by HTTP status
            if failure.http_status:
                status = str(failure.http_status)
                patterns['by_http_status'][status] = patterns['by_http_status'].get(status, 0) + 1
            
            # Count by company
            company = failure.company
            patterns['by_company'][company] = patterns['by_company'].get(company, 0) + 1
        
        # Identify common patterns
        if patterns['by_error_type'].get('not_found', 0) > len(failures) * 0.3:
            patterns['common_patterns'].append("High 404 rate - URLs may be expired or invalid")
        
        if patterns['by_error_type'].get('access_forbidden', 0) > len(failures) * 0.2:
            patterns['common_patterns'].append("High 403 rate - possible access restrictions")
        
        if patterns['by_error_type'].get('missing_title', 0) > len(failures) * 0.1:
            patterns['common_patterns'].append("Title extraction issues - page structure may have changed")
        
        return patterns
    
    def _calculate_next_retry_time(self, retry_type: str) -> str:
        """Calculate recommended next retry time"""
        if retry_type == "rate_limited":
            next_time = datetime.utcnow() + timedelta(seconds=self.rate_limit_cooldown)
        else:
            next_time = datetime.utcnow() + timedelta(seconds=self.retry_delays[0])
        
        return next_time.isoformat()
    
    def _get_retry_delay(self, retry_count: int, retry_type: str) -> int:
        """Get recommended delay for retry attempt"""
        if retry_type == "rate_limited":
            return self.rate_limit_cooldown
        
        if retry_count < len(self.retry_delays):
            return self.retry_delays[retry_count]
        
        return self.retry_delays[-1]  # Use max delay for high retry counts
    
    def _get_retry_strategy(self, error_type: str) -> str:
        """Get retry strategy recommendation based on error type"""
        strategies = {
            'timeout': 'Increase timeout, reduce workers',
            'connection_error': 'Check network, reduce workers',
            'rate_limited': 'Wait for cooldown period, reduce request rate',
            'server_error': 'Server issue, exponential backoff',
            'temporary_error': 'General retry with exponential backoff'
        }
        
        return strategies.get(error_type, 'Standard retry with exponential backoff')
    
    def _get_retry_instructions(self, retry_type: str) -> Dict[str, str]:
        """Get human-readable retry instructions"""
        if retry_type == "rate_limited":
            return {
                'when_to_retry': f'Wait at least {self.rate_limit_cooldown // 60} minutes before retrying',
                'recommended_settings': 'Reduce max_workers to 2-3, increase request delays',
                'command_example': 'python extract_job_details.py --input rate_limited_file.jsonl --max-workers 2 --delay 1.0'
            }
        else:
            return {
                'when_to_retry': 'Wait 5+ minutes before first retry, use exponential backoff',
                'recommended_settings': 'Use standard settings, may increase timeout',
                'command_example': 'python extract_job_details.py --input retryable_file.jsonl --timeout 40'
            }
    
    def load_retry_queue(self, retry_file: str) -> List[Dict]:
        """Load retry queue from file"""
        retry_path = Path(retry_file)
        
        if not retry_path.exists():
            raise FileNotFoundError(f"Retry file not found: {retry_file}")
        
        with open(retry_path, 'r', encoding='utf-8') as f:
            retry_data = json.load(f)
        
        # Extract URLs from retry queue
        urls = []
        for failure_data in retry_data.get('failures', []):
            urls.append({
                'url': failure_data['url'],
                'company': failure_data['company'],
                'retry_count': failure_data.get('retry_count', 0),
                'original_error': failure_data['error_type']
            })
        
        logger.info(f"Loaded {len(urls)} URLs from retry queue: {retry_file}")
        
        return urls
    
    def check_retry_readiness(self, retry_file: str) -> Dict[str, any]:
        """Check if retry file is ready for processing"""
        retry_path = Path(retry_file)
        
        if not retry_path.exists():
            return {'ready': False, 'reason': 'File not found'}
        
        with open(retry_path, 'r', encoding='utf-8') as f:
            retry_data = json.load(f)
        
        metadata = retry_data.get('metadata', {})
        next_retry_time = metadata.get('next_retry_time')
        
        if next_retry_time:
            next_time = datetime.fromisoformat(next_retry_time.replace('Z', '+00:00'))
            current_time = datetime.utcnow().replace(tzinfo=next_time.tzinfo)
            
            if current_time < next_time:
                wait_time = (next_time - current_time).total_seconds()
                return {
                    'ready': False,
                    'reason': f'Wait {int(wait_time // 60)} more minutes',
                    'next_retry_time': next_retry_time
                }
        
        return {
            'ready': True,
            'total_items': metadata.get('total_items', 0),
            'retry_type': metadata.get('retry_type', 'unknown')
        }
    
    def get_retry_statistics(self) -> Dict[str, any]:
        """Get statistics about retry files in directory"""
        retry_files = list(self.retries_dir.glob("*.jsonl"))
        
        stats = {
            'total_retry_files': len(retry_files),
            'retryable_files': len([f for f in retry_files if 'retryable_' in f.name]),
            'rate_limited_files': len([f for f in retry_files if 'rate_limited_' in f.name]),
            'permanent_failure_files': len([f for f in retry_files if 'permanent_failures_' in f.name]),
            'files': []
        }
        
        for file_path in sorted(retry_files, key=lambda x: x.stat().st_mtime, reverse=True):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                file_stats = {
                    'filename': file_path.name,
                    'created_at': data.get('metadata', {}).get('created_at'),
                    'total_items': data.get('metadata', {}).get('total_items', len(data.get('failures', []))),
                    'retry_type': data.get('metadata', {}).get('retry_type', 'unknown'),
                    'ready_for_retry': self.check_retry_readiness(str(file_path))['ready']
                }
                stats['files'].append(file_stats)
                
            except Exception as e:
                logger.warning(f"Error reading retry file {file_path}: {e}")
        
        return stats