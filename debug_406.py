#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / 'scraper'))

from scraper.job_details_extractor import JobFailure

# Test 406 as rate_limited
failure = JobFailure(
    url='test',
    job_id='123', 
    company='test',
    error_type='rate_limited',
    error_message='HTTP 406',
    http_status=406
)

print(f"Is retryable: {failure.is_retryable}")