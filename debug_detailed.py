#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / 'scraper'))

from scraper.job_details_extractor import JobFailure

print("Creating JobFailure with rate_limited error type and 406 status...")

# Test 406 as rate_limited
failure = JobFailure(
    url='test',
    job_id='123', 
    company='test',
    error_type='rate_limited',
    error_message='HTTP 406',
    http_status=406
)

print(f"Error type: {failure.error_type}")
print(f"HTTP status: {failure.http_status}")
print(f"Is retryable: {failure.is_retryable}")

# Let's manually test the logic
retryable_types = {'timeout', 'connection_error', 'rate_limited', 'server_error', 'temporary_error'}
retryable_status_codes = {406, 429, 500, 502, 503, 504}

print(f"\nManual check:")
print(f"'rate_limited' in retryable_types: {'rate_limited' in retryable_types}")
print(f"406 in retryable_status_codes: {406 in retryable_status_codes}")

should_be_retryable = ('rate_limited' in retryable_types) or (406 in retryable_status_codes)
print(f"Should be retryable: {should_be_retryable}")