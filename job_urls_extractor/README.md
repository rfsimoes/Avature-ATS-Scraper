# Job URL Extractor

A dedicated Python script for extracting job detail URLs from career pages with intelligent failure handling and proper categorization.

## Features

- **Multi-strategy extraction**: Uses sitemap XML parsing (fast) and HTML pagination (fallback)
- **Rate limiting detection**: Automatically detects and handles rate limiting
- **Intelligent failure categorization**: Separates rate-limited URLs for retry vs permanent failures
- **Comprehensive output**: Success, failure, and retry files with detailed information
- **Concurrent processing**: Uses thread pool for efficient parallel processing

## Usage

### Basic Usage

```bash
python job_url_extractor.py input_urls.txt
```

### Input File Format

The input file supports two formats:

#### Format 1: URL only (company name extracted from subdomain)
```
https://bloomberg.avature.net/careers
https://uclahealth.avature.net/careers
```

#### Format 2: Company name and URL separated by pipe
```
Bloomberg|https://bloomberg.avature.net/careers
UCLA Health|https://uclahealth.avature.net/careers
```

### Output Files

The script creates timestamped output files in the `job_urls_extractor/` directory:

1. **`success_[input]_[timestamp].jsonl`** - Successful extractions with job URLs
2. **`failures_[input]_[timestamp].jsonl`** - Failed extractions with error details  
3. **`retry_[input]_[timestamp].jsonl`** - Rate-limited URLs for retry later

## Extraction Strategies

### 1. Sitemap Strategy (Primary)
- Fetches `/sitemap.xml` from career site
- Parses XML for URLs containing `/JobDetail/`
- Fastest and most comprehensive method
- May include historical/closed positions

### 2. HTML Pagination (Fallback)
- Iterates through job listing pages
- Extracts job URLs from HTML articles
- Always works but slower than sitemap
- Only shows currently active positions

## Error Handling

### Rate Limiting Detection
The script detects rate limiting through:
- HTTP 429 (Too Many Requests) responses
- HTTP 403 with rate limit indicators
- Rate limit headers (X-RateLimit-Remaining, Retry-After)

When rate limiting is detected:
- Affected URLs are saved to retry file
- Execution stops immediately
- Exit code 2 indicates rate limiting

### Other Failures
Non-rate-limiting failures are categorized as:
- `sitemap_timeout` - Sitemap request timeout
- `sitemap_connection_error` - Connection failed
- `sitemap_parse_error` - XML parsing error
- `html_extraction_error` - HTML parsing error
- `unexpected_error` - Other unexpected errors

## Exit Codes

- **0**: Success - all URLs processed without errors
- **1**: Partial failure - some URLs failed (not rate limited)
- **2**: Rate limited - execution stopped, retry needed
- **130**: Interrupted by user (Ctrl+C)

## Configuration

### Constructor Parameters

```python
extractor = JobUrlExtractor(
    max_workers=3,        # Concurrent threads
    request_delay=1.0     # Seconds between requests
)
```

### Rate Limiting Settings

The default configuration is conservative to avoid rate limiting:
- 3 concurrent workers
- 1.0 second delay between requests
- 15 second timeout per request

## Examples

### Test with Sample Data

```bash
python test_extractor.py
```

### Process Real URLs

```bash
python job_url_extractor.py companies.txt
```

### Resume from Retry File

1. Check retry file for rate-limited URLs
2. Create new input file with those URLs
3. Wait for cooldown period (recommended: 1+ hours)
4. Run again with new input file

## Output Data Structure

### Success Record
```json
{
  "career_url": "https://bloomberg.avature.net/careers",
  "company": "Bloomberg", 
  "extraction_method": "sitemap",
  "job_urls_count": 1165,
  "job_urls": ["https://bloomberg.avature.net/careers/JobDetail/...", ...],
  "timestamp": "2026-02-01T16:39:06.123456"
}
```

### Failure Record
```json
{
  "career_url": "https://example.avature.net/careers",
  "company": "Example",
  "error_type": "sitemap_timeout", 
  "error_message": "Sitemap request timed out",
  "http_status": null,
  "timestamp": "2026-02-01T16:39:06.123456"
}
```

### Retry Record
```json
{
  "career_url": "https://bloomberg.avature.net/careers",
  "company": "Bloomberg",
  "error_message": "HTTP 429 - Too Many Requests",
  "http_status": 429,
  "timestamp": "2026-02-01T16:39:06.123456"
}
```

## Dependencies

- requests
- beautifulsoup4
- lxml (for XML parsing)

Install with:
```bash
pip install requests beautifulsoup4 lxml
```