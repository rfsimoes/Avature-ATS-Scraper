# Avature Job Details Extractor

A comprehensive web scraper for extracting detailed job information from Avature ATS (Applicant Tracking System) job posting URLs. Built on proven extraction patterns from the hybrid_scraper.py implementation.

## Features

✅ **Multi-Format Input Support**
- Plain text files (one URL per line)  
- JSONL files from URL extractors
- JSON files and retry queues
- Automatic URL validation and filtering

✅ **Comprehensive Job Details Extraction**
- Job Title (required)
- Clean Job Description (HTML/text)
- Application URLs
- Location information
- Posted dates, departments, employment types
- Company identification from URLs

✅ **Intelligent Retry Management**
- Separates retryable failures (timeouts, rate limits) from permanent failures (404s)
- Exponential backoff with configurable retry attempts
- Rate-limit detection with cooldown periods
- Retry queue files for reprocessing

✅ **Robust Error Handling**
- Handles 404s, 403s, timeouts, connection errors
- Detects "position filled" and "applications closed" states
- Comprehensive failure categorization and analysis
- HTTP status code tracking

✅ **Structured Output System**
- Job details in JSONL format
- Retry files for failed extractions  
- Failure analysis with recommendations
- Extraction statistics and performance metrics
- Human-readable summary reports

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# The extractor uses the existing dependencies from your hybrid scraper:
# requests, beautifulsoup4, lxml
```

## Quick Start

### 1. Basic Usage with URL List

```bash
# Create a simple text file with URLs
echo "https://bloomberg.avature.net/careers/JobDetail/New-York-New-York-United-States-Software-Engineer/12345" > job_urls.txt

# Extract job details
python extract_job_details.py --input job_urls.txt --output bloomberg_jobs
```

### 2. Process JSONL from URL Extractor

```bash
# Use output from your job URL extractor
python extract_job_details.py --input job_urls_extractor/urls_bloomberg_20260208.jsonl --company Bloomberg
```

### 3. Handle Retry Files

```bash
# Process retry file from previous failed extraction
python extract_job_details.py --input retries/retryable_bloomberg_20260208_143022.jsonl --max-workers 2
```

## Input Formats

### Plain Text (.txt)
```
https://company1.avature.net/careers/JobDetail/Location-Job-Title/12345
https://company2.avature.net/careers/JobDetail/Another-Job/67890
# Comments are ignored
```

### JSONL from URL Extractor (.jsonl)
```jsonl
{"url": "https://company.avature.net/careers/JobDetail/Job/123", "company": "Company", "job_id": "123"}
{"url": "https://company.avature.net/careers/JobDetail/Job/456", "company": "Company", "job_id": "456"}
```

### Retry Files (.jsonl)
Generated automatically from previous extraction failures, contain retry metadata and failure analysis.

## Command Line Options

```bash
python extract_job_details.py [OPTIONS]

Required:
  --input, -i FILE          Input file with job URLs (txt, jsonl, json)

Output:
  --output, -o PREFIX       Output file prefix (default: job_details)
  --output-dir DIR          Output directory (default: current)

Extraction:
  --max-workers N           Concurrent workers (default: 5)
  --timeout N               Request timeout in seconds (default: 25)
  --max-retries N           Max retry attempts per URL (default: 3)
  --delay N                 Delay between requests in seconds (default: 0.2)

Filtering:
  --company NAME            Filter URLs for specific company
  --limit N                 Process only first N URLs (testing)

Logging:
  --verbose, -v             Enable verbose logging
  --log-file FILE           Save detailed logs to file
  --quiet, -q               Suppress progress output

Advanced:
  --check-retry-file        Check if retry file is ready for processing
  --stats-only              Show URL statistics without extracting
  --no-retries              Don't generate retry files
```

## Output Structure

### Job Details Output (`job_details/`)
```jsonl
{
  "job_id": "12345",
  "title": "Software Engineer",
  "url": "https://company.avature.net/careers/JobDetail/Software-Engineer/12345",
  "location": "New York, NY",
  "company": "company",
  "description": "Full job description text...",
  "date_posted": "2026-02-01",
  "department": "Engineering", 
  "employment_type": "Full-time",
  "application_url": "https://company.avature.net/careers/Login?jobId=12345",
  "scraped_at": "2026-02-08T14:30:00.123456",
  "source_method": "url_list",
  "extraction_metadata": {
    "extracted_at": "2026-02-08T14:30:00.123456",
    "extractor_version": "1.0.0",
    "fields_extracted": ["job_id", "title", "url", "location", "company", "description"]
  }
}
```

### Retry Files (`retries/`)

**Retryable Failures** (`retryable_*.jsonl`):
- Timeouts, connection errors, rate limits
- Includes retry metadata and recommended delays
- Ready for reprocessing with adjusted settings

**Rate-Limited Failures** (`rate_limited_*.jsonl`):
- 429 errors requiring cooldown period
- Includes recommended retry time and reduced worker settings

**Permanent Failures** (`permanent_failures_*.jsonl`):
- 404s, access denied, parsing errors
- Not suitable for retry, included for analysis

### Analysis Files (`logs/`)

**Extraction Statistics** (`extraction_stats_*.json`):
```json
{
  "summary": {
    "total_processed": 100,
    "successful_extractions": 85, 
    "failed_extractions": 15,
    "success_rate_percent": 85.0
  },
  "field_analysis": {
    "description": {"extracted": 80, "extraction_rate": 94.1},
    "application_url": {"extracted": 75, "extraction_rate": 88.2}
  },
  "failure_breakdown": {
    "by_type": {"not_found": 8, "timeout": 4, "access_forbidden": 3}
  }
}
```

**Extraction Summary** (`extraction_summary_*.json`):
Human-readable summary with recommendations:
```json
{
  "extraction_summary": {
    "success_rate": "85.0%",
    "duration": "120.5 seconds"
  },
  "recommendations": [
    "High timeout rate - increase timeout settings or reduce concurrent workers",
    "Low description extraction rate - page structure may have changed"
  ]
}
```

## Advanced Usage Examples

### 1. Company-Specific Extraction with Custom Settings
```bash
# Bloomberg with conservative settings (slower servers)
python extract_job_details.py \
  --input bloomberg_urls.txt \
  --company Bloomberg \
  --max-workers 3 \
  --timeout 40 \
  --delay 0.5 \
  --output bloomberg_jobs

# UCLA Health with standard settings
python extract_job_details.py \
  --input ucla_urls.jsonl \
  --company "UCLA Health" \
  --max-workers 5 \
  --output ucla_jobs
```

### 2. Retry File Processing
```bash
# Check if retry file is ready
python extract_job_details.py --check-retry-file retries/rate_limited_bloomberg_20260208_143022.jsonl

# Process retry file with reduced rate
python extract_job_details.py \
  --input retries/retryable_company_20260208.jsonl \
  --max-workers 2 \
  --timeout 35 \
  --delay 1.0
```

### 3. Testing and Development
```bash
# Test with limited URLs
python extract_job_details.py --input large_url_list.txt --limit 10 --verbose

# Show statistics without extraction
python extract_job_details.py --input urls.jsonl --stats-only

# Debug with full logging
python extract_job_details.py \
  --input problematic_urls.txt \
  --verbose \
  --log-file debug.log \
  --max-workers 1
```

## Integration with Existing Workflow

This extractor is designed to work with your existing Avature scraping pipeline:

```bash
# 1. Extract URLs using your existing tools
python job_urls_extractor/extract_urls.py --company bloomberg

# 2. Extract job details from discovered URLs  
python extract_job_details.py --input job_urls_extractor/urls_bloomberg_20260208.jsonl

# 3. Process any failures with retry files
python extract_job_details.py --input retries/retryable_bloomberg_20260208.jsonl
```

## Error Handling and Retry Strategy

### Automatic Retry Classification

**Retryable Errors:**
- `timeout`: Server response timeout
- `connection_error`: Network connectivity issues  
- `rate_limited`: 429 Too Many Requests
- `server_error`: 5xx server errors

**Permanent Errors:**
- `not_found`: 404 Job not found (likely removed)
- `access_forbidden`: 403 Access denied
- `position_filled`: Job posting indicates position filled
- `applications_closed`: Job no longer accepting applications
- `missing_title`: Cannot extract required job title

### Retry Configuration

The system uses exponential backoff with configurable limits:

- **Max Retries**: 3 attempts by default (configurable)
- **Retry Delays**: 5min, 10min, 20min, 40min, 80min
- **Rate Limit Cooldown**: 30 minutes for 429 errors
- **Timeout Scaling**: Increased timeout on retry attempts

## Performance Optimization

### Recommended Settings by Company

**Bloomberg** (slower responses):
```bash
--max-workers 3 --timeout 40 --delay 0.5
```

**UCLA Health** (standard responses):
```bash  
--max-workers 5 --timeout 25 --delay 0.2
```

**General High-Volume** (fast responses):
```bash
--max-workers 8 --timeout 20 --delay 0.1
```

### Rate Limiting Best Practices

1. **Start Conservative**: Use lower worker counts initially
2. **Monitor for 429s**: Check retry files for rate limit errors
3. **Adjust Based on Response**: Increase workers if no rate limiting
4. **Use Company-Specific Settings**: Different servers have different limits

## Troubleshooting

### Common Issues

**High Failure Rate (>50%)**:
- Check URL validity with `--stats-only`
- Verify URLs are job detail pages (contain `/JobDetail/`)
- Test with `--limit 5` for debugging

**Rate Limiting (429 errors)**:
- Reduce `--max-workers` to 2-3
- Increase `--delay` to 0.5-1.0 seconds
- Process rate-limited retry files after cooldown

**Timeouts**:
- Increase `--timeout` to 30-40 seconds
- Reduce `--max-workers` to decrease server load
- Check network connectivity

**Low Field Extraction Rate**:
- Page structure may have changed
- Review extraction selectors in job_details_extractor.py
- Enable `--verbose` logging for debugging

### Debugging Commands

```bash
# Test single URL with full logging
python extract_job_details.py --input single_url.txt --verbose --max-workers 1

# Analyze existing output files
python example_usage.py  # Runs analysis examples

# Check retry file status  
python extract_job_details.py --check-retry-file retries/file.jsonl
```

## Development and Customization

### Key Components

- **`scraper/job_details_extractor.py`**: Core extraction logic
- **`scraper/url_processor.py`**: Input file processing  
- **`scraper/output_manager.py`**: Output generation
- **`scraper/retry_manager.py`**: Retry queue management
- **`extract_job_details.py`**: CLI interface

### Customizing Extraction

To modify field extraction patterns, edit the selector lists in `job_details_extractor.py`:

```python
# Add new title selectors
title_selectors = [
    'h2.banner__text__title',  # UCLA Health
    'div.article__content__view__field__value--font .article__content__view__field__value',  # Bloomberg  
    'h1.new-title-selector',  # Your custom selector
    'h1.title', 'h1', 'h2'    # Fallbacks
]
```

## License

This tool extends the existing Avature ATS scraper codebase and maintains the same license terms.