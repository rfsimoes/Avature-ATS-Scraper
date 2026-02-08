#!/usr/bin/env python3
"""
Avature Tenant Extractor

Single responsibility: Extract unique tenant names from Avature URLs.
Reads from Urls.txt and outputs a clean list of company tenants.
"""

import re
import sys
from urllib.parse import urlparse
from typing import Set, List
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_tenants_from_file(file_path: str) -> Set[str]:
    """
    Extract unique tenant names from Avature URLs.
    
    Args:
        file_path: Path to file containing URLs
        
    Returns:
        Set of unique tenant names
    """
    tenants = set()
    total_urls = 0
    
    # Avature URL pattern: https://TENANT.avature.net/...
    avature_pattern = re.compile(r'https?://([^./]+)\.avature\.net')
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                total_urls += 1
                
                # Progress indicator for large files
                if line_num % 50000 == 0:
                    logger.info(f"Processed {line_num:,} URLs, found {len(tenants):,} unique tenants")
                
                line = line.strip()
                if not line:
                    continue
                
                # Extract tenant from URL
                match = avature_pattern.match(line)
                if match:
                    tenant = match.group(1).lower()
                    
                    # Skip obvious test/demo tenants
                    if tenant not in ['demo', 'test', 'sandbox', 'staging']:
                        tenants.add(tenant)
    
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return set()
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return set()
    
    logger.info(f"Processed {total_urls:,} URLs total")
    logger.info(f"Found {len(tenants):,} unique tenants")
    
    return tenants


def save_tenants(tenants: Set[str], output_file: str) -> None:
    """Save tenants to file, one per line, sorted."""
    sorted_tenants = sorted(tenants)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for tenant in sorted_tenants:
            f.write(f"{tenant}\n")
    
    logger.info(f"Saved {len(sorted_tenants):,} tenants to {output_file}")


def main():
    """Extract tenants from Urls.txt"""
    input_file = "Urls.txt"
    output_file = "tenants.txt"
    
    logger.info("Starting tenant extraction...")
    logger.info(f"Input: {input_file}")
    logger.info(f"Output: {output_file}")
    
    # Extract tenants
    tenants = extract_tenants_from_file(input_file)
    
    if not tenants:
        logger.error("No tenants found!")
        sys.exit(1)
    
    # Save results
    save_tenants(tenants, output_file)
    
    # Show sample
    sorted_tenants = sorted(tenants)
    logger.info(f"\nSample tenants (first 20):")
    for tenant in sorted_tenants[:20]:
        logger.info(f"  {tenant}")
    
    logger.info(f"\nComplete! {len(tenants):,} unique tenants extracted.")
    print(f"\nNext steps:")
    print(f"1. Review {output_file} for data quality")
    print(f"2. Use tenants to generate candidate URLs for discovery")
    print(f"3. Validate discovered sites with site_validator.py")


if __name__ == "__main__":
    main()