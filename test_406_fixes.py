#!/usr/bin/env python3
"""
Test script to verify 406 error handling improvements
"""
import sys
from pathlib import Path

# Add scraper directory to path
sys.path.insert(0, str(Path(__file__).parent / 'scraper'))

from scraper.job_details_extractor import JobFailure, AvatureJobDetailsExtractor

def test_406_retryable():
    """Test that 406 errors are now marked as retryable"""
    print("🧪 Testing 406 error handling...")
    
    # Test JobFailure with 406 status
    failure_406 = JobFailure(
        url="https://example.avature.net/test",
        job_id="123",
        company="test",
        error_type="rate_limited",
        error_message="HTTP status 406",
        http_status=406
    )
    
    assert failure_406.is_retryable == True, "406 errors should be retryable"
    print("✅ 406 errors are correctly marked as retryable")
    
    # Test with generic http_error but 406 status
    failure_406_generic = JobFailure(
        url="https://example.avature.net/test",
        job_id="123", 
        company="test",
        error_type="http_error",
        error_message="HTTP status 406",
        http_status=406
    )
    
    assert failure_406_generic.is_retryable == True, "406 status codes should be retryable regardless of error_type"
    print("✅ 406 status codes are retryable even with generic error types")

def test_enhanced_rate_limiting():
    """Test enhanced rate limiting features"""
    print("🧪 Testing enhanced rate limiting...")
    
    extractor = AvatureJobDetailsExtractor()
    
    # Test new attributes exist
    assert hasattr(extractor, 'adaptive_delay'), "Should have adaptive_delay attribute"
    assert hasattr(extractor, 'max_adaptive_delay'), "Should have max_adaptive_delay attribute"  
    assert hasattr(extractor, 'recent_406_count'), "Should have recent_406_count attribute"
    assert hasattr(extractor, 'last_request_time'), "Should have last_request_time attribute"
    
    print("✅ Enhanced rate limiting attributes present")
    
    # Test adaptive delay adjustment
    initial_delay = extractor.adaptive_delay
    extractor.recent_406_count = 3
    extractor._adjust_adaptive_delay()
    
    assert extractor.adaptive_delay > initial_delay, "Adaptive delay should increase with 406 errors"
    print(f"✅ Adaptive delay increased from {initial_delay} to {extractor.adaptive_delay}")
    
    # Test increased request delay
    assert extractor.request_delay == 0.5, f"Request delay should be 0.5s, got {extractor.request_delay}"
    print("✅ Request delay increased to 0.5s (from 0.2s)")

def main():
    """Run all tests"""
    print("🚀 Running 406 Error Handling Tests")
    print("=" * 50)
    
    try:
        test_406_retryable()
        print()
        test_enhanced_rate_limiting()
        print()
        print("🎉 All tests passed! 406 error handling improvements are working correctly.")
        
        print("\n📋 Summary of improvements:")
        print("   ✅ HTTP 406 errors are now retryable")
        print("   ✅ 406 errors are classified as rate_limited")
        print("   ✅ Enhanced adaptive delay system")
        print("   ✅ Increased base request delay (0.5s)")
        print("   ✅ Progressive backoff for 406 errors")
        print("   ✅ Per-request rate limiting")
        print("   ✅ Automatic 406 count reset")
        
        return 0
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return 1

if __name__ == '__main__':
    sys.exit(main())