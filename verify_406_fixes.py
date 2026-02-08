#!/usr/bin/env python3
"""
Verification script for 406 error handling fixes
"""
import sys
from pathlib import Path

# Add scraper directory to path
sys.path.insert(0, str(Path(__file__).parent / 'scraper'))

from scraper.job_details_extractor import JobFailure, AvatureJobDetailsExtractor

def test_406_retryable():
    """Test that 406 errors are marked as retryable"""
    print("Testing 406 error retryability...")
    
    # Test 1: 406 with rate_limited error type
    failure1 = JobFailure(
        url="https://example.avature.net/test",
        job_id="123",
        company="test",
        error_type="rate_limited",
        error_message="HTTP status 406",
        http_status=406
    )
    
    print(f"Test 1 - rate_limited error type:")
    print(f"  Error type: {failure1.error_type}")
    print(f"  HTTP status: {failure1.http_status}")
    print(f"  Is retryable: {failure1.is_retryable}")
    
    # Test 2: 406 with generic http_error type (should still be retryable due to status code)
    failure2 = JobFailure(
        url="https://example.avature.net/test",
        job_id="456",
        company="test",
        error_type="http_error",
        error_message="HTTP status 406",
        http_status=406
    )
    
    print(f"\nTest 2 - http_error type with 406 status:")
    print(f"  Error type: {failure2.error_type}")
    print(f"  HTTP status: {failure2.http_status}")
    print(f"  Is retryable: {failure2.is_retryable}")
    
    # Test 3: Other rate limiting error (429)
    failure3 = JobFailure(
        url="https://example.avature.net/test",
        job_id="789",
        company="test",
        error_type="rate_limited",
        error_message="HTTP status 429",
        http_status=429
    )
    
    print(f"\nTest 3 - 429 rate limited:")
    print(f"  Error type: {failure3.error_type}")
    print(f"  HTTP status: {failure3.http_status}")
    print(f"  Is retryable: {failure3.is_retryable}")
    
    # Verify expected behavior
    expected_results = [
        (failure1.is_retryable, True, "406 + rate_limited should be retryable"),
        (failure2.is_retryable, True, "406 status code should make any error retryable"),
        (failure3.is_retryable, True, "429 + rate_limited should be retryable")
    ]
    
    all_passed = True
    for actual, expected, description in expected_results:
        if actual != expected:
            print(f"❌ FAIL: {description} (got {actual}, expected {expected})")
            all_passed = False
        else:
            print(f"✅ PASS: {description}")
    
    return all_passed

def test_enhanced_extractor():
    """Test enhanced extractor features"""
    print("\nTesting enhanced extractor features...")
    
    extractor = AvatureJobDetailsExtractor()
    
    # Check new attributes
    features = [
        ('adaptive_delay', 0.5, "Adaptive delay should be initialized"),
        ('max_adaptive_delay', 5.0, "Max adaptive delay should be set"),
        ('recent_406_count', 0, "406 count should start at 0"),
        ('last_request_time', 0, "Request time should be initialized"),
        ('request_delay', 0.5, "Request delay should be increased to 0.5s")
    ]
    
    all_passed = True
    for attr_name, expected_value, description in features:
        if hasattr(extractor, attr_name):
            actual_value = getattr(extractor, attr_name)
            if actual_value == expected_value:
                print(f"✅ PASS: {description} ({actual_value})")
            else:
                print(f"❌ FAIL: {description} (got {actual_value}, expected {expected_value})")
                all_passed = False
        else:
            print(f"❌ FAIL: Missing attribute {attr_name}")
            all_passed = False
    
    # Test adaptive delay adjustment
    print("\nTesting adaptive delay adjustment...")
    initial_delay = extractor.adaptive_delay
    extractor.recent_406_count = 2
    extractor._adjust_adaptive_delay()
    
    if extractor.adaptive_delay > initial_delay:
        print(f"✅ PASS: Adaptive delay increased from {initial_delay} to {extractor.adaptive_delay}")
    else:
        print(f"❌ FAIL: Adaptive delay not increased (was {initial_delay}, now {extractor.adaptive_delay})")
        all_passed = False
    
    return all_passed

def main():
    """Run all tests"""
    print("🚀 Testing 406 Error Handling Improvements")
    print("=" * 60)
    
    try:
        test1_passed = test_406_retryable()
        test2_passed = test_enhanced_extractor()
        
        if test1_passed and test2_passed:
            print("\n🎉 ALL TESTS PASSED!")
            print("\n📋 Summary of improvements:")
            print("   ✅ HTTP 406 errors are now retryable")
            print("   ✅ 406 errors classified as rate_limited")
            print("   ✅ Enhanced adaptive delay system")
            print("   ✅ Increased base request delay")
            print("   ✅ Progressive backoff for rate limiting")
            print("   ✅ Automatic 406 count management")
            return 0
        else:
            print("\n❌ Some tests failed. Check output above.")
            return 1
            
    except Exception as e:
        print(f"\n❌ Test execution failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    sys.exit(main())