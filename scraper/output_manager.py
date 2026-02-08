"""
Output Manager for Job Details Extractor
Handles structured output generation and file management
"""

import json
import logging
import re
from typing import List, Dict, Optional, Set
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from .job_details_extractor import Job, JobFailure

logger = logging.getLogger(__name__)


class OutputManager:
    """
    Manages all output generation for job details extraction
    Creates structured outputs: job details, retries, failures, statistics
    """
    
    def __init__(self, output_dir: str = ".", create_subdirs: bool = True):
        self.output_dir = Path(output_dir)
        self.create_subdirs = create_subdirs
        
        if create_subdirs:
            self.job_details_dir = self.output_dir / "job_details"
            self.retries_dir = self.output_dir / "retries"
            self.failures_dir = self.output_dir / "failures"
            self.logs_dir = self.output_dir / "logs"
            
            # Create directories
            for dir_path in [self.job_details_dir, self.retries_dir, self.failures_dir, self.logs_dir]:
                dir_path.mkdir(parents=True, exist_ok=True)
        else:
            self.job_details_dir = self.output_dir
            self.retries_dir = self.output_dir
            self.failures_dir = self.output_dir
            self.logs_dir = self.output_dir
    
    def save_extraction_results(self, 
                              jobs: List[Job], 
                              failures: List[JobFailure],
                              extraction_metadata: Dict,
                              output_prefix: str = "job_details") -> Dict[str, str]:
        """
        Save complete extraction results
        Returns dict of generated file paths
        """
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        safe_prefix = self._sanitize_filename(output_prefix)
        
        files_created = {}
        
        # Save job details
        if jobs:
            job_details_file = self.job_details_dir / f"{safe_prefix}_{timestamp}.jsonl"
            self._save_job_details(jobs, job_details_file)
            files_created['job_details'] = str(job_details_file)
        
        # Save failures for analysis
        if failures:
            failures_file = self.failures_dir / f"failures_{safe_prefix}_{timestamp}.jsonl"
            self._save_failures_analysis(failures, failures_file)
            files_created['failures'] = str(failures_file)
        
        # Save extraction statistics
        stats_file = self.logs_dir / f"extraction_stats_{safe_prefix}_{timestamp}.json"
        self._save_extraction_statistics(jobs, failures, extraction_metadata, stats_file)
        files_created['statistics'] = str(stats_file)
        
        # Save summary report
        summary_file = self.logs_dir / f"extraction_summary_{safe_prefix}_{timestamp}.json"
        self._save_extraction_summary(jobs, failures, extraction_metadata, summary_file)
        files_created['summary'] = str(summary_file)
        
        logger.info(f"✓ Saved extraction results to {len(files_created)} files")
        
        return files_created
    
    def _save_job_details(self, jobs: List[Job], file_path: Path):
        """Save job details in JSONL format"""
        with open(file_path, 'w', encoding='utf-8') as f:
            for job in jobs:
                job_data = asdict(job)
                # Add extraction metadata
                job_data['extraction_metadata'] = {
                    'extracted_at': datetime.utcnow().isoformat(),
                    'extractor_version': '1.0.0',
                    'fields_extracted': [key for key, value in job_data.items() if value is not None]
                }
                f.write(json.dumps(job_data, ensure_ascii=False) + '\n')
        
        logger.info(f"✓ Saved {len(jobs)} job details to {file_path}")
    
    def _save_failures_analysis(self, failures: List[JobFailure], file_path: Path):
        """Save failures with comprehensive analysis"""
        failure_analysis = {
            'metadata': {
                'created_at': datetime.utcnow().isoformat(),
                'total_failures': len(failures),
                'analysis': self._analyze_failures(failures)
            },
            'failures': [asdict(failure) for failure in failures]
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(failure_analysis, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✓ Saved {len(failures)} failures with analysis to {file_path}")
    
    def _save_extraction_statistics(self, jobs: List[Job], failures: List[JobFailure], 
                                  metadata: Dict, file_path: Path):
        """Save detailed extraction statistics"""
        total_processed = len(jobs) + len(failures)
        success_rate = (len(jobs) / total_processed * 100) if total_processed > 0 else 0
        
        # Field extraction analysis
        field_stats = self._analyze_field_extraction(jobs)
        
        # Performance metrics
        duration = metadata.get('duration_seconds', 0)
        throughput = total_processed / duration if duration > 0 else 0
        
        stats = {
            'extraction_metadata': {
                'timestamp': datetime.utcnow().isoformat(),
                'input_source': metadata.get('input_file', 'unknown'),
                'extractor_settings': metadata.get('settings', {}),
                'duration_seconds': duration
            },
            'summary': {
                'total_processed': total_processed,
                'successful_extractions': len(jobs),
                'failed_extractions': len(failures),
                'success_rate_percent': round(success_rate, 2),
                'throughput_per_second': round(throughput, 2)
            },
            'field_analysis': field_stats,
            'failure_breakdown': self._get_failure_breakdown(failures),
            'company_breakdown': self._get_company_breakdown(jobs, failures),
            'performance_metrics': {
                'avg_extraction_time': metadata.get('avg_extraction_time', 0),
                'worker_utilization': metadata.get('worker_utilization', 0),
                'rate_limit_incidents': sum(1 for f in failures if f.error_type == 'rate_limited')
            }
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✓ Saved extraction statistics to {file_path}")
    
    def _save_extraction_summary(self, jobs: List[Job], failures: List[JobFailure], 
                                metadata: Dict, file_path: Path):
        """Save human-readable extraction summary"""
        total_processed = len(jobs) + len(failures)
        success_rate = (len(jobs) / total_processed * 100) if total_processed > 0 else 0
        
        # Get top failure reasons
        failure_counts = {}
        for failure in failures:
            failure_counts[failure.error_type] = failure_counts.get(failure.error_type, 0) + 1
        
        top_failures = sorted(failure_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        
        # Company statistics
        company_stats = {}
        for job in jobs:
            company_stats[job.company] = company_stats.get(job.company, 0) + 1
        
        summary = {
            'extraction_summary': {
                'timestamp': datetime.utcnow().isoformat(),
                'input_file': metadata.get('input_file', 'unknown'),
                'total_urls_processed': total_processed,
                'successful_extractions': len(jobs),
                'failed_extractions': len(failures),
                'success_rate': f"{success_rate:.1f}%",
                'duration': f"{metadata.get('duration_seconds', 0):.1f} seconds"
            },
            'key_metrics': {
                'jobs_with_full_descriptions': sum(1 for job in jobs if job.description and len(job.description) > 100),
                'jobs_with_application_urls': sum(1 for job in jobs if job.application_url),
                'jobs_with_metadata': sum(1 for job in jobs if job.date_posted or job.department or job.employment_type),
                'unique_companies': len(set(job.company for job in jobs))
            },
            'top_failure_reasons': [
                {'error_type': error_type, 'count': count, 'percentage': f"{count/len(failures)*100:.1f}%"}
                for error_type, count in top_failures
            ] if failures else [],
            'company_breakdown': [
                {'company': company, 'jobs_extracted': count}
                for company, count in sorted(company_stats.items(), key=lambda x: x[1], reverse=True)
            ],
            'recommendations': self._generate_recommendations(jobs, failures, metadata)
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✓ Saved extraction summary to {file_path}")
    
    def _analyze_failures(self, failures: List[JobFailure]) -> Dict:
        """Analyze failure patterns"""
        analysis = {
            'total_failures': len(failures),
            'by_error_type': {},
            'by_http_status': {},
            'by_company': {},
            'retryable_failures': sum(1 for f in failures if f.is_retryable),
            'permanent_failures': sum(1 for f in failures if not f.is_retryable),
            'common_patterns': []
        }
        
        for failure in failures:
            # Count by error type
            error_type = failure.error_type
            analysis['by_error_type'][error_type] = analysis['by_error_type'].get(error_type, 0) + 1
            
            # Count by HTTP status
            if failure.http_status:
                status = str(failure.http_status)
                analysis['by_http_status'][status] = analysis['by_http_status'].get(status, 0) + 1
            
            # Count by company
            company = failure.company
            analysis['by_company'][company] = analysis['by_company'].get(company, 0) + 1
        
        # Identify patterns
        total = len(failures)
        not_found_rate = analysis['by_error_type'].get('not_found', 0) / total
        forbidden_rate = analysis['by_error_type'].get('access_forbidden', 0) / total
        timeout_rate = analysis['by_error_type'].get('timeout', 0) / total
        
        if not_found_rate > 0.3:
            analysis['common_patterns'].append(f"High 404 rate ({not_found_rate:.1%}) - URLs may be expired")
        
        if forbidden_rate > 0.2:
            analysis['common_patterns'].append(f"High 403 rate ({forbidden_rate:.1%}) - possible access restrictions")
        
        if timeout_rate > 0.1:
            analysis['common_patterns'].append(f"High timeout rate ({timeout_rate:.1%}) - server may be slow")
        
        return analysis
    
    def _analyze_field_extraction(self, jobs: List[Job]) -> Dict:
        """Analyze field extraction success rates"""
        total_jobs = len(jobs)
        
        if total_jobs == 0:
            return {}
        
        field_stats = {
            'title': {'extracted': 0, 'missing': 0},
            'location': {'extracted': 0, 'missing': 0},
            'description': {'extracted': 0, 'missing': 0},
            'date_posted': {'extracted': 0, 'missing': 0},
            'department': {'extracted': 0, 'missing': 0},
            'employment_type': {'extracted': 0, 'missing': 0},
            'application_url': {'extracted': 0, 'missing': 0}
        }
        
        for job in jobs:
            for field in field_stats.keys():
                value = getattr(job, field, None)
                if value and value != 'Not specified' and value.strip():
                    field_stats[field]['extracted'] += 1
                else:
                    field_stats[field]['missing'] += 1
        
        # Calculate percentages
        for field, stats in field_stats.items():
            stats['extraction_rate'] = round(stats['extracted'] / total_jobs * 100, 1)
        
        return field_stats
    
    def _get_failure_breakdown(self, failures: List[JobFailure]) -> Dict:
        """Get detailed failure breakdown"""
        breakdown = {
            'total': len(failures),
            'by_type': {},
            'retryable_count': 0,
            'permanent_count': 0
        }
        
        for failure in failures:
            error_type = failure.error_type
            breakdown['by_type'][error_type] = breakdown['by_type'].get(error_type, 0) + 1
            
            if failure.is_retryable:
                breakdown['retryable_count'] += 1
            else:
                breakdown['permanent_count'] += 1
        
        return breakdown
    
    def _get_company_breakdown(self, jobs: List[Job], failures: List[JobFailure]) -> Dict:
        """Get breakdown by company"""
        company_stats = {}
        
        # Count successful extractions
        for job in jobs:
            if job.company not in company_stats:
                company_stats[job.company] = {'successful': 0, 'failed': 0, 'total': 0}
            company_stats[job.company]['successful'] += 1
        
        # Count failures
        for failure in failures:
            if failure.company not in company_stats:
                company_stats[failure.company] = {'successful': 0, 'failed': 0, 'total': 0}
            company_stats[failure.company]['failed'] += 1
        
        # Calculate totals and success rates
        for company, stats in company_stats.items():
            stats['total'] = stats['successful'] + stats['failed']
            if stats['total'] > 0:
                stats['success_rate'] = round(stats['successful'] / stats['total'] * 100, 1)
            else:
                stats['success_rate'] = 0
        
        return company_stats
    
    def _generate_recommendations(self, jobs: List[Job], failures: List[JobFailure], 
                                metadata: Dict) -> List[str]:
        """Generate actionable recommendations based on results"""
        recommendations = []
        total_processed = len(jobs) + len(failures)
        
        if total_processed == 0:
            return ["No URLs were processed"]
        
        success_rate = len(jobs) / total_processed
        
        # Success rate recommendations
        if success_rate < 0.5:
            recommendations.append("Low success rate (<50%) - consider checking URL validity and server availability")
        
        # Failure pattern recommendations
        failure_types = {}
        for failure in failures:
            failure_types[failure.error_type] = failure_types.get(failure.error_type, 0) + 1
        
        if failure_types.get('timeout', 0) > len(failures) * 0.2:
            recommendations.append("High timeout rate - increase timeout settings or reduce concurrent workers")
        
        if failure_types.get('rate_limited', 0) > 0:
            recommendations.append("Rate limiting detected - reduce request rate and use retry files")
        
        if failure_types.get('not_found', 0) > len(failures) * 0.3:
            recommendations.append("Many 404 errors - URLs may be expired, verify URL source freshness")
        
        # Field extraction recommendations
        if jobs:
            description_rate = sum(1 for job in jobs if job.description and len(job.description) > 50) / len(jobs)
            if description_rate < 0.8:
                recommendations.append("Low description extraction rate - page structure may have changed")
            
            location_rate = sum(1 for job in jobs if job.location and job.location != 'Not specified') / len(jobs)
            if location_rate < 0.7:
                recommendations.append("Low location extraction rate - review location selectors")
        
        # Performance recommendations
        duration = metadata.get('duration_seconds', 0)
        if duration > 0:
            throughput = total_processed / duration
            if throughput < 1:
                recommendations.append("Low throughput (<1 job/sec) - consider optimizing extraction or increasing workers")
        
        return recommendations if recommendations else ["Extraction completed successfully with good performance"]
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for safe file creation"""
        # Remove invalid characters
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Remove multiple underscores
        sanitized = re.sub(r'_{2,}', '_', sanitized)
        # Trim and ensure not empty
        sanitized = sanitized.strip('_')
        return sanitized if sanitized else 'output'
    
    def create_extraction_report(self, jobs: List[Job], failures: List[JobFailure], 
                               metadata: Dict) -> str:
        """Create a human-readable extraction report"""
        total_processed = len(jobs) + len(failures)
        success_rate = (len(jobs) / total_processed * 100) if total_processed > 0 else 0
        
        report_lines = [
            "="*60,
            "JOB DETAILS EXTRACTION REPORT",
            "="*60,
            f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
            f"Input Source: {metadata.get('input_file', 'unknown')}",
            f"Duration: {metadata.get('duration_seconds', 0):.1f} seconds",
            "",
            "SUMMARY:",
            f"  Total URLs processed: {total_processed}",
            f"  Successful extractions: {len(jobs)}",
            f"  Failed extractions: {len(failures)}",
            f"  Success rate: {success_rate:.1f}%",
            ""
        ]
        
        if jobs:
            report_lines.extend([
                "JOB DETAILS EXTRACTED:",
                f"  Jobs with descriptions: {sum(1 for job in jobs if job.description and len(job.description) > 50)}",
                f"  Jobs with application URLs: {sum(1 for job in jobs if job.application_url)}",
                f"  Jobs with date posted: {sum(1 for job in jobs if job.date_posted)}",
                f"  Jobs with department info: {sum(1 for job in jobs if job.department)}",
                f"  Unique companies: {len(set(job.company for job in jobs))}",
                ""
            ])
        
        if failures:
            failure_counts = {}
            for failure in failures:
                failure_counts[failure.error_type] = failure_counts.get(failure.error_type, 0) + 1
            
            report_lines.extend([
                "TOP FAILURE REASONS:",
            ])
            
            for error_type, count in sorted(failure_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                percentage = count / len(failures) * 100
                report_lines.append(f"  {error_type}: {count} ({percentage:.1f}%)")
            
            report_lines.append("")
        
        recommendations = self._generate_recommendations(jobs, failures, metadata)
        if recommendations:
            report_lines.extend([
                "RECOMMENDATIONS:",
            ])
            for rec in recommendations:
                report_lines.append(f"  • {rec}")
        
        report_lines.append("="*60)
        
        return "\n".join(report_lines)