"""
Dashboard Blueprint

This module defines the routes for the dashboard, which provides an overview
of the Data Scraper application status and recent activity.
"""

from flask import Blueprint, render_template, jsonify
import sys
from pathlib import Path
import datetime
import random

# Add parent directory to path to import from src
parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.append(str(parent_dir))

from src.ui.services.job_service import JobService
from src.ui.services.result_service import ResultService
from src.utils.logger import setup_logger

# Initialize logger
logger = setup_logger("dashboard_routes")

# Create blueprint
dashboard_bp = Blueprint("dashboard", __name__)

@dashboard_bp.route("/")
def home():
    """Render the dashboard home page"""
    try:
        # Get job statistics
        job_service = JobService()
        job_stats = job_service.get_job_statistics()
        
        # Get recent jobs
        recent_jobs = job_service.get_recent_jobs(limit=5)
        
        # Get result statistics
        result_service = ResultService()
        result_stats = result_service.get_result_statistics()
        
        # Generate sample data for activity chart
        activity_data = _generate_activity_data()
        
        # Get system status
        system_status = _get_system_status()
        
        return render_template(
            "dashboard.html",
            job_stats=job_stats,
            recent_jobs=recent_jobs,
            result_stats=result_stats,
            activity_data=activity_data,
            system_status=system_status
        )
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        return render_template("dashboard.html", error=str(e))

@dashboard_bp.route("/api/job-stats")
def job_stats_api():
    """Return job statistics as JSON for AJAX requests"""
    try:
        job_service = JobService()
        job_stats = job_service.get_job_statistics()
        return jsonify({"success": True, "data": job_stats})
    except Exception as e:
        logger.error(f"Error getting job stats: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@dashboard_bp.route("/api/recent-jobs")
def recent_jobs_api():
    """Return recent jobs as JSON for AJAX requests"""
    try:
        job_service = JobService()
        recent_jobs = job_service.get_recent_jobs(limit=5)
        return jsonify({"success": True, "data": recent_jobs})
    except Exception as e:
        logger.error(f"Error getting recent jobs: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@dashboard_bp.route("/api/result-stats")
def result_stats_api():
    """Return result statistics as JSON for AJAX requests"""
    try:
        result_service = ResultService()
        result_stats = result_service.get_result_statistics()
        return jsonify({"success": True, "data": result_stats})
    except Exception as e:
        logger.error(f"Error getting result stats: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@dashboard_bp.route("/api/activity-data")
def activity_data_api():
    """Return activity data as JSON for chart"""
    try:
        activity_data = _generate_activity_data()
        return jsonify({"success": True, "data": activity_data})
    except Exception as e:
        logger.error(f"Error getting activity data: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@dashboard_bp.route("/api/system-status")
def system_status_api():
    """Return system status as JSON for AJAX requests"""
    try:
        system_status = _get_system_status()
        return jsonify({"success": True, "data": system_status})
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

def _generate_activity_data():
    """Generate sample activity data for the dashboard chart"""
    # In a real application, this would fetch data from a database
    # For now, we'll generate some sample data
    
    # Get the current date
    now = datetime.datetime.now()
    
    # Generate data for the last 7 days
    days = []
    jobs_data = []
    results_data = []
    
    for i in range(6, -1, -1):
        day = now - datetime.timedelta(days=i)
        days.append(day.strftime("%Y-%m-%d"))
        
        # Generate random data for jobs and results
        # In a real application, this would come from a database
        jobs_count = 5 + (i * 2) + random.randint(-2, 2)
        results_count = jobs_count * 3 + random.randint(-5, 5)
        
        jobs_data.append(jobs_count)
        results_data.append(results_count)
    
    return {
        "labels": days,
        "datasets": [
            {
                "label": "Jobs",
                "data": jobs_data,
                "borderColor": "#4a6bff",
                "backgroundColor": "rgba(74, 107, 255, 0.2)"
            },
            {
                "label": "Results",
                "data": results_data,
                "borderColor": "#28a745",
                "backgroundColor": "rgba(40, 167, 69, 0.2)"
            }
        ]
    }

def _get_system_status():
    """Get system status information"""
    # In a real application, this would fetch actual system metrics
    # For now, we'll generate some sample data
    
    return {
        "cpu_usage": random.randint(15, 45),
        "memory_usage": random.randint(30, 60),
        "disk_usage": random.randint(40, 70),
        "active_connections": random.randint(1, 10),
        "uptime": "7 days, 12 hours"
    }
