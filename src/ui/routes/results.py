"""
Results Blueprint

This module defines the routes for managing extraction results,
including viewing, exporting, and analyzing results.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file
import sys
from pathlib import Path
import json
import tempfile
import os

# Add parent directory to path to import from src
parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.append(str(parent_dir))

from src.ui.services.result_service import ResultService
from src.ui.services.job_service import JobService
from src.utils.logger import setup_logger

# Initialize logger
logger = setup_logger("results_routes")

# Create blueprint
results_bp = Blueprint("results", __name__)

@results_bp.route("/")
def list():
    """List all results"""
    try:
        result_service = ResultService()
        results = result_service.get_all_results()
        
        # Get job filter if provided
        job_id = request.args.get("job_id")
        
        # Filter results by job if needed
        if job_id:
            results = [result for result in results if result.get("job_id") == job_id]
            
            # Get job information
            job_service = JobService()
            job = job_service.get_job_by_id(job_id)
        else:
            job = None
        
        # Sort results by created date (newest first)
        results = sorted(results, key=lambda x: x.get("created_at", ""), reverse=True)
        
        return render_template("results/list.html", results=results, job=job)
    except Exception as e:
        logger.error(f"Error listing results: {e}")
        flash(f"Error loading results: {e}", "danger")
        return render_template("results/list.html", results=[], job=None)

@results_bp.route("/view/<result_id>")
def view(result_id):
    """View result details"""
    try:
        result_service = ResultService()
        result = result_service.get_result_by_id(result_id)
        
        if not result:
            flash("Result not found", "warning")
            return redirect(url_for("results.list"))
        
        # Get job information if available
        job = None
        if result.get("job_id"):
            job_service = JobService()
            job = job_service.get_job_by_id(result.get("job_id"))
        
        return render_template("results/view.html", result=result, job=job)
    except Exception as e:
        logger.error(f"Error viewing result: {e}")
        flash(f"Error viewing result: {e}", "danger")
        return redirect(url_for("results.list"))

@results_bp.route("/export/<result_id>")
def export_result(result_id):
    """Export a result in the specified format"""
    try:
        format = request.args.get("format", "json")
        
        result_service = ResultService()
        result = result_service.get_result_by_id(result_id)
        
        if not result:
            flash("Result not found", "warning")
            return redirect(url_for("results.list"))
        
        # Create a temporary file for the export
        with tempfile.NamedTemporaryFile(delete=False, suffix=f'.{format}') as temp_file:
            temp_path = temp_file.name
            
            # Export the result to the temporary file
            if format == "json":
                # Export as JSON
                json.dump(result, temp_file, indent=2)
                mime_type = "application/json"
            elif format == "csv":
                # In a real application, this would convert to CSV
                # For demonstration, we'll just write the JSON for now
                json.dump(result, temp_file, indent=2)
                mime_type = "text/csv"
            else:
                # Default to JSON
                json.dump(result, temp_file, indent=2)
                mime_type = "application/json"
        
        # Generate a filename for the download
        filename = f"result_{result_id}.{format}"
        
        # Send the file as an attachment
        return send_file(
            temp_path, 
            as_attachment=True,
            download_name=filename,
            mimetype=mime_type
        )
    except Exception as e:
        logger.error(f"Error exporting result: {e}")
        flash(f"Error exporting result: {e}", "danger")
        return redirect(url_for("results.view", result_id=result_id))

@results_bp.route("/delete/<result_id>", methods=["POST"])
def delete_result(result_id):
    """Delete a result"""
    try:
        result_service = ResultService()
        result = result_service.get_result_by_id(result_id)
        
        if not result:
            flash("Result not found", "warning")
            return redirect(url_for("results.list"))
        
        # Delete the result
        success = result_service.delete_result(result_id)
        
        if success:
            flash("Result deleted successfully", "success")
        else:
            flash("Failed to delete result", "danger")
        
        # Redirect back to the results list for the job if specified
        if result.get("job_id"):
            return redirect(url_for("results.list", job_id=result.get("job_id")))
        else:
            return redirect(url_for("results.list"))
    except Exception as e:
        logger.error(f"Error deleting result: {e}")
        flash(f"Error deleting result: {e}", "danger")
        return redirect(url_for("results.list"))

@results_bp.route("/api/result/<result_id>")
def get_result_api(result_id):
    """Get result data as JSON for AJAX requests"""
    try:
        result_service = ResultService()
        result = result_service.get_result_by_id(result_id)
        
        if not result:
            return jsonify({"success": False, "error": "Result not found"}), 404
        
        return jsonify({"success": True, "data": result})
    except Exception as e:
        logger.error(f"Error getting result data: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@results_bp.route("/analyze/<result_id>")
def analyze_result(result_id):
    """Analyze a result with visualizations and summaries"""
    try:
        result_service = ResultService()
        result = result_service.get_result_by_id(result_id)
        
        if not result:
            flash("Result not found", "warning")
            return redirect(url_for("results.list"))
        
        # In a real application, this would generate analysis and visualizations
        # For demonstration, we'll just pass the raw result
        
        # Get job information if available
        job = None
        if result.get("job_id"):
            job_service = JobService()
            job = job_service.get_job_by_id(result.get("job_id"))
        
        return render_template("results/analyze.html", result=result, job=job)
    except Exception as e:
        logger.error(f"Error analyzing result: {e}")
        flash(f"Error analyzing result: {e}", "danger")
        return redirect(url_for("results.view", result_id=result_id))

@results_bp.route("/compare")
def compare_results():
    """Compare multiple results"""
    try:
        # Get result IDs from the query string
        result_ids = request.args.getlist("result_id")
        
        if not result_ids or len(result_ids) < 2:
            flash("Please select at least two results to compare", "warning")
            return redirect(url_for("results.list"))
        
        # Get the results
        result_service = ResultService()
        results = [result_service.get_result_by_id(result_id) for result_id in result_ids]
        
        # Filter out any None results (not found)
        results = [result for result in results if result]
        
        if len(results) < 2:
            flash("Not enough valid results to compare", "warning")
            return redirect(url_for("results.list"))
        
        return render_template("results/compare.html", results=results)
    except Exception as e:
        logger.error(f"Error comparing results: {e}")
        flash(f"Error comparing results: {e}", "danger")
        return redirect(url_for("results.list"))
