"""
Config Blueprint

This module defines the routes for managing configurations,
including configuration creation, viewing, editing, and deletion.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
import sys
from pathlib import Path
import json

# Add parent directory to path to import from src
parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.append(str(parent_dir))

from src.ui.services.config_service import ConfigService
from src.utils.logger import setup_logger

# Initialize logger
logger = setup_logger("config_routes")

# Create blueprint
config_bp = Blueprint("config", __name__)

@config_bp.route("/")
def list():
    """List all configurations"""
    try:
        config_service = ConfigService()
        configs = config_service.get_all_configs()
        
        # Get type filter if provided
        type_filter = request.args.get("type", "all")
        
        # Filter configs by type if needed
        if type_filter != "all":
            configs = [config for config in configs if config.get("type", "") == type_filter]
        
        # Sort configs by name
        configs = sorted(configs, key=lambda x: x.get("name", "").lower())
        
        return render_template("config/list.html", configs=configs, type_filter=type_filter)
    except Exception as e:
        logger.error(f"Error listing configurations: {e}")
        flash(f"Error loading configurations: {e}", "danger")
        return render_template("config/list.html", configs=[], type_filter="all")

@config_bp.route("/create", methods=["GET", "POST"])
def create():
    """Create a new configuration"""
    try:
        config_service = ConfigService()
        
        if request.method == "POST":
            # Get form data
            config_data = {
                "name": request.form.get("name"),
                "type": request.form.get("type"),
                "description": request.form.get("description", ""),
            }
            
            # Parse the configuration JSON
            try:
                config_json = json.loads(request.form.get("config_json", "{}"))
                config_data["config"] = config_json
            except json.JSONDecodeError:
                flash("Invalid JSON configuration", "danger")
                return render_template("config/create.html", 
                                      templates=config_service.get_config_templates(),
                                      config_data=config_data)
            
            # Create the configuration
            config_id = config_service.create_config(config_data)
            
            # Redirect to the configuration view page
            flash(f"Configuration '{config_data['name']}' created successfully", "success")
            return redirect(url_for("config.view", config_id=config_id))
        else:
            # Get available templates for the form
            templates = config_service.get_config_templates()
            
            return render_template("config/create.html", templates=templates)
    except Exception as e:
        logger.error(f"Error creating configuration: {e}")
        flash(f"Error creating configuration: {e}", "danger")
        return redirect(url_for("config.list"))

@config_bp.route("/view/<config_id>")
def view(config_id):
    """View configuration details"""
    try:
        config_service = ConfigService()
        config = config_service.get_config_by_id(config_id)
        
        if not config:
            flash("Configuration not found", "warning")
            return redirect(url_for("config.list"))
        
        return render_template("config/view.html", config=config)
    except Exception as e:
        logger.error(f"Error viewing configuration: {e}")
        flash(f"Error viewing configuration: {e}", "danger")
        return redirect(url_for("config.list"))

@config_bp.route("/edit/<config_id>", methods=["GET", "POST"])
def edit(config_id):
    """Edit a configuration"""
    try:
        config_service = ConfigService()
        config = config_service.get_config_by_id(config_id)
        
        if not config:
            flash("Configuration not found", "warning")
            return redirect(url_for("config.list"))
        
        if request.method == "POST":
            # Get form data
            update_data = {
                "name": request.form.get("name"),
                "description": request.form.get("description", ""),
            }
            
            # Parse the configuration JSON
            try:
                config_json = json.loads(request.form.get("config_json", "{}"))
                update_data["config"] = config_json
            except json.JSONDecodeError:
                flash("Invalid JSON configuration", "danger")
                return render_template("config/edit.html", config=config)
            
            # Update the configuration
            success = config_service.update_config(config_id, update_data)
            
            if success:
                flash(f"Configuration '{update_data['name']}' updated successfully", "success")
                return redirect(url_for("config.view", config_id=config_id))
            else:
                flash("Failed to update configuration", "danger")
                return render_template("config/edit.html", config=config)
        else:
            return render_template("config/edit.html", config=config)
    except Exception as e:
        logger.error(f"Error editing configuration: {e}")
        flash(f"Error editing configuration: {e}", "danger")
        return redirect(url_for("config.list"))

@config_bp.route("/delete/<config_id>", methods=["POST"])
def delete(config_id):
    """Delete a configuration"""
    try:
        config_service = ConfigService()
        config = config_service.get_config_by_id(config_id)
        
        if not config:
            flash("Configuration not found", "warning")
            return redirect(url_for("config.list"))
        
        # Delete the configuration
        success = config_service.delete_config(config_id)
        
        if success:
            flash(f"Configuration '{config.get('name')}' deleted successfully", "success")
        else:
            flash("Failed to delete configuration", "danger")
        
        return redirect(url_for("config.list"))
    except Exception as e:
        logger.error(f"Error deleting configuration: {e}")
        flash(f"Error deleting configuration: {e}", "danger")
        return redirect(url_for("config.list"))

@config_bp.route("/api/template/<template_id>")
def get_template_api(template_id):
    """Get a configuration template as JSON for AJAX requests"""
    try:
        config_service = ConfigService()
        template = config_service.get_default_config(template_id)
        
        if not template:
            return jsonify({"success": False, "error": "Template not found"}), 404
        
        return jsonify({"success": True, "data": template})
    except Exception as e:
        logger.error(f"Error getting template: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
