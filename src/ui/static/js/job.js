/**
 * Data Scraper - Jobs JavaScript
 * 
 * This file contains JavaScript functionality specific to job management.
 */

$(document).ready(function() {
    // Initialize tooltips
    $('[data-bs-toggle="tooltip"]').tooltip();
    
    // Set up dynamic form based on job type selection in create/edit forms
    initJobTypeSelectionHandlers();
    
    // Set up job monitoring for running jobs
    initJobMonitoring();
});

/**
 * Initialize handlers for job type selection in forms
 */
function initJobTypeSelectionHandlers() {
    $('input[name="type"]').change(function() {
        // Hide all configuration sections
        $('#web_scraper_config, #file_processor_config, #api_connector_config, #batch_processor_config').hide();
        
        // Show the selected configuration section
        $('#' + $(this).val() + '_config').show();
    });
}

/**
 * Initialize job monitoring functionality
 */
function initJobMonitoring() {
    // Check if there are any running jobs to monitor
    const runningJobs = $('.job-status-running');
    
    if (runningJobs.length > 0) {
        // Set up refresh interval for each running job
        runningJobs.each(function() {
            const jobId = $(this).data('job-id');
            const progressBar = $(`#progress-${jobId}`);
            
            // Set up refresh interval
            const refreshInterval = setInterval(function() {
                refreshJobStatus(jobId, progressBar, refreshInterval);
            }, 5000); // Refresh every 5 seconds
        });
    }
}

/**
 * Refresh the status of a specific job
 */
function refreshJobStatus(jobId, progressBar, refreshInterval) {
    $.ajax({
        url: `/jobs/api/status/${jobId}`,
        type: 'GET',
        dataType: 'json',
        success: function(response) {
            if (response.success) {
                // Update progress bar if it exists
                if (progressBar && progressBar.length > 0) {
                    progressBar.css('width', `${response.data.progress}%`);
                    progressBar.text(`${response.data.progress}%`);
                } else {
                    // Update the progress bar on the job view page
                    $('.progress-bar').css('width', `${response.data.progress}%`);
                    $('.progress-bar').text(`${response.data.progress}%`);
                }
                
                // If status has changed from running, reload the page
                if (response.data.status.toLowerCase() !== 'running') {
                    clearInterval(refreshInterval);
                    location.reload();
                }
            }
        },
        error: function() {
            // Stop refreshing on error
            clearInterval(refreshInterval);
        }
    });
}

/**
 * Handle job run action
 */
function runJob(jobId) {
    if (confirm("Are you sure you want to run this job?")) {
        // Show loading overlay
        showLoading('.card');
        
        $.ajax({
            url: `/jobs/run/${jobId}`,
            type: 'POST',
            success: function(response) {
                location.reload();
            },
            error: function(xhr) {
                hideLoading('.card');
                alert("Error starting job. Please try again.");
            }
        });
    }
}

/**
 * Handle job cancel action
 */
function cancelJob(jobId) {
    if (confirm("Are you sure you want to cancel this job?")) {
        // Show loading overlay
        showLoading('.card');
        
        $.ajax({
            url: `/jobs/cancel/${jobId}`,
            type: 'POST',
            success: function(response) {
                location.reload();
            },
            error: function(xhr) {
                hideLoading('.card');
                alert("Error cancelling job. Please try again.");
            }
        });
    }
}

/**
 * Handle job delete action
 */
function deleteJob(jobId, jobName) {
    if (confirm(`Are you sure you want to delete the job "${jobName}"?`)) {
        // Show loading overlay
        showLoading('.card');
        
        $.ajax({
            url: `/jobs/delete/${jobId}`,
            type: 'POST',
            success: function(response) {
                window.location.href = '/jobs';
            },
            error: function(xhr) {
                hideLoading('.card');
                alert("Error deleting job. Please try again.");
            }
        });
    }
}

/**
 * Validate job form before submission
 */
function validateJobForm() {
    // Get the selected job type
    const selectedType = $('input[name="type"]:checked').val();
    
    // Validate required fields based on job type
    if (selectedType === 'web_scraper') {
        if (!$('#url').val()) {
            alert("URL is required for Web Scraper jobs.");
            return false;
        }
    } else if (selectedType === 'file_processor') {
        if (!$('#file_path').val()) {
            alert("File Path is required for File Processor jobs.");
            return false;
        }
    } else if (selectedType === 'api_connector') {
        if (!$('#api_url').val()) {
            alert("API URL is required for API Connector jobs.");
            return false;
        }
    } else if (selectedType === 'batch_processor') {
        if (!$('#sources').val()) {
            alert("At least one source is required for Batch Processor jobs.");
            return false;
        }
    }
    
    return true;
}
