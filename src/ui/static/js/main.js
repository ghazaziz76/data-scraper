/**
 * Data Scraper - Main JavaScript
 * 
 * This file contains the main JavaScript functionality for the Data Scraper UI.
 * It includes common utilities and initialization code used across the application.
 */

// Initialize when the document is ready
$(document).ready(function() {
    // Initialize tooltips
    $('[data-bs-toggle="tooltip"]').tooltip();
    
    // Initialize popovers
    $('[data-bs-toggle="popover"]').popover();
    
    // Load recent jobs in sidebar
    loadRecentJobs();
    
    // Set up refresh button on dashboard
    $('#refresh-dashboard').on('click', function() {
        refreshDashboard();
    });
    
    // Set up automatic refresh for running jobs (every 10 seconds)
    if ($('.job-status-running').length > 0) {
        setInterval(function() {
            refreshJobStatus();
        }, 10000);
    }
});

/**
 * Load recent jobs in the sidebar
 */
function loadRecentJobs() {
    $.ajax({
        url: '/api/recent-jobs',
        type: 'GET',
        dataType: 'json',
        success: function(response) {
            if (response.success) {
                updateRecentJobsList(response.data);
            }
        },
        error: function(xhr, status, error) {
            console.error('Error loading recent jobs:', error);
            $('#recent-jobs-list').html('<li class="nav-item"><div class="nav-link text-danger"><i class="fas fa-exclamation-circle"></i> Failed to load jobs</div></li>');
        }
    });
}

/**
 * Update the recent jobs list in the sidebar
 */
function updateRecentJobsList(jobs) {
    if (!jobs || jobs.length === 0) {
        $('#recent-jobs-list').html('<li class="nav-item"><div class="nav-link text-muted"><i class="fas fa-info-circle"></i> No recent jobs</div></li>');
        return;
    }
    
    let html = '';
    jobs.forEach(function(job) {
        let statusIcon = getStatusIcon(job.status);
        html += `
            <li class="nav-item">
                <a class="nav-link" href="/jobs/view/${job.id}" title="${job.name}">
                    ${statusIcon} ${truncateText(job.name, 20)}
                </a>
            </li>
        `;
    });
    
    $('#recent-jobs-list').html(html);
}

/**
 * Get an icon representing the job status
 */
function getStatusIcon(status) {
    switch(status.toLowerCase()) {
        case 'running':
            return '<i class="fas fa-spinner fa-spin text-primary"></i>';
        case 'completed':
            return '<i class="fas fa-check-circle text-success"></i>';
        case 'failed':
            return '<i class="fas fa-times-circle text-danger"></i>';
        case 'queued':
            return '<i class="fas fa-clock text-warning"></i>';
        default:
            return '<i class="fas fa-question-circle text-secondary"></i>';
    }
}

/**
 * Refresh the dashboard data
 */
function refreshDashboard() {
    // Show loading indicator
    $('#refresh-dashboard').html('<i class="fas fa-sync-alt fa-spin"></i> Refreshing');
    
    // Reload job statistics
    $.ajax({
        url: '/api/job-stats',
        type: 'GET',
        dataType: 'json',
        success: function(response) {
            if (response.success) {
                $('#total-jobs').text(response.data.total);
                $('#completed-jobs').text(response.data.completed);
                $('#running-jobs').text(response.data.running);
            }
        }
    });
    
    // Reload result statistics
    $.ajax({
        url: '/api/result-stats',
        type: 'GET',
        dataType: 'json',
        success: function(response) {
            if (response.success) {
                $('#total-results').text(response.data.total);
            }
        }
    });
    
    // Reload recent jobs table
    $.ajax({
        url: '/api/recent-jobs',
        type: 'GET',
        dataType: 'json',
        success: function(response) {
            if (response.success) {
                updateRecentJobsTable(response.data);
            }
        },
        complete: function() {
            // Hide loading indicator
            setTimeout(function() {
                $('#refresh-dashboard').html('<i class="fas fa-sync-alt"></i> Refresh');
            }, 500);
        }
    });
}

/**
 * Update the recent jobs table on the dashboard
 */
function updateRecentJobsTable(jobs) {
    if (!jobs || jobs.length === 0) {
        $('#recent-jobs-table').html('<tr><td colspan="6" class="text-center">No jobs found</td></tr>');
        return;
    }
    
    let html = '';
    jobs.forEach(function(job) {
        html += `
            <tr>
                <td>${job.name}</td>
                <td>${job.type}</td>
                <td>
                    <span class="status-badge status-${job.status.toLowerCase()}">
                        ${job.status}
                    </span>
                </td>
                <td>${formatDate(job.created_at)}</td>
                <td>
                    <div class="progress">
                        <div class="progress-bar" role="progressbar" style="width: ${job.progress}%"
                            aria-valuenow="${job.progress}" aria-valuemin="0" aria-valuemax="100">
                            ${job.progress}%
                        </div>
                    </div>
                </td>
                <td>
                    <a href="/jobs/view/${job.id}" class="btn btn-sm btn-outline-primary">
                        <i class="fas fa-eye"></i>
                    </a>
                </td>
            </tr>
        `;
    });
    
    $('#recent-jobs-table').html(html);
}

/**
 * Refresh the status of running jobs
 */
function refreshJobStatus() {
    $('.job-status-running').each(function() {
        const jobId = $(this).data('job-id');
        const statusElement = $(this);
        const progressBar = $(`#progress-${jobId}`);
        
        $.ajax({
            url: `/jobs/api/status/${jobId}`,
            type: 'GET',
            dataType: 'json',
            success: function(response) {
                if (response.success) {
                    // Update progress bar
                    progressBar.css('width', `${response.data.progress}%`);
                    progressBar.text(`${response.data.progress}%`);
                    
                    // Update status if changed
                    if (response.data.status.toLowerCase() !== 'running') {
                        location.reload();
                    }
                }
            }
        });
    });
}

/**
 * Format a date string
 */
function formatDate(dateString) {
    if (!dateString) return '';
    
    const date = new Date(dateString);
    return date.toLocaleString();
}

/**
 * Truncate text if it exceeds the specified length
 */
function truncateText(text, maxLength) {
    if (!text) return '';
    if (text.length <= maxLength) return text;
    
    return text.substring(0, maxLength) + '...';
}

/**
 * Show a confirmation dialog
 */
function confirmAction(message, callback) {
    if (confirm(message)) {
        callback();
    }
}

/**
 * Show a loading overlay
 */
function showLoading(targetElement) {
    const loadingHtml = `
        <div class="loading-overlay">
            <div class="spinner-border text-primary loading-spinner" role="status">
                <span class="visually-hidden">Loading...</span>
            </div>
        </div>
    `;
    
    $(targetElement).css('position', 'relative').append(loadingHtml);
}

/**
 * Hide the loading overlay
 */
function hideLoading(targetElement) {
    $(targetElement).find('.loading-overlay').remove();
}
