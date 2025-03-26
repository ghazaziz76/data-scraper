{% extends "base.html" %}

{% block title %}Job Details - Data Scraper{% endblock %}

{% block page_title %}{{ job.name }}{% endblock %}

{% block page_actions %}
<div class="btn-group">
    <a href="{{ url_for('jobs.list') }}" class="btn btn-sm btn-outline-secondary">
        <i class="fas fa-arrow-left"></i> Back to Jobs
    </a>
    {% if job.status == 'Queued' %}
    <button type="button" class="btn btn-sm btn-success" onclick="submitForm('run-job')">
        <i class="fas fa-play"></i> Run Job
    </button>
    {% endif %}
    {% if job.status == 'Running' %}
    <button type="button" class="btn btn-sm btn-warning" onclick="submitForm('cancel-job')">
        <i class="fas fa-stop"></i> Cancel Job
    </button>
    {% endif %}
    <button type="button" class="btn btn-sm btn-danger" onclick="confirmDelete()">
        <i class="fas fa-trash"></i> Delete Job
    </button>
</div>
<!-- Hidden forms for actions -->
<form id="run-job" action="{{ url_for('jobs.run_job', job_id=job.id) }}" method="post" class="d-none"></form>
<form id="cancel-job" action="{{ url_for('jobs.cancel_job', job_id=job.id) }}" method="post" class="d-none"></form>
<form id="delete-job" action="{{ url_for('jobs.delete', job_id=job.id) }}" method="post" class="d-none"></form>
{% endblock %}

{% block content %}
<div class="row">
    <div class="col-md-4">
        <!-- Job Details -->
        <div class="card mb-4">
            <div class="card-header">
                <i class="fas fa-info-circle"></i> Job Details
            </div>
            <div class="card-body">
                <ul class="list-group list-group-flush">
                    <li class="list-group-item d-flex justify-content-between align-items-center">
                        <span class="detail-label">ID</span>
                        <span>{{ job.id }}</span>
                    </li>
                    <li class="list-group-item d-flex justify-content-between align-items-center">
                        <span class="detail-label">Type</span>
                        <span>{{ job.type }}</span>
                    </li>
                    <li class="list-group-item d-flex justify-content-between align-items-center">
                        <span class="detail-label">Status</span>
                        <span class="status-badge status-{{ job.status.lower() }}">{{ job.status }}</span>
                    </li>
                    <li class="list-group-item d-flex justify-content-between align-items-center">
                        <span class="detail-label">Created</span>
                        <span>{{ job.created_at }}</span>
                    </li>
                    {% if job.updated_at %}
                    <li class="list-group-item d-flex justify-content-between align-items-center">
                        <span class="detail-label">Last Updated</span>
                        <span>{{ job.updated_at }}</span>
                    </li>
                    {% endif %}
                    <li class="list-group-item d-flex justify-content-between align-items-center">
                        <span class="detail-label">Priority</span>
                        <span>
                            {% if job.priority == 1 %}
                            <span class="badge bg-secondary">Low</span>
                            {% elif job.priority == 2 %}
                            <span class="badge bg-primary">Normal</span>
                            {% elif job.priority == 3 %}
                            <span class="badge bg-danger">High</span>
                            {% endif %}
                        </span>
                    </li>
                </ul>
            </div>
        </div>

        <!-- Job Progress -->
        <div class="card mb-4">
            <div class="card-header">
                <i class="fas fa-tasks"></i> Progress
            </div>
            <div class="card-body">
                <div class="mb-3">
                    <div class="progress" style="height: 20px;">
                        <div class="progress-bar {% if job.status == 'Running' %}progress-bar-striped progress-bar-animated{% endif %}" 
                             role="progressbar" 
                             style="width: {{ job.progress }}%;" 
                             aria-valuenow="{{ job.progress }}" 
                             aria-valuemin="0" 
                             aria-valuemax="100">
                            {{ job.progress }}%
                        </div>
                    </div>
                </div>
                
                {% if job.status == 'Running' %}
                <div class="job-status-running" data-job-id="{{ job.id }}">
                    <small class="text-muted">Refreshing status automatically...</small>
                </div>
                {% endif %}
            </div>
        </div>

        <!-- Configuration -->
        {% if config %}
        <div class="card mb-4">
            <div class="card-header">
                <i class="fas fa-cog"></i> Configuration
            </div>
            <div class="card-body">
                <p><strong>{{ config.name }}</strong></p>
                <p class="text-muted">{{ config.description }}</p>
                <a href="{{ url_for('config.view', config_id=config.id) }}" class="btn btn-sm btn-outline-primary">
                    <i class="fas fa-eye"></i> View Configuration
                </a>
            </div>
        </div>
        {% endif %}
    </div>

    <div class="col-md-8">
        <!-- Job Description -->
        {% if job.description %}
        <div class="card mb-4">
            <div class="card-header">
                <i class="fas fa-align-left"></i> Description
            </div>
            <div class="card-body">
                <p>{{ job.description }}</p>
            </div>
        </div>
        {% endif %}

        <!-- Source Details -->
        <div class="card mb-4">
            <div class="card-header">
                <i class="fas fa-database"></i> Source Details
            </div>
            <div class="card-body">
                {% if job.type == 'web_scraper' and job.source %}
                <dl class="row">
                    <dt class="col-sm-3">URL</dt>
                    <dd class="col-sm-9"><a href="{{ job.source.url }}" target="_blank">{{ job.source.url }}</a></dd>
                    
                    <dt class="col-sm-3">Follow Links</dt>
                    <dd class="col-sm-9">{% if job.source.follow_links %}Yes{% else %}No{% endif %}</dd>
                    
                    {% if job.source.follow_links %}
                    <dt class="col-sm-3">Max Depth</dt>
                    <dd class="col-sm-9">{{ job.source.max_depth }}</dd>
                    {% endif %}
                </dl>
                {% elif job.type == 'file_processor' and job.source %}
                <dl class="row">
                    <dt class="col-sm-3">File Path</dt>
                    <dd class="col-sm-9">{{ job.source.file_path }}</dd>
                    
                    <dt class="col-sm-3">File Type</dt>
                    <dd class="col-sm-9">{{ job.source.file_type }}</dd>
                </dl>
                {% elif job.type == 'api_connector' and job.source %}
                <dl class="row">
                    <dt class="col-sm-3">API URL</dt>
                    <dd class="col-sm-9">{{ job.source.api_url }}</dd>
                    
                    <dt class="col-sm-3">Endpoint</dt>
                    <dd class="col-sm-9">{{ job.source.endpoint }}</dd>
                    
                    <dt class="col-sm-3">Method</dt>
                    <dd class="col-sm-9">{{ job.source.method }}</dd>
                </dl>
                {% elif job.type == 'batch_processor' and job.source %}
                <dl class="row">
                    <dt class="col-sm-3">Parallel Processing</dt>
                    <dd class="col-sm-9">{% if job.source.parallel %}Yes{% else %}No{% endif %}</dd>
                </dl>
                <h5 class="mt-3">Sources</h5>
                <ul class="list-group">
                    {% for source in job.source.sources %}
                    <li class="list-group-item">{{ source }}</li>
                    {% endfor %}
                </ul>
                {% else %}
                <p class="text-muted">No source details available.</p>
                {% endif %}
            </div>
        </div>

        <!-- Results -->
        <div class="card">
            <div class="card-header d-flex justify-content-between align-items-center">
                <span><i class="fas fa-chart-bar"></i> Results</span>
                {% if results %}
                <a href="{{ url_for('results.list', job_id=job.id) }}" class="btn btn-sm btn-outline-primary">
                    View All Results
                </a>
                {% endif %}
            </div>
            <div class="card-body">
                {% if results %}
                <div class="table-responsive">
                    <table class="table table-hover">
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Type</th>
                                <th>Items</th>
                                <th>Created</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for result in results %}
                            <tr>
                                <td>{{ result.id }}</td>
                                <td>{{ result.type }}</td>
                                <td>{{ result.data|length if result.data else 0 }}</td>
                                <td>{{ result.created_at }}</td>
                                <td>
                                    <a href="{{ url_for('results.view', result_id=result.id) }}" class="btn btn-sm btn-outline-primary">
                                        <i class="fas fa-eye"></i>
                                    </a>
                                </td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
                {% else %}
                <div class="text-center py-5">
                    <i class="fas fa-chart-pie text-muted fa-3x mb-3"></i>
                    <p class="lead">No results available for this job yet.</p>
                    {% if job.status == 'Queued' %}
                    <button type="button" class="btn btn-primary" onclick="submitForm('run-job')">
                        <i class="fas fa-play"></i> Run Job Now
                    </button>
                    {% endif %}
                </div>
                {% endif %}
            </div>
        </div>
    </div>
</div>
{% endblock %}

{% block extra_js %}
<script>
function submitForm(formId) {
    document.getElementById(formId).submit();
}

function confirmDelete() {
    if (confirm('Are you sure you want to delete this job?')) {
        document.getElementById('delete-job').submit();
    }
}

// Refresh job status if it's running
$(document).ready(function() {
    if ($('.job-status-running').length > 0) {
        setInterval(function() {
            const jobId = $('.job-status-running').data('job-id');
            $.ajax({
                url: `/jobs/api/status/${jobId}`,
                type: 'GET',
                dataType: 'json',
                success: function(response) {
                    if (response.success) {
                        // Update progress bar
                        $('.progress-bar').css('width', `${response.data.progress}%`);
                        $('.progress-bar').text(`${response.data.progress}%`);
                        
                        // Reload page if status changed
                        if (response.data.status.toLowerCase() !== 'running') {
                            location.reload();
                        }
                    }
                }
            });
        }, 5000); // Refresh every 5 seconds
    }
});
</script>
{% endblock %}
