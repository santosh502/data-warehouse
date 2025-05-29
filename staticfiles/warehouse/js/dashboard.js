/**
 * Dashboard JavaScript for the Data Warehouse System
 * Handles charts, search, and dynamic data loading
 */

// Global variables
let ingestionChart = null;
let schemaChart = null;
let dashboardData = {};

/**
 * Initialize the dashboard
 */
function initializeDashboard() {
    console.log('Initializing dashboard...');
    
    // Load initial data
    loadDashboardStats();
    loadUserProfiles();
    
    // Set up search form
    setupSearchForm();
    
    // Set up auto-refresh
    setInterval(refreshDashboardStats, 30000); // Refresh every 30 seconds
}

/**
 * Load dashboard statistics and update charts
 */
async function loadDashboardStats() {
    try {
        const response = await fetch('/api/stats/');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        dashboardData = data;
        
        // Update statistics cards
        updateStatisticsCards(data);
        
        // Update charts
        updateIngestionChart(data.daily_ingestion || []);
        updateSchemaChart(data.schema_distribution || []);
        
        console.log('Dashboard stats loaded successfully');
    } catch (error) {
        console.error('Error loading dashboard stats:', error);
        showError('Failed to load dashboard statistics');
    }
}

/**
 * Update statistics cards with new data
 */
function updateStatisticsCards(data) {
    const overview = data.overview || {};
    
    document.getElementById('total-records').textContent = 
        formatNumber(overview.total_records || 0);
    document.getElementById('total-schemas').textContent = 
        formatNumber(overview.total_schemas || 0);
    document.getElementById('total-unstructured').textContent = 
        formatNumber(overview.total_unstructured || 0);
    document.getElementById('total-history').textContent = 
        formatNumber(overview.total_history || 0);
}

/**
 * Update the daily ingestion trend chart
 */
function updateIngestionChart(data) {
    const ctx = document.getElementById('ingestionChart').getContext('2d');
    
    // Destroy existing chart if it exists
    if (ingestionChart) {
        ingestionChart.destroy();
    }
    
    const labels = data.map(item => {
        const date = new Date(item.day);
        return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    });
    
    const values = data.map(item => item.count);
    
    ingestionChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Records Ingested',
                data: values,
                borderColor: '#0d6efd',
                backgroundColor: 'rgba(13, 110, 253, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: function(value) {
                            return formatNumber(value);
                        }
                    }
                }
            },
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return `${formatNumber(context.parsed.y)} records`;
                        }
                    }
                }
            }
        }
    });
}

/**
 * Update the schema distribution pie chart
 */
function updateSchemaChart(data) {
    const ctx = document.getElementById('schemaChart').getContext('2d');
    
    // Destroy existing chart if it exists
    if (schemaChart) {
        schemaChart.destroy();
    }
    
    const labels = data.slice(0, 5).map(item => item.schema__name || 'Unknown');
    const values = data.slice(0, 5).map(item => item.count);
    
    const colors = [
        '#0d6efd', '#198754', '#ffc107', '#dc3545', '#6f42c1'
    ];
    
    schemaChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: colors,
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        padding: 20,
                        usePointStyle: true
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const percentage = ((context.parsed / total) * 100).toFixed(1);
                            return `${context.label}: ${formatNumber(context.parsed)} (${percentage}%)`;
                        }
                    }
                }
            }
        }
    });
}

/**
 * Load and display user profiles
 */
async function loadUserProfiles() {
    try {
        const response = await fetch('/api/profiles/');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        displayUserProfiles(data.profiles || []);
        
    } catch (error) {
        console.error('Error loading user profiles:', error);
        document.getElementById('userProfiles').innerHTML = 
            '<div class="alert alert-warning">Failed to load user profiles</div>';
    }
}

/**
 * Display user profiles in the dashboard
 */
function displayUserProfiles(profiles) {
    const container = document.getElementById('userProfiles');
    
    if (profiles.length === 0) {
        container.innerHTML = '<div class="text-muted text-center py-3">No user profiles found</div>';
        return;
    }
    
    const html = profiles.slice(0, 3).map(profile => `
        <div class="profile-card">
            <div class="d-flex align-items-center">
                <div class="profile-avatar me-3">
                    ${profile.full_name.split(' ').map(n => n[0]).join('')}
                </div>
                <div class="flex-grow-1">
                    <div class="fw-bold">${escapeHtml(profile.full_name)}</div>
                    <div class="text-muted small">
                        Age: ${profile.age} | 
                        Addresses: ${profile.addresses.length} | 
                        Incomes: ${profile.incomes.length} |
                        Goals: ${profile.goals_count}
                    </div>
                    <div class="text-muted small">
                        Created: ${new Date(profile.created_at).toLocaleDateString()}
                    </div>
                </div>
            </div>
        </div>
    `).join('');
    
    container.innerHTML = html;
}

/**
 * Setup search form functionality
 */
function setupSearchForm() {
    const searchForm = document.getElementById('searchForm');
    const searchQuery = document.getElementById('searchQuery');
    
    searchForm.addEventListener('submit', function(e) {
        e.preventDefault();
        performSearch();
    });
    
    // Enable search on Enter key
    searchQuery.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            performSearch();
        }
    });
}

/**
 * Perform search operation
 */
async function performSearch() {
    const query = document.getElementById('searchQuery').value.trim();
    const dataType = document.getElementById('dataType').value;
    const schema = document.getElementById('schemaFilter').value;
    
    if (!query) {
        showError('Please enter a search query');
        return;
    }
    
    try {
        const params = new URLSearchParams({
            q: query,
            type: dataType,
            limit: '20'
        });
        
        if (schema) {
            params.append('schema', schema);
        }
        
        const response = await fetch(`/api/search/?${params}`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        displaySearchResults(data);
        
    } catch (error) {
        console.error('Search error:', error);
        showError('Search failed. Please try again.');
    }
}

/**
 * Display search results
 */
function displaySearchResults(data) {
    const resultsContainer = document.getElementById('searchResults');
    const resultsBody = document.getElementById('searchResultsBody');
    
    resultsContainer.classList.remove('d-none');
    
    const allResults = [
        ...data.results.structured.map(r => ({...r, type: 'Structured'})),
        ...data.results.unstructured.map(r => ({...r, type: 'Unstructured'}))
    ];
    
    if (allResults.length === 0) {
        resultsBody.innerHTML = `
            <tr>
                <td colspan="6" class="text-center text-muted py-3">
                    No results found for "${escapeHtml(data.query)}"
                </td>
            </tr>
        `;
        return;
    }
    
    const html = allResults.map(result => {
        const content = result.type === 'Structured' 
            ? JSON.stringify(result.data).substring(0, 100) + '...'
            : (result.content || '').substring(0, 100) + '...';
        
        const schemaOrTitle = result.type === 'Structured' 
            ? result.schema 
            : result.title || 'Untitled';
        
        return `
            <tr>
                <td>
                    <span class="badge ${result.type === 'Structured' ? 'bg-primary' : 'bg-info'}">
                        ${result.type}
                    </span>
                </td>
                <td class="font-monospace small">${result.id.substring(0, 8)}...</td>
                <td class="text-truncate" style="max-width: 200px;">
                    ${escapeHtml(content)}
                </td>
                <td>${escapeHtml(schemaOrTitle)}</td>
                <td class="small">${new Date(result.created_at).toLocaleDateString()}</td>
                <td>
                    <button class="btn btn-outline-primary btn-sm" 
                            onclick="viewDetails('${result.id}', '${result.type}')">
                        <i class="fas fa-eye"></i>
                    </button>
                </td>
            </tr>
        `;
    }).join('');
    
    resultsBody.innerHTML = html;
}

/**
 * View details for a specific record
 */
function viewDetails(id, type) {
    // This would typically open a modal or navigate to a detail page
    console.log(`Viewing details for ${type} record: ${id}`);
    showInfo(`Viewing ${type.toLowerCase()} record: ${id}`);
}

/**
 * Clear search results
 */
function clearSearch() {
    document.getElementById('searchQuery').value = '';
    document.getElementById('dataType').value = 'all';
    document.getElementById('schemaFilter').value = '';
    document.getElementById('searchResults').classList.add('d-none');
}

/**
 * Refresh dashboard data
 */
function refreshDashboard() {
    console.log('Refreshing dashboard...');
    loadDashboardStats();
    loadUserProfiles();
    showSuccess('Dashboard refreshed');
}

/**
 * Refresh only dashboard statistics
 */
function refreshDashboardStats() {
    loadDashboardStats();
}

/**
 * Utility function to format numbers
 */
function formatNumber(num) {
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
}

/**
 * Utility function to escape HTML
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Show success message
 */
function showSuccess(message) {
    showToast(message, 'success');
}

/**
 * Show error message
 */
function showError(message) {
    showToast(message, 'danger');
}

/**
 * Show info message
 */
function showInfo(message) {
    showToast(message, 'info');
}

/**
 * Show toast notification
 */
function showToast(message, type = 'info') {
    // Create toast element
    const toastId = 'toast-' + Date.now();
    const toastHtml = `
        <div id="${toastId}" class="toast align-items-center text-bg-${type} border-0" role="alert">
            <div class="d-flex">
                <div class="toast-body">
                    ${escapeHtml(message)}
                </div>
                <button type="button" class="btn-close btn-close-white me-2 m-auto" 
                        data-bs-dismiss="toast"></button>
            </div>
        </div>
    `;
    
    // Add to toast container or create one
    let toastContainer = document.querySelector('.toast-container');
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.className = 'toast-container position-fixed top-0 end-0 p-3';
        document.body.appendChild(toastContainer);
    }
    
    toastContainer.insertAdjacentHTML('beforeend', toastHtml);
    
    // Show toast
    const toastElement = document.getElementById(toastId);
    const toast = new bootstrap.Toast(toastElement, {
        autohide: true,
        delay: 5000
    });
    toast.show();
    
    // Remove toast element after it's hidden
    toastElement.addEventListener('hidden.bs.toast', function() {
        toastElement.remove();
    });
}

/**
 * Export functions for external use
 */
window.refreshDashboard = refreshDashboard;
window.clearSearch = clearSearch;
window.viewDetails = viewDetails;