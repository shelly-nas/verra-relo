"""
Web server with Flask API and UI for managing the data fetching process.
"""
import logging
import threading
import time
from datetime import datetime, timedelta
from flask import Flask, render_template_string, jsonify, request
from utils import get_scheduler_state, save_scheduler_state, get_mailing_list, save_mailing_list

logger = logging.getLogger(__name__)

app = Flask(__name__)

# Load persisted scheduler state from config
_persisted_state = get_scheduler_state()

# Global state for the scheduler
scheduler_state = {
    'is_running': False,  # Always start stopped, will auto-start if was running
    'interval_days': _persisted_state.get('interval_days', 28),
    'selected_day': _persisted_state.get('selected_day', 1),
    'last_run': _persisted_state.get('last_run'),
    'next_run': _persisted_state.get('next_run'),
    'scheduler_thread': None,
    'stop_event': threading.Event(),
    '_was_running': _persisted_state.get('is_running', False)  # Track if should auto-start
}

# Global state for batch process
batch_state = {
    'is_running': False,
    'last_result': None,  # 'success' or 'error'
    'last_message': None,
    'started_at': None,
    'last_run_details': None  # Details about what was processed
}

# Global state for email notifications
email_state = {
    'last_sent': None,
    'last_subject': None,
    'last_summary': None,  # Summary of what was in the email
    'last_recipients': 0
}

# HTML template for the web UI - Notion style
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>IND Register Scheduler</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: #ffffff;
            min-height: 100vh;
            color: #37352f;
            line-height: 1.5;
            padding: 3rem 1.5rem;
        }
        
        .container {
            max-width: 680px;
            margin: 0 auto;
        }
        
        h1 {
            font-size: 2.25rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
            color: #37352f;
        }
        
        .subtitle {
            color: #787774;
            font-size: 1rem;
            margin-bottom: 2.5rem;
        }
        
        .card {
            background: #ffffff;
            border: 1px solid #e3e2e0;
            border-radius: 4px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
        }
        
        .card-title {
            font-size: 0.875rem;
            font-weight: 600;
            color: #37352f;
            margin-bottom: 1rem;
            text-transform: uppercase;
            letter-spacing: 0.03em;
        }
        
        .status-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 0.75rem 0;
            border-bottom: 1px solid #f1f1ef;
        }
        
        .status-row:last-child {
            border-bottom: none;
        }
        
        .status-label {
            color: #787774;
            font-size: 0.9375rem;
        }
        
        .status-value {
            font-size: 0.9375rem;
            color: #37352f;
        }
        
        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.375rem;
            padding: 0.25rem 0.625rem;
            border-radius: 3px;
            font-size: 0.8125rem;
            font-weight: 500;
        }
        
        .status-badge.running {
            background: #dff5e3;
            color: #0f7b0f;
        }
        
        .status-badge.stopped {
            background: #f1f1ef;
            color: #787774;
        }
        
        .status-dot {
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: currentColor;
        }
        
        .form-group {
            margin-bottom: 1.25rem;
        }
        
        .form-label {
            display: block;
            font-size: 0.875rem;
            color: #37352f;
            margin-bottom: 0.375rem;
            font-weight: 500;
        }
        
        .schedule-select {
            width: 100%;
            padding: 0.5rem 0.75rem;
            background: #ffffff;
            border: 1px solid #e3e2e0;
            border-radius: 4px;
            color: #37352f;
            font-family: inherit;
            font-size: 0.9375rem;
            cursor: pointer;
            transition: border-color 0.15s, box-shadow 0.15s;
        }
        
        .schedule-select:hover {
            border-color: #c4c4c1;
        }
        
        .schedule-select:focus {
            outline: none;
            border-color: #2eaadc;
            box-shadow: 0 0 0 3px rgba(46, 170, 220, 0.15);
        }
        
        .day-picker {
            display: none;
            gap: 0.375rem;
            flex-wrap: wrap;
            margin-top: 0.75rem;
        }
        
        .day-picker.visible {
            display: flex;
        }
        
        .day-btn {
            padding: 0.375rem 0.75rem;
            background: #ffffff;
            border: 1px solid #e3e2e0;
            border-radius: 4px;
            color: #37352f;
            font-family: inherit;
            font-size: 0.8125rem;
            cursor: pointer;
            transition: all 0.15s;
        }
        
        .day-btn:hover {
            background: #f7f6f5;
            border-color: #c4c4c1;
        }
        
        .day-btn.selected {
            background: #37352f;
            border-color: #37352f;
            color: #ffffff;
        }
        
        .schedule-summary {
            margin-top: 1rem;
            padding: 0.75rem 1rem;
            background: #f7f6f5;
            border-radius: 4px;
            color: #787774;
            font-size: 0.875rem;
        }
        
        .button-group {
            display: flex;
            gap: 0.75rem;
            margin-top: 1.5rem;
        }
        
        .btn {
            padding: 0.5rem 1rem;
            border: none;
            border-radius: 4px;
            font-family: inherit;
            font-size: 0.875rem;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.15s;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 0.375rem;
        }
        
        .btn-primary {
            background: #2eaadc;
            color: #ffffff;
        }
        
        .btn-primary:hover {
            background: #2496c4;
        }
        
        .btn-primary:disabled {
            background: #c4c4c1;
            cursor: not-allowed;
        }
        
        .btn-secondary {
            background: #f7f6f5;
            color: #37352f;
            border: 1px solid #e3e2e0;
        }
        
        .btn-secondary:hover {
            background: #eeeeec;
        }
        
        .btn-danger {
            background: #eb5757;
            color: #ffffff;
        }
        
        .btn-danger:hover {
            background: #d94848;
        }
        
        .spinner {
            width: 14px;
            height: 14px;
            border: 2px solid transparent;
            border-top-color: currentColor;
            border-radius: 50%;
            animation: spin 0.6s linear infinite;
        }
        
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
        
        .message {
            margin-top: 1rem;
            padding: 0.75rem 1rem;
            border-radius: 4px;
            font-size: 0.875rem;
            display: none;
        }
        
        .message.success {
            display: block;
            background: #dff5e3;
            color: #0f7b0f;
        }
        
        .message.error {
            display: block;
            background: #fde8e8;
            color: #c53030;
        }
        
        .message.info {
            display: block;
            background: #e8f4fd;
            color: #2563eb;
        }
        
        @media (max-width: 600px) {
            body {
                padding: 2rem 1rem;
            }
            
            h1 {
                font-size: 1.75rem;
            }
            
            .button-group {
                flex-direction: column;
            }
            
            .btn {
                width: 100%;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>IND Register Scheduler</h1>
        <p class="subtitle">Automated data collection from the IND public register</p>
        
        <div class="card">
            <div class="card-title">Status</div>
            <div class="status-row">
                <span class="status-label">Scheduler</span>
                <span class="status-badge stopped" id="scheduler-status">
                    <span class="status-dot"></span>
                    Stopped
                </span>
            </div>
            <div class="status-row">
                <span class="status-label">Last run</span>
                <span class="status-value" id="last-run">Never</span>
            </div>
            <div class="status-row">
                <span class="status-label">Last run result</span>
                <span class="status-value" id="last-run-result">-</span>
            </div>
            <div class="status-row">
                <span class="status-label">Last run details</span>
                <span class="status-value" id="last-run-details" style="font-size: 0.8rem; color: #787774;">-</span>
            </div>
            <div class="status-row">
                <span class="status-label">Next run</span>
                <span class="status-value" id="next-run">-</span>
            </div>
        </div>
        
        <div class="card">
            <div class="card-title">Schedule</div>
            
            <div class="form-group">
                <label class="form-label" for="schedule-type">Frequency</label>
                <select class="schedule-select" id="schedule-type">
                    <option value="daily">Daily</option>
                    <option value="weekly">Weekly</option>
                    <option value="biweekly">Every 2 weeks</option>
                    <option value="monthly" selected>Every 4 weeks</option>
                </select>
            </div>
            
            <div class="form-group">
                <label class="form-label">Day</label>
                <div class="day-picker visible" id="day-picker">
                    <button type="button" class="day-btn" data-day="0">Sun</button>
                    <button type="button" class="day-btn selected" data-day="1">Mon</button>
                    <button type="button" class="day-btn" data-day="2">Tue</button>
                    <button type="button" class="day-btn" data-day="3">Wed</button>
                    <button type="button" class="day-btn" data-day="4">Thu</button>
                    <button type="button" class="day-btn" data-day="5">Fri</button>
                    <button type="button" class="day-btn" data-day="6">Sat</button>
                </div>
            </div>
            
            <div class="schedule-summary" id="schedule-summary">
                Runs every 4th Monday at 00:00
            </div>
            
            <div class="button-group">
                <button class="btn btn-primary" id="run-now-btn" onclick="runNow()">Run Now</button>
                <button class="btn btn-secondary" id="toggle-scheduler-btn" onclick="toggleScheduler()">Start Scheduler</button>
            </div>
            
            <div class="message" id="message"></div>
        </div>
        
        <div class="card">
            <div class="card-title">Email Notifications</div>
            
            <div class="form-group">
                <label class="form-label" for="mailing-list">Mailing List</label>
                <textarea 
                    id="mailing-list" 
                    class="schedule-select" 
                    style="min-height: 80px; resize: vertical; font-size: 0.875rem;"
                    placeholder="Enter email addresses, one per line or comma-separated"
                ></textarea>
                <p style="color: #787774; font-size: 0.75rem; margin-top: 0.375rem;">
                    Enter email addresses separated by commas or new lines
                </p>
            </div>
            
            <div class="button-group" style="margin-top: 0.75rem; margin-bottom: 1.25rem;">
                <button class="btn btn-primary" id="save-mailing-list-btn" onclick="saveMailingList()">Save Mailing List</button>
            </div>
            
            <div class="message" id="mailing-list-message"></div>
            
            <div style="border-top: 1px solid #e3e2e0; margin: 1.25rem 0; padding-top: 1.25rem;">
                <div class="status-row">
                    <span class="status-label">Last email sent</span>
                    <span class="status-value" id="last-email-sent">Never</span>
                </div>
                <div class="status-row">
                    <span class="status-label">Subject</span>
                    <span class="status-value" id="last-email-subject" style="font-size: 0.8rem;">-</span>
                </div>
                <div class="status-row">
                    <span class="status-label">Summary</span>
                    <span class="status-value" id="last-email-summary" style="font-size: 0.8rem; color: #787774;">-</span>
                </div>
                <div class="status-row">
                    <span class="status-label">Recipients</span>
                    <span class="status-value" id="last-email-recipients">-</span>
                </div>
            </div>
            
            <p style="color: #787774; font-size: 0.875rem; margin: 1rem 0;">Send a test email to verify your SMTP configuration and mailing list.</p>
            <button class="btn btn-secondary" id="test-email-btn" onclick="sendTestEmail()">Send Test Email</button>
            <div class="message" id="email-message"></div>
        </div>
    </div>
    
    <script>
        const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
        let selectedDay = 1;
        const messageTimeouts = {};
        
        function showMessage(elementId, text, type) {
            const messageEl = document.getElementById(elementId);
            if (!messageEl) return;
            
            messageEl.textContent = text;
            messageEl.className = 'message ' + type;
            
            // Clear any existing timeout for this element
            if (messageTimeouts[elementId]) {
                clearTimeout(messageTimeouts[elementId]);
            }
            
            // Auto-remove after 10 seconds
            messageTimeouts[elementId] = setTimeout(() => {
                messageEl.className = 'message';
                messageEl.textContent = '';
            }, 10000);
        }
        
        document.addEventListener('DOMContentLoaded', () => {
            updateStatus();
            setupDayPicker();
            updateScheduleUI();
            loadMailingList();
        });
        
        function setupDayPicker() {
            const dayPicker = document.getElementById('day-picker');
            dayPicker.querySelectorAll('.day-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    dayPicker.querySelectorAll('.day-btn').forEach(b => b.classList.remove('selected'));
                    btn.classList.add('selected');
                    selectedDay = parseInt(btn.dataset.day);
                    updateScheduleSummary();
                    saveSchedule();
                });
            });
            
            document.getElementById('schedule-type').addEventListener('change', () => {
                updateScheduleUI();
                saveSchedule();
            });
        }
        
        function selectDay(day) {
            selectedDay = day;
            const dayPicker = document.getElementById('day-picker');
            dayPicker.querySelectorAll('.day-btn').forEach(btn => {
                btn.classList.toggle('selected', parseInt(btn.dataset.day) === day);
            });
            updateScheduleSummary();
            saveSchedule();
        }
        
        function updateScheduleUI() {
            const scheduleType = document.getElementById('schedule-type').value;
            const dayPicker = document.getElementById('day-picker');
            
            if (scheduleType === 'daily') {
                dayPicker.classList.remove('visible');
            } else {
                dayPicker.classList.add('visible');
            }
            
            updateScheduleSummary();
        }
        
        function updateScheduleSummary() {
            const scheduleType = document.getElementById('schedule-type').value;
            const summaryEl = document.getElementById('schedule-summary');
            const dayName = dayNames[selectedDay];
            
            let summary = '';
            switch (scheduleType) {
                case 'daily':
                    summary = 'Runs every day at 00:00';
                    break;
                case 'weekly':
                    summary = `Runs every ${dayName} at 00:00`;
                    break;
                case 'biweekly':
                    summary = `Runs every other ${dayName} at 00:00`;
                    break;
                case 'monthly':
                    summary = `Runs every 4th ${dayName} at 00:00`;
                    break;
            }
            summaryEl.textContent = summary;
        }
        
        function getIntervalDays() {
            const scheduleType = document.getElementById('schedule-type').value;
            switch (scheduleType) {
                case 'daily': return 1;
                case 'weekly': return 7;
                case 'biweekly': return 14;
                case 'monthly': return 28;
                default: return 7;
            }
        }
        
        function setScheduleFromDays(days) {
            const scheduleSelect = document.getElementById('schedule-type');
            if (days <= 1) {
                scheduleSelect.value = 'daily';
            } else if (days <= 7) {
                scheduleSelect.value = 'weekly';
            } else if (days <= 14) {
                scheduleSelect.value = 'biweekly';
            } else {
                scheduleSelect.value = 'monthly';
            }
            updateScheduleUI();
        }
        
        async function updateStatus() {
            try {
                const response = await fetch('/api/status');
                const data = await response.json();
                
                const statusEl = document.getElementById('scheduler-status');
                const lastRunEl = document.getElementById('last-run');
                const lastRunResultEl = document.getElementById('last-run-result');
                const lastRunDetailsEl = document.getElementById('last-run-details');
                const nextRunEl = document.getElementById('next-run');
                const toggleBtn = document.getElementById('toggle-scheduler-btn');
                
                // Email status elements
                const lastEmailSentEl = document.getElementById('last-email-sent');
                const lastEmailSubjectEl = document.getElementById('last-email-subject');
                const lastEmailSummaryEl = document.getElementById('last-email-summary');
                const lastEmailRecipientsEl = document.getElementById('last-email-recipients');
                
                if (data.is_running) {
                    statusEl.innerHTML = '<span class="status-dot"></span>Running';
                    statusEl.className = 'status-badge running';
                    toggleBtn.textContent = 'Stop Scheduler';
                    toggleBtn.className = 'btn btn-danger';
                } else {
                    statusEl.innerHTML = '<span class="status-dot"></span>Stopped';
                    statusEl.className = 'status-badge stopped';
                    toggleBtn.textContent = 'Start Scheduler';
                    toggleBtn.className = 'btn btn-secondary';
                }
                
                lastRunEl.textContent = data.last_run || 'Never';
                nextRunEl.textContent = data.next_run || '-';
                
                // Last run result
                if (data.batch_result === 'success') {
                    lastRunResultEl.textContent = '✓ Success';
                    lastRunResultEl.style.color = '#0f7b0f';
                } else if (data.batch_result === 'error') {
                    lastRunResultEl.textContent = '✗ Error';
                    lastRunResultEl.style.color = '#c53030';
                } else {
                    lastRunResultEl.textContent = '-';
                    lastRunResultEl.style.color = '#37352f';
                }
                
                // Last run details
                lastRunDetailsEl.textContent = data.last_run_details || '-';
                
                // Email status
                lastEmailSentEl.textContent = data.email_last_sent || 'Never';
                lastEmailSubjectEl.textContent = data.email_last_subject || '-';
                lastEmailSummaryEl.textContent = data.email_last_summary || '-';
                lastEmailRecipientsEl.textContent = data.email_last_recipients ? `${data.email_last_recipients} recipient(s)` : '-';
                
                // Set schedule from server state
                if (data.interval_days) {
                    setScheduleFromDays(data.interval_days);
                }
                if (data.selected_day !== undefined) {
                    selectedDay = data.selected_day;
                    const dayPicker = document.getElementById('day-picker');
                    dayPicker.querySelectorAll('.day-btn').forEach(btn => {
                        btn.classList.toggle('selected', parseInt(btn.dataset.day) === selectedDay);
                    });
                }
                updateScheduleSummary();
            } catch (error) {
                console.error('Failed to fetch status:', error);
            }
        }
        
        async function runNow() {
            const btn = document.getElementById('run-now-btn');
            const messageEl = document.getElementById('message');
            
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner"></span>Running...';
            showMessage('message', 'Batch process started...', 'info');
            
            try {
                // Start the batch process
                await fetch('/api/run', { method: 'POST' });
                
                // Poll for completion
                await pollBatchStatus(btn, messageEl);
            } catch (error) {
                showMessage('message', 'Failed to start batch process', 'error');
                btn.disabled = false;
                btn.innerHTML = 'Run Now';
            }
        }
        
        async function pollBatchStatus(btn, messageEl) {
            const pollInterval = 2000; // 2 seconds
            const maxPolls = 300; // 10 minutes max
            let polls = 0;
            
            const poll = async () => {
                try {
                    const response = await fetch('/api/status');
                    const data = await response.json();
                    
                    if (data.batch_running) {
                        // Still running, continue polling
                        polls++;
                        if (polls < maxPolls) {
                            setTimeout(poll, pollInterval);
                        } else {
                            showMessage('message', 'Batch process is taking longer than expected. Check logs for status.', 'info');
                            btn.disabled = false;
                            btn.innerHTML = 'Run Now';
                        }
                    } else {
                        // Batch finished
                        if (data.batch_result === 'success') {
                            showMessage('message', data.batch_message || 'Batch process completed successfully', 'success');
                        } else if (data.batch_result === 'error') {
                            showMessage('message', data.batch_message || 'Batch process failed', 'error');
                        }
                        btn.disabled = false;
                        btn.innerHTML = 'Run Now';
                        updateStatus();
                    }
                } catch (error) {
                    console.error('Failed to poll status:', error);
                    btn.disabled = false;
                    btn.innerHTML = 'Run Now';
                }
            };
            
            // Start polling after a short delay
            setTimeout(poll, pollInterval);
        }
        
        async function toggleScheduler() {
            const interval = getIntervalDays();
            
            try {
                const response = await fetch('/api/scheduler/toggle', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ interval_days: interval, selected_day: selectedDay })
                });
                const data = await response.json();
                
                showMessage('message', data.message, data.success ? 'success' : 'error');
                updateStatus();
            } catch (error) {
                showMessage('message', 'Failed to toggle scheduler', 'error');
            }
        }
        
        async function saveSchedule() {
            const interval = getIntervalDays();
            try {
                await fetch('/api/scheduler/interval', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ interval_days: interval, selected_day: selectedDay })
                });
            } catch (error) {
                console.error('Failed to save schedule:', error);
            }
        }
        
        async function sendTestEmail() {
            const btn = document.getElementById('test-email-btn');
            
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner"></span>Sending...';
            
            try {
                const response = await fetch('/api/email/test', { method: 'POST' });
                const data = await response.json();
                
                showMessage('email-message', data.message, data.success ? 'success' : 'error');
            } catch (error) {
                showMessage('email-message', 'Failed to send test email', 'error');
            } finally {
                btn.disabled = false;
                btn.innerHTML = 'Send Test Email';
            }
        }
        
        async function loadMailingList() {
            try {
                const response = await fetch('/api/mailing-list');
                const data = await response.json();
                
                const textarea = document.getElementById('mailing-list');
                if (data.emails && data.emails.length > 0) {
                    textarea.value = data.emails.join('\\n');
                } else {
                    textarea.value = '';
                }
            } catch (error) {
                console.error('Failed to load mailing list:', error);
            }
        }
        
        async function saveMailingList() {
            const btn = document.getElementById('save-mailing-list-btn');
            const textarea = document.getElementById('mailing-list');
            
            // Parse emails from textarea (split by newlines and commas)
            const text = textarea.value;
            const emails = text
                .split(/[,\\n]+/)
                .map(e => e.trim())
                .filter(e => e.length > 0);
            
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner"></span>Saving...';
            
            try {
                const response = await fetch('/api/mailing-list', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ emails: emails })
                });
                const data = await response.json();
                
                showMessage('mailing-list-message', data.message, data.success ? 'success' : 'error');
                
                // Reload to show cleaned up list
                if (data.success) {
                    await loadMailingList();
                }
            } catch (error) {
                showMessage('mailing-list-message', 'Failed to save mailing list', 'error');
            } finally {
                btn.disabled = false;
                btn.innerHTML = 'Save Mailing List';
            }
        }
    </script>
</body>
</html>
"""


def run_batch_process():
    """Run the main batch process."""
    from main import main as batch_main, get_last_run_info
    
    batch_state['is_running'] = True
    batch_state['started_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    batch_state['last_result'] = None
    batch_state['last_message'] = None
    batch_state['last_run_details'] = None
    
    try:
        logger.info("=" * 60)
        logger.info("BATCH PROCESS STARTED")
        logger.info("=" * 60)
        batch_main()
        scheduler_state['last_run'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        batch_state['last_result'] = 'success'
        batch_state['last_message'] = 'Batch process completed successfully'
        
        # Persist scheduler state with updated last_run
        save_scheduler_state(scheduler_state)
        
        # Get details from main module
        run_info = get_last_run_info()
        batch_state['last_run_details'] = run_info.get('details')
        
        # Update email state if email was sent
        if run_info.get('email_sent'):
            email_state['last_sent'] = run_info.get('email_sent_at')
            email_state['last_subject'] = run_info.get('email_subject')
            email_state['last_summary'] = run_info.get('email_summary')
            email_state['last_recipients'] = run_info.get('email_recipients', 0)
        
        logger.info("=" * 60)
        logger.info("BATCH PROCESS COMPLETED")
        logger.info("=" * 60)
        return True, "Batch process completed successfully"
    except Exception as e:
        logger.error(f"BATCH PROCESS FAILED: {e}")
        batch_state['last_result'] = 'error'
        batch_state['last_message'] = f'Batch process failed: {str(e)}'
        return False, f"Batch process failed: {str(e)}"
    finally:
        batch_state['is_running'] = False


def get_next_midnight_on_day(selected_day: int) -> datetime:
    """
    Get the next occurrence of 00:00:00 on the specified day of week.
    
    Args:
        selected_day: Day of week (0=Sunday, 1=Monday, ..., 6=Saturday)
    
    Returns:
        datetime: Next occurrence at 00:00:00
    """
    now = datetime.now()
    
    # Convert our format (0=Sunday) to Python's (0=Monday, 6=Sunday)
    target_weekday = (selected_day - 1) % 7 if selected_day > 0 else 6
    
    # Start checking from tomorrow to avoid same-day re-runs
    next_candidate = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Find the next occurrence of target weekday
    while next_candidate.weekday() != target_weekday:
        next_candidate += timedelta(days=1)
    
    return next_candidate


def scheduler_loop():
    """Background scheduler loop."""
    while not scheduler_state['stop_event'].is_set():
        # Get next scheduled run at 00:00:00 on the target day
        next_run = get_next_midnight_on_day(scheduler_state['selected_day'])
        
        # For bi-weekly or monthly, add extra weeks
        interval_days = scheduler_state['interval_days']
        if interval_days > 7:
            weeks_to_add = (interval_days // 7) - 1
            next_run += timedelta(weeks=weeks_to_add)
        
        scheduler_state['next_run'] = next_run.strftime('%Y-%m-%d %H:%M:%S')
        
        # Wait until that exact time
        seconds_until_run = (next_run - datetime.now()).total_seconds()
        logger.info(f"Next scheduled run: {next_run.strftime('%Y-%m-%d %H:%M:%S')} (in {seconds_until_run/3600:.1f} hours)")
        
        if scheduler_state['stop_event'].wait(timeout=seconds_until_run):
            break  # Scheduler was stopped
        
        # Time to run
        if not scheduler_state['stop_event'].is_set():
            logger.info("Triggering scheduled batch process")
            run_batch_process()


@app.route('/')
def index():
    """Serve the main UI page."""
    return render_template_string(HTML_TEMPLATE)


@app.route('/api/status')
def get_status():
    """Get the current scheduler status."""
    return jsonify({
        'is_running': scheduler_state['is_running'],
        'interval_days': scheduler_state['interval_days'],
        'selected_day': scheduler_state['selected_day'],
        'last_run': scheduler_state['last_run'],
        'next_run': scheduler_state['next_run'] if scheduler_state['is_running'] else None,
        'batch_running': batch_state['is_running'],
        'batch_result': batch_state['last_result'],
        'batch_message': batch_state['last_message'],
        'last_run_details': batch_state['last_run_details'],
        'email_last_sent': email_state['last_sent'],
        'email_last_subject': email_state['last_subject'],
        'email_last_summary': email_state['last_summary'],
        'email_last_recipients': email_state['last_recipients']
    })


@app.route('/api/run', methods=['POST'])
def run_now():
    """Trigger an immediate batch run."""
    # Run in a separate thread to not block the response
    def run_async():
        run_batch_process()
    
    thread = threading.Thread(target=run_async)
    thread.start()
    
    return jsonify({
        'success': True,
        'message': 'Batch process started. Check logs for progress.'
    })


@app.route('/api/scheduler/toggle', methods=['POST'])
def toggle_scheduler():
    """Start or stop the scheduler."""
    data = request.get_json() or {}
    interval = data.get('interval_days', scheduler_state['interval_days'])
    selected_day = data.get('selected_day', scheduler_state['selected_day'])
    
    if scheduler_state['is_running']:
        # Stop the scheduler
        scheduler_state['stop_event'].set()
        if scheduler_state['scheduler_thread']:
            scheduler_state['scheduler_thread'].join(timeout=5)
        scheduler_state['is_running'] = False
        scheduler_state['next_run'] = None
        scheduler_state['stop_event'].clear()
        
        # Persist state
        save_scheduler_state(scheduler_state)
        
        logger.info("Scheduler STOPPED")
        
        return jsonify({
            'success': True,
            'message': 'Scheduler stopped'
        })
    else:
        # Start the scheduler
        scheduler_state['interval_days'] = interval
        scheduler_state['selected_day'] = selected_day
        scheduler_state['is_running'] = True
        scheduler_state['stop_event'].clear()
        
        thread = threading.Thread(target=scheduler_loop, daemon=True)
        scheduler_state['scheduler_thread'] = thread
        thread.start()
        
        # Persist state
        save_scheduler_state(scheduler_state)
        
        # Format interval for message
        day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
        if interval <= 1:
            schedule_desc = 'daily at 00:00'
        elif interval <= 7:
            schedule_desc = f'every {day_names[selected_day]} at 00:00'
        elif interval <= 14:
            schedule_desc = f'every other {day_names[selected_day]} at 00:00'
        else:
            schedule_desc = f'every 4th {day_names[selected_day]} at 00:00'
        
        logger.info(f"Scheduler STARTED - Schedule: {schedule_desc} (interval: {interval} days, day: {day_names[selected_day]})")
        
        return jsonify({
            'success': True,
            'message': f'Scheduler started: runs {schedule_desc}'
        })


@app.route('/api/scheduler/interval', methods=['POST'])
def set_interval():
    """Update the scheduler interval and selected day."""
    data = request.get_json() or {}
    interval = data.get('interval_days', 28)
    selected_day = data.get('selected_day', scheduler_state['selected_day'])
    
    old_interval = scheduler_state['interval_days']
    old_day = scheduler_state['selected_day']
    
    scheduler_state['interval_days'] = interval
    scheduler_state['selected_day'] = selected_day
    
    # Persist state
    save_scheduler_state(scheduler_state)
    
    day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    if interval <= 1:
        schedule_desc = 'daily at 00:00'
    elif interval <= 7:
        schedule_desc = f'every {day_names[selected_day]} at 00:00'
    elif interval <= 14:
        schedule_desc = f'every other {day_names[selected_day]} at 00:00'
    else:
        schedule_desc = f'every 4th {day_names[selected_day]} at 00:00'
    
    logger.info(f"Schedule CHANGED - New: {schedule_desc} (interval: {interval} days, day: {day_names[selected_day]}) | Previous: {old_interval} days, {day_names[old_day]}")
    
    return jsonify({
        'success': True,
        'message': 'Schedule updated'
    })


@app.route('/api/email/test', methods=['POST'])
def send_test_email():
    """Send a test email to verify SMTP configuration."""
    from email_notifier import EmailNotifier
    
    notifier = EmailNotifier()
    
    if not notifier.enabled:
        return jsonify({
            'success': False,
            'message': 'Email notifications not configured. Set SMTP_USERNAME and SMTP_PASSWORD in .env file, and add recipients to the mailing list above.'
        })
    
    # Use the notifier's built-in test email method which includes CSV attachment
    success = notifier.send_test_email()
    
    if success:
        return jsonify({
            'success': True,
            'message': f'Test email sent successfully to {len(notifier.mailing_list)} recipient(s)'
        })
    else:
        return jsonify({
            'success': False,
            'message': 'Failed to send test email. Check server logs for details.'
        })


@app.route('/api/mailing-list', methods=['GET'])
def get_mailing_list_api():
    """Get the current mailing list."""
    emails = get_mailing_list()
    return jsonify({
        'success': True,
        'emails': emails
    })


@app.route('/api/mailing-list', methods=['POST'])
def set_mailing_list_api():
    """Update the mailing list."""
    data = request.get_json() or {}
    emails = data.get('emails', [])
    
    # Validate that it's a list
    if not isinstance(emails, list):
        return jsonify({
            'success': False,
            'message': 'Invalid format: emails must be a list'
        })
    
    # Basic email validation
    valid_emails = []
    for email in emails:
        email = str(email).strip()
        if email and '@' in email:
            valid_emails.append(email)
    
    old_list = get_mailing_list()
    save_mailing_list(valid_emails)
    
    logger.info(f"Mailing list UPDATED - Recipients: {len(valid_emails)} ({', '.join(valid_emails)}) | Previous: {len(old_list)}")
    
    return jsonify({
        'success': True,
        'message': f'Mailing list saved with {len(valid_emails)} recipient(s)'
    })


def start_server(host='0.0.0.0', port=8080, debug=False):
    """Start the Flask web server."""
    # Auto-start scheduler if it was running before
    if scheduler_state.get('_was_running', False):
        logger.info("Starting scheduler (auto-start from previous state)")
        scheduler_state['is_running'] = True
        scheduler_state['stop_event'].clear()
        thread = threading.Thread(target=scheduler_loop, daemon=True)
        scheduler_state['scheduler_thread'] = thread
        thread.start()
    else:
        logger.info("Scheduler is stopped (manual start required)")
    
    logger.info(f"Starting web server on {host}:{port}")
    app.run(host=host, port=port, debug=debug, threaded=True)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s: %(message)s'
    )
    start_server(debug=False)
