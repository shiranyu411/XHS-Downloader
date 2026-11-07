"""Pure decisions for an opt-in, single-batch unattended supervisor.

This module never opens a browser, writes an archive, clears a restriction, or
stops a worker. The caller must check the writer lock and existing browser state
again before starting a worker. Counts are progress, never a goal-completion gate.
"""
from datetime import datetime


# A page can redirect without user intervention. Only an explicit pause is
# manual; page changes go through the same fresh-page recovery check.
MANUAL_REASONS = {'user_pause'}
STORAGE_REASONS = {'storage_error', 'archive_corrupt'}


def cooldown_until(controls):
    return max(float(controls.get(key) or 0) for key in
               ('pacing_cooldown_until', 'pacing_session_rest_until'))


def browser_resume_reason(states):
    """All relevant existing tabs must be free of a visible login/risk gate."""
    if not states:
        return 'no_platform_page'
    if any(state.get('blocked') for state in states):
        return 'manual_login_or_verification_required'
    if not all(state.get('logged_in') is True for state in states):
        return 'manual_login_or_verification_required'
    return ''


def retryable_task(task):
    if task.get('body') == 'unavailable':
        return False
    if task.get('status') in {'pending', 'running', 'interrupted', 'paused'}:
        return True
    return task.get('status') == 'finished' and any(
        task.get(key) != expected for key, expected in
        [('body', 'saved'), ('comments', 'complete_observed'),
         ('media', 'success'), ('comment_media', 'success')]
    )


def stalled_worker_decision(resume, controls, *, now, alive, progress_at, next_retry_at=None):
    """Restart only an opted-in, live worker with no meaningful progress.

    The caller verifies process identity and first requests a graceful stop.
    Planned waits and explicit cancellation must never trigger this watchdog.
    """
    if not alive or not resume.get('auto_restart_stalled') or resume.get('status') == 'cancelled_by_user':
        return None
    if cooldown_until(controls) > now or (next_retry_at or 0) > now:
        return None
    timeout = max(60, float(resume.get('stall_timeout_seconds') or 600))
    if now - progress_at < timeout:
        return None
    return {'action': 'restart', 'reason': 'no_meaningful_progress', 'idle_seconds': round(now - progress_at)}


def retry_decision(status, controls, resume, *, now, alive, active, free_gib,
                   pause_mtime=None, min_free_gib=1.0):
    """Return a decision without changing the persisted backoff or authorization.

    A worker with a live PID OR the archive writer lock is never replaced. User
    pause always wins over automatic retry, even if a platform cooldown expired.
    Browser connectivity/login/captcha are assessed by the caller only after the
    returned deadline. A transient backoff must be persisted once, not slid
    forward on every polling tick.
    """
    requested = float(resume.get('requested_at_epoch') or 0)
    if pause_mtime is not None and pause_mtime >= requested:
        return {'action': 'cancel', 'reason': 'user_pause'}
    if resume.get('status') == 'cancelled_by_user':
        return {'action': 'cancel', 'reason': 'user_pause'}
    if active or alive:
        return {'action': 'observe', 'reason': 'worker_active' if active else 'worker_starting_or_finalizing'}
    if free_gib < min_free_gib:
        return {'action': 'attention', 'reason': 'insufficient_disk_space'}
    if status.get('outcome') == 'complete':
        return {'action': 'audit', 'reason': 'observed_scope_complete_goal_audit_required'}

    tasks = status.get('tasks', [])
    searches = status.get('search', [])
    for task in tasks:
        if task.get('status') not in {'paused', 'interrupted', 'running'}:
            continue
        reason = task.get('reason')
        if reason in STORAGE_REASONS:
            return {'action': 'attention', 'reason': reason}
        if reason in MANUAL_REASONS:
            # An explicit newer resume can supersede an old user pause. It cannot
            # be inferred from the automatic launch timestamp.
            try:
                updated = datetime.fromisoformat(task['updated']).timestamp()
            except (KeyError, TypeError, ValueError):
                updated = float('inf')
            if updated >= requested:
                return {'action': 'attention', 'reason': reason}
    for search in searches:
        if search.get('status') not in {'paused', 'partial'}:
            continue
        if search.get('reason') in STORAGE_REASONS:
            return {'action': 'attention', 'reason': search['reason']}
        if search.get('reason') in MANUAL_REASONS:
            try:
                paused_at = datetime.fromisoformat(search['paused_at']).timestamp()
            except (KeyError, TypeError, ValueError):
                paused_at = float('inf')
            if paused_at >= requested:
                return {'action': 'attention', 'reason': search['reason']}

    work = any(retryable_task(t) for t in tasks) or any(s.get('status') != 'complete' for s in searches)
    if not work:
        return {'action': 'attention', 'reason': 'no_retryable_work_but_goal_incomplete'}

    restricted = bool(controls.get('workspace_blocked')) or status.get('access_restricted', False)
    until = cooldown_until(controls)
    if restricted:
        if not resume.get('auto_retry_access'):
            return {'action': 'attention', 'reason': 'access_restricted'}
        reason, deadline = 'persisted_access_cooldown', max(until, float(resume.get('recovery_probe_after') or 0))
    else:
        if not resume.get('auto_retry_transient'):
            return {'action': 'attention', 'reason': 'worker_not_running'}
        if until > now:
            reason, deadline = 'persisted_session_cooldown', until
        elif resume.get('status') == 'waiting' and 'not_before' in resume:
            reason = resume.get('retry_reason', 'scheduled_retry')
            deadline = max(until, float(resume['not_before']))
        else:
            reason = 'retry_incomplete_after_worker_exit'
            deadline = max(until, now + max(30.0, float(resume.get('wait_seconds') or 1800)))
    deadline = max(deadline, float(resume.get('recovery_probe_after') or 0))
    return {'action': 'wait' if now < deadline else 'check_browser',
            'reason': reason, 'not_before': deadline}
