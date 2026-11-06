import pytest
from src.state_machine import WorkflowStateMachine, JobState, InvalidStateTransitionError


def test_valid_happy_path_transitions():
    """Tests the normal success lifecycle of a workflow."""
    sm = WorkflowStateMachine(initial_state=JobState.PENDING)
    assert sm.current_state == JobState.PENDING
    assert not sm.is_terminal()

    sm.transition_to(JobState.RUNNING)
    assert sm.current_state == JobState.RUNNING

    sm.transition_to(JobState.VALIDATING)
    assert sm.current_state == JobState.VALIDATING

    sm.transition_to(JobState.COMPLETED)
    assert sm.current_state == JobState.COMPLETED
    assert sm.is_terminal()


def test_illegal_transition_raises_error():
    """Tests that skipping steps or going backwards raises an error."""
    sm = WorkflowStateMachine(initial_state=JobState.PENDING)

    # Cannot skip straight to COMPLETED from PENDING
    with pytest.raises(InvalidStateTransitionError):
        sm.transition_to(JobState.COMPLETED)

    # Move to RUNNING
    sm.transition_to(JobState.RUNNING)

    # Cannot go backward to PENDING
    with pytest.raises(InvalidStateTransitionError):
        sm.transition_to(JobState.PENDING)


def test_completed_state_is_terminal():
    """Tests that a completed job cannot transition anywhere."""
    sm = WorkflowStateMachine(initial_state=JobState.PENDING)
    sm.transition_to(JobState.RUNNING)
    sm.transition_to(JobState.VALIDATING)
    sm.transition_to(JobState.COMPLETED)

    with pytest.raises(InvalidStateTransitionError):
        sm.transition_to(JobState.RUNNING)


def test_retry_and_dead_letter_escalation():
    """Tests that repeated failures increment retries and escalate to DEAD_LETTER."""
    sm = WorkflowStateMachine(initial_state=JobState.PENDING, max_retries=2)

    # First attempt fails
    sm.transition_to(JobState.RUNNING)
    state = sm.handle_failure()
    assert state == JobState.FAILED
    assert sm.retry_count == 1
    assert not sm.is_terminal()

    # Retry 1: RUNNING again, then fails
    sm.transition_to(JobState.RUNNING)
    state = sm.handle_failure()
    assert state == JobState.FAILED
    assert sm.retry_count == 2
    assert not sm.is_terminal()

    # Retry 2: RUNNING again, then fails (exceeds max_retries=2)
    sm.transition_to(JobState.RUNNING)
    state = sm.handle_failure()
    assert state == JobState.DEAD_LETTER
    assert sm.is_terminal()
