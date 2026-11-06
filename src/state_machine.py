import enum
import logging
from typing import Dict, Set

logger = logging.getLogger(__name__)


class JobState(str, enum.Enum):
    """Deterministic states of an operational workflow task."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    VALIDATING = "VALIDATING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"


class InvalidStateTransitionError(Exception):
    """Raised when an illegal workflow state transition is attempted."""
    pass


class WorkflowStateMachine:
    """Enforces deterministic state transitions and retry policies for cloud operations.

    Allowed Transitions:
        PENDING     -> RUNNING, FAILED
        RUNNING     -> VALIDATING, FAILED
        VALIDATING  -> COMPLETED, FAILED
        FAILED      -> RUNNING (Retry), DEAD_LETTER (Max retries exceeded)
        COMPLETED   -> (Terminal state - no further transitions)
        DEAD_LETTER -> (Terminal state - manual intervention required)
    """

    ALLOWED_TRANSITIONS: Dict[JobState, Set[JobState]] = {
        JobState.PENDING: {JobState.RUNNING, JobState.FAILED},
        JobState.RUNNING: {JobState.VALIDATING, JobState.FAILED},
        JobState.VALIDATING: {JobState.COMPLETED, JobState.FAILED},
        JobState.FAILED: {JobState.RUNNING, JobState.DEAD_LETTER},
        JobState.COMPLETED: set(),
        JobState.DEAD_LETTER: set(),
    }

    def __init__(self, initial_state: JobState = JobState.PENDING, max_retries: int = 3):
        self.current_state = initial_state
        self.max_retries = max_retries
        self.retry_count = 0

    def can_transition_to(self, target_state: JobState) -> bool:
        """Checks whether moving from current_state to target_state is permissible."""
        return target_state in self.ALLOWED_TRANSITIONS.get(self.current_state, set())

    def transition_to(self, target_state: JobState) -> JobState:
        """Transitions the machine to target_state if allowed; raises error otherwise."""
        if not self.can_transition_to(target_state):
            raise InvalidStateTransitionError(
                f"Illegal state transition attempted: cannot move from {self.current_state.value} to {target_state.value}"
            )
        old_state = self.current_state
        self.current_state = target_state
        logger.info(f"State transition: {old_state.value} -> {self.current_state.value}")
        return self.current_state

    def handle_failure(self) -> JobState:
        """Handles a failure event by evaluating retry budget against max_retries."""
        self.retry_count += 1
        if self.retry_count <= self.max_retries:
            # Transition to FAILED, ready for retry
            self.transition_to(JobState.FAILED)
            return JobState.FAILED
        else:
            # Retry budget exhausted: move directly to DEAD_LETTER
            self.transition_to(JobState.FAILED)
            self.transition_to(JobState.DEAD_LETTER)
            logger.error(
                f"Max retries ({self.max_retries}) exceeded. Escalated to {JobState.DEAD_LETTER.value}"
            )
            return JobState.DEAD_LETTER

    def is_terminal(self) -> bool:
        """Returns True if the workflow has reached a terminal state (COMPLETED or DEAD_LETTER)."""
        return self.current_state in (JobState.COMPLETED, JobState.DEAD_LETTER)
