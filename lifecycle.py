"""
Lifecycle-related messages for ROS2 DEVS simulation.
"""

from dataclasses import dataclass, field
from typing import List, Optional
from enum import IntEnum
import time

from message import Message, MessageType

class State(IntEnum):
    """Lifecycle states"""
    PRIMARY_STATE_UNKNOWN = 0
    PRIMARY_STATE_UNCONFIGURED = 1
    PRIMARY_STATE_INACTIVE = 2
    PRIMARY_STATE_ACTIVE = 3
    PRIMARY_STATE_FINALIZED = 4
    # Transition states
    TRANSITION_STATE_CONFIGURING = 10
    TRANSITION_STATE_CLEANINGUP = 11
    TRANSITION_STATE_SHUTTINGDOWN = 12
    TRANSITION_STATE_ACTIVATING = 13
    TRANSITION_STATE_DEACTIVATING = 14
    TRANSITION_STATE_ERRORPROCESSING = 15


class Transition(IntEnum):
    """Lifecycle transitions"""
    TRANSITION_CREATE = 0
    TRANSITION_CONFIGURE = 1
    TRANSITION_CLEANUP = 2
    TRANSITION_ACTIVATE = 3
    TRANSITION_DEACTIVATE = 4
    TRANSITION_UNCONFIGURED_SHUTDOWN = 5
    TRANSITION_INACTIVE_SHUTDOWN = 6
    TRANSITION_ACTIVE_SHUTDOWN = 7
    TRANSITION_DESTROY = 8
    TRANSITION_ON_CONFIGURE_SUCCESS = 40
    TRANSITION_ON_CONFIGURE_FAILURE = 41
    TRANSITION_ON_CONFIGURE_ERROR = 42
    TRANSITION_ON_CLEANUP_SUCCESS = 50
    TRANSITION_ON_CLEANUP_FAILURE = 51
    TRANSITION_ON_CLEANUP_ERROR = 52
    TRANSITION_ON_ACTIVATE_SUCCESS = 60
    TRANSITION_ON_ACTIVATE_FAILURE = 61
    TRANSITION_ON_ACTIVATE_ERROR = 62
    TRANSITION_ON_DEACTIVATE_SUCCESS = 70
    TRANSITION_ON_DEACTIVATE_FAILURE = 71
    TRANSITION_ON_DEACTIVATE_ERROR = 72
    TRANSITION_ON_SHUTDOWN_SUCCESS = 80
    TRANSITION_ON_SHUTDOWN_FAILURE = 81
    TRANSITION_ON_SHUTDOWN_ERROR = 82
    TRANSITION_ON_ERROR_SUCCESS = 90
    TRANSITION_ON_ERROR_FAILURE = 91
    TRANSITION_ON_ERROR_ERROR = 92


@dataclass
class LifecycleState(Message):
    """lifecycle_msgs/State message"""
    id: int = State.PRIMARY_STATE_UNKNOWN
    label: str = ""
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.DATA
        if not self.label:
            self.label = self._get_label_for_state(self.id)
            
    def _get_label_for_state(self, state_id: int) -> str:
        """Get human-readable label for state"""
        labels = {
            State.PRIMARY_STATE_UNKNOWN: "unknown",
            State.PRIMARY_STATE_UNCONFIGURED: "unconfigured",
            State.PRIMARY_STATE_INACTIVE: "inactive",
            State.PRIMARY_STATE_ACTIVE: "active",
            State.PRIMARY_STATE_FINALIZED: "finalized",
            State.TRANSITION_STATE_CONFIGURING: "configuring",
            State.TRANSITION_STATE_CLEANINGUP: "cleaningup",
            State.TRANSITION_STATE_SHUTTINGDOWN: "shuttingdown",
            State.TRANSITION_STATE_ACTIVATING: "activating",
            State.TRANSITION_STATE_DEACTIVATING: "deactivating",
            State.TRANSITION_STATE_ERRORPROCESSING: "errorprocessing"
        }
        return labels.get(state_id, "unknown")


@dataclass
class LifecycleTransition(Message):
    """lifecycle_msgs/Transition message"""
    id: int = Transition.TRANSITION_CREATE
    label: str = ""
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.DATA
        if not self.label:
            self.label = self._get_label_for_transition(self.id)
            
    def _get_label_for_transition(self, transition_id: int) -> str:
        """Get human-readable label for transition"""
        labels = {
            Transition.TRANSITION_CREATE: "create",
            Transition.TRANSITION_CONFIGURE: "configure",
            Transition.TRANSITION_CLEANUP: "cleanup",
            Transition.TRANSITION_ACTIVATE: "activate",
            Transition.TRANSITION_DEACTIVATE: "deactivate",
            Transition.TRANSITION_UNCONFIGURED_SHUTDOWN: "unconfigured_shutdown",
            Transition.TRANSITION_INACTIVE_SHUTDOWN: "inactive_shutdown",
            Transition.TRANSITION_ACTIVE_SHUTDOWN: "active_shutdown",
            Transition.TRANSITION_DESTROY: "destroy"
        }
        return labels.get(transition_id, f"transition_{transition_id}")


@dataclass
class TransitionDescription:
    """Describes a valid transition"""
    transition: LifecycleTransition = field(default_factory=LifecycleTransition)
    start_state: LifecycleState = field(default_factory=LifecycleState)
    goal_state: LifecycleState = field(default_factory=LifecycleState)


@dataclass
class TransitionEvent(Message):
    """lifecycle_msgs/TransitionEvent message"""
    timestamp: int = 0  # nanoseconds
    transition: LifecycleTransition = field(default_factory=LifecycleTransition)
    start_state: LifecycleState = field(default_factory=LifecycleState)
    goal_state: LifecycleState = field(default_factory=LifecycleState)
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.DATA
        if self.timestamp == 0:
            self.timestamp = int(time.time() * 1e9)


# Service message types for lifecycle management

@dataclass
class GetStateRequest(Message):
    """Request for GetState service"""
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.SERVICE_REQUEST


@dataclass
class GetStateResponse(Message):
    """Response for GetState service"""
    current_state: LifecycleState = field(default_factory=LifecycleState)
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.SERVICE_RESPONSE


@dataclass
class GetAvailableStatesRequest(Message):
    """Request for GetAvailableStates service"""
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.SERVICE_REQUEST


@dataclass
class GetAvailableStatesResponse(Message):
    """Response for GetAvailableStates service"""
    available_states: List[LifecycleState] = field(default_factory=list)
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.SERVICE_RESPONSE


@dataclass
class GetAvailableTransitionsRequest(Message):
    """Request for GetAvailableTransitions service"""
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.SERVICE_REQUEST


@dataclass
class GetAvailableTransitionsResponse(Message):
    """Response for GetAvailableTransitions service"""
    available_transitions: List[TransitionDescription] = field(default_factory=list)
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.SERVICE_RESPONSE


@dataclass
class ChangeStateRequest(Message):
    """Request for ChangeState service"""
    transition: LifecycleTransition = field(default_factory=LifecycleTransition)
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.SERVICE_REQUEST


@dataclass
class ChangeStateResponse(Message):
    """Response for ChangeState service"""
    success: bool = False
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.SERVICE_RESPONSE


# Lifecycle transition event for internal use
@dataclass
class LifecycleTransitionEvent(Message):
    """Internal lifecycle transition event"""
    transition: Transition = Transition.TRANSITION_CREATE
    requesting_node: str = ""
    target_node: str = ""
    timestamp: float = field(default_factory=time.time)
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.DATA