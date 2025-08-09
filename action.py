"""
Action-related messages for ROS2 DEVS simulation.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, List
import uuid
import time

from message import MessageType

class GoalStatus(Enum):
    """Action goal status"""
    STATUS_UNKNOWN = 0
    STATUS_ACCEPTED = 1
    STATUS_EXECUTING = 2
    STATUS_CANCELING = 3
    STATUS_SUCCEEDED = 4
    STATUS_CANCELED = 5
    STATUS_ABORTED = 6


@dataclass
class ActionGoal:
    """Action goal message"""
    goal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    action_type: str = ""
    goal_data: dict = field(default_factory=dict)
    client_node: str = ""
    type: MessageType = MessageType.ACTION_GOAL
    timestamp: float = field(default_factory=time.time)


@dataclass
class ActionGoalStatus:
    """Action goal status message"""
    goal_id: str = ""
    status: GoalStatus = GoalStatus.STATUS_UNKNOWN
    type: MessageType = MessageType.DATA


@dataclass
class ActionFeedback:
    """Action feedback message"""
    goal_id: str = ""
    action_type: str = ""
    feedback_data: dict = field(default_factory=dict)
    progress_percent: float = 0.0
    type: MessageType = MessageType.ACTION_FEEDBACK
    timestamp: float = field(default_factory=time.time)


@dataclass
class ActionResult:
    """Action result message"""
    goal_id: str = ""
    action_type: str = ""
    status: GoalStatus = GoalStatus.STATUS_UNKNOWN
    result_data: dict = field(default_factory=dict)
    error_message: str = ""
    type: MessageType = MessageType.ACTION_RESULT
    timestamp: float = field(default_factory=time.time)


@dataclass
class CancelGoalRequest:
    """Request to cancel an action goal"""
    goal_id: str = ""
    type: MessageType = MessageType.SERVICE_REQUEST


@dataclass
class CancelGoalResponse:
    """Response to cancel goal request"""
    goals_canceling: list = field(default_factory=list)
    type: MessageType = MessageType.SERVICE_RESPONSE


# Example action type definitions

@dataclass
class NavigateToPoseGoal:
    """Navigate to pose action goal"""
    goal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    action_type: str = "NavigateToPose"
    pose_x: float = 0.0
    pose_y: float = 0.0
    pose_theta: float = 0.0
    max_velocity: float = 1.0
    type: MessageType = MessageType.ACTION_GOAL
    timestamp: float = field(default_factory=time.time)
    
    def __post_init__(self):
        self.goal_data = {
            "pose": {"x": self.pose_x, "y": self.pose_y, "theta": self.pose_theta},
            "max_velocity": self.max_velocity
        }


@dataclass
class NavigateToPoseFeedback:
    """Navigate to pose action feedback"""
    goal_id: str = ""
    action_type: str = "NavigateToPose"
    current_pose_x: float = 0.0
    current_pose_y: float = 0.0
    distance_remaining: float = 0.0
    estimated_time_remaining: float = 0.0
    progress_percent: float = 0.0
    type: MessageType = MessageType.ACTION_FEEDBACK
    timestamp: float = field(default_factory=time.time)
    
    def __post_init__(self):
        self.feedback_data = {
            "current_pose": {"x": self.current_pose_x, "y": self.current_pose_y},
            "distance_remaining": self.distance_remaining,
            "estimated_time_remaining": self.estimated_time_remaining
        }
        if self.distance_remaining > 0:
            self.progress_percent = max(0, 100 - self.distance_remaining * 10)
        else:
            self.progress_percent = 100.0


@dataclass
class NavigateToPoseResult:
    """Navigate to pose action result"""
    goal_id: str = ""
    action_type: str = "NavigateToPose"
    status: GoalStatus = GoalStatus.STATUS_UNKNOWN
    final_pose_x: float = 0.0
    final_pose_y: float = 0.0
    final_pose_theta: float = 0.0
    total_elapsed_time: float = 0.0
    result_data: dict = field(default_factory=dict)
    error_message: str = ""
    type: MessageType = MessageType.ACTION_RESULT
    timestamp: float = field(default_factory=time.time)
    
    def __post_init__(self):
        self.result_data = {
            "final_pose": {
                "x": self.final_pose_x,
                "y": self.final_pose_y,
                "theta": self.final_pose_theta
            },
            "total_elapsed_time": self.total_elapsed_time
        }


@dataclass
class GoalStatusMessage:
    """Action goal status message"""
    goal_id: str = ""
    status: GoalStatus = GoalStatus.STATUS_UNKNOWN
    type: MessageType = MessageType.DATA


@dataclass
class GoalStatusArray:
    """Array of goal statuses"""
    status_list: List[GoalStatusMessage] = field(default_factory=list)
    type: MessageType = MessageType.DATA


@dataclass
class SendGoalRequest:
    """Request to send a goal"""
    goal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    goal: Any = None
    type: MessageType = MessageType.SERVICE_REQUEST


@dataclass
class SendGoalResponse:
    """Response to send goal request"""
    accepted: bool = False
    stamp: float = field(default_factory=time.time)
    type: MessageType = MessageType.SERVICE_RESPONSE


@dataclass
class GetResultRequest:
    """Request to get action result"""
    goal_id: str = ""
    type: MessageType = MessageType.SERVICE_REQUEST


@dataclass
class GetResultResponse:
    """Response with action result"""
    status: GoalStatus = GoalStatus.STATUS_UNKNOWN
    result: Any = None
    type: MessageType = MessageType.SERVICE_RESPONSE


@dataclass
class NavigateToPoseActionGoal:
    """NavigateToPose action goal"""
    goal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    action_type: str = "NavigateToPose"
    pose_x: float = 0.0
    pose_y: float = 0.0
    pose_theta: float = 0.0
    goal_data: dict = field(default_factory=dict)
    client_node: str = ""
    type: MessageType = MessageType.ACTION_GOAL
    timestamp: float = field(default_factory=time.time)
    
    def __post_init__(self):
        self.goal_data = {
            "pose": {
                "x": self.pose_x,
                "y": self.pose_y,
                "theta": self.pose_theta
            }
        }


@dataclass
class NavigateToPoseActionResult:
    """NavigateToPose action result"""
    goal_id: str = ""
    action_type: str = "NavigateToPose"
    status: GoalStatus = GoalStatus.STATUS_UNKNOWN
    final_pose_x: float = 0.0
    final_pose_y: float = 0.0
    final_pose_theta: float = 0.0
    result_data: dict = field(default_factory=dict)
    error_message: str = ""
    type: MessageType = MessageType.ACTION_RESULT
    timestamp: float = field(default_factory=time.time)
    
    def __post_init__(self):
        self.result_data = {
            "final_pose": {
                "x": self.final_pose_x,
                "y": self.final_pose_y,
                "theta": self.final_pose_theta
            }
        }


@dataclass
class NavigateToPoseActionFeedback:
    """NavigateToPose action feedback"""
    goal_id: str = ""
    action_type: str = "NavigateToPose"
    current_pose_x: float = 0.0
    current_pose_y: float = 0.0
    distance_remaining: float = 0.0
    feedback_data: dict = field(default_factory=dict)
    progress_percent: float = 0.0
    type: MessageType = MessageType.ACTION_FEEDBACK
    timestamp: float = field(default_factory=time.time)
    
    def __post_init__(self):
        self.feedback_data = {
            "current_pose": {
                "x": self.current_pose_x,
                "y": self.current_pose_y
            },
            "distance_remaining": self.distance_remaining
        }


@dataclass
class FibonacciActionGoal:
    """Fibonacci action goal"""
    goal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    action_type: str = "Fibonacci"
    order: int = 0
    goal_data: dict = field(default_factory=dict)
    client_node: str = ""
    type: MessageType = MessageType.ACTION_GOAL
    timestamp: float = field(default_factory=time.time)
    
    def __post_init__(self):
        self.goal_data = {"order": self.order}


@dataclass
class FibonacciActionResult:
    """Fibonacci action result"""
    goal_id: str = ""
    action_type: str = "Fibonacci"
    status: GoalStatus = GoalStatus.STATUS_UNKNOWN
    sequence: List[int] = field(default_factory=list)
    result_data: dict = field(default_factory=dict)
    error_message: str = ""
    type: MessageType = MessageType.ACTION_RESULT
    timestamp: float = field(default_factory=time.time)
    
    def __post_init__(self):
        self.result_data = {"sequence": self.sequence}


@dataclass
class FibonacciActionFeedback:
    """Fibonacci action feedback"""
    goal_id: str = ""
    action_type: str = "Fibonacci"
    sequence: List[int] = field(default_factory=list)
    feedback_data: dict = field(default_factory=dict)
    progress_percent: float = 0.0
    type: MessageType = MessageType.ACTION_FEEDBACK
    timestamp: float = field(default_factory=time.time)
    
    def __post_init__(self):
        self.feedback_data = {"sequence": self.sequence}
        # Calculate progress based on sequence length vs expected order
        # For Fibonacci, progress is based on sequence length
        self.progress_percent = min(100.0, len(self.sequence) * 10.0)  # Simple progress calculation
