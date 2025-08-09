"""
Action-related messages for ROS2 DEVS simulation.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional
import uuid
import time

from message import Message, MessageType    

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
class ActionGoal(Message):
    """Action goal message"""
    goal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    action_type: str = ""
    goal_data: dict = field(default_factory=dict)
    client_node: str = ""
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.ACTION_GOAL
        self.timestamp = time.time()


@dataclass
class ActionGoalStatus(Message):
    """Action goal status message"""
    goal_id: str
    status: GoalStatus
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.DATA


@dataclass
class ActionFeedback(Message):
    """Action feedback message"""
    goal_id: str
    action_type: str
    feedback_data: dict = field(default_factory=dict)
    progress_percent: float = 0.0
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.ACTION_FEEDBACK
        self.timestamp = time.time()


@dataclass
class ActionResult(Message):
    """Action result message"""
    goal_id: str
    action_type: str
    status: GoalStatus
    result_data: dict = field(default_factory=dict)
    error_message: str = ""
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.ACTION_RESULT
        self.timestamp = time.time()


@dataclass
class CancelGoalRequest(Message):
    """Request to cancel an action goal"""
    goal_id: str
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.SERVICE_REQUEST


@dataclass
class CancelGoalResponse(Message):
    """Response to cancel goal request"""
    goals_canceling: list = field(default_factory=list)
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.SERVICE_RESPONSE


# Example action type definitions

@dataclass
class NavigateToPoseGoal(ActionGoal):
    """Navigate to pose action goal"""
    pose_x: float = 0.0
    pose_y: float = 0.0
    pose_theta: float = 0.0
    max_velocity: float = 1.0
    
    def __post_init__(self):
        super().__init__()
        self.action_type = "NavigateToPose"
        self.goal_data = {
            "pose": {"x": self.pose_x, "y": self.pose_y, "theta": self.pose_theta},
            "max_velocity": self.max_velocity
        }


@dataclass
class NavigateToPoseFeedback(ActionFeedback):
    """Navigate to pose action feedback"""
    current_pose_x: float = 0.0
    current_pose_y: float = 0.0
    distance_remaining: float = 0.0
    estimated_time_remaining: float = 0.0
    
    def __post_init__(self):
        super().__init__()
        self.action_type = "NavigateToPose"
        self.feedback_data = {
            "current_pose": {"x": self.current_pose_x, "y": self.current_pose_y},
            "distance_remaining": self.distance_remaining,
            "estimated_time_remaining": self.estimated_time_remaining
        }
        if self.distance_remaining > 0:
            self.progress_percent = max(0, 100 - self.distance_remaining * 10)


@dataclass
class NavigateToPoseResult(ActionResult):
    """Navigate to pose action result"""
    final_pose_x: float = 0.0
    final_pose_y: float = 0.0
    final_pose_theta: float = 0.0
    total_elapsed_time: float = 0.0
    
    def __post_init__(self):
        super().__init__()
        self.action_type = "NavigateToPose"
        self.result_data = {
            "final_pose": {
                "x": self.final_pose_x,
                "y": self.final_pose_y,
                "theta": self.final_pose_theta
            },
            "total_elapsed_time": self.total_elapsed_time
        }
