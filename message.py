"""
Message types and headers for ROS2 DEVS simulation.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional
from enum import Enum
import time
import uuid


class MessageType(Enum):
    """ROS2 message types"""
    DATA = "data"
    SERVICE_REQUEST = "service_request" 
    SERVICE_RESPONSE = "service_response"
    ACTION_GOAL = "action_goal"
    ACTION_RESULT = "action_result"
    ACTION_FEEDBACK = "action_feedback"
    PARAMETER = "parameter"
    LIFECYCLE = "lifecycle"


@dataclass
class MessageHeader:
    """ROS2 message header"""
    stamp: float = 0.0
    frame_id: str = ""
    
    def __post_init__(self):
        if self.stamp == 0.0:
            self.stamp = time.time()


@dataclass
class Message:
    """Basic ROS2 message"""
    header: MessageHeader
    message_type: MessageType = MessageType.DATA
    topic: str = ""
    data: Any = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if self.header is None:
            self.header = MessageHeader()
    
    @property
    def timestamp(self) -> float:
        """Get message timestamp"""
        return self.header.stamp
    
    @property
    def id(self) -> str:
        """Get unique message ID"""
        if 'id' not in self.metadata:
            self.metadata['id'] = str(uuid.uuid4())
        return self.metadata['id']
    
    def copy(self) -> 'Message':
        """Create a copy of the message"""
        return Message(
            header=MessageHeader(self.header.stamp, self.header.frame_id),
            message_type=self.message_type,
            topic=self.topic,
            data=self.data,
            metadata=self.metadata.copy()
        )
    
    def mark_published(self):
        """Mark message as published"""
        self.metadata['published'] = True
        self.metadata['publish_time'] = time.time() 