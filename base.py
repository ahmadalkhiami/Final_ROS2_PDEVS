"""
Base message types for ROS2 DEVS simulation.
"""

from dataclasses import dataclass, field
from typing import Any, Optional, List
import time

from message import MessageType

@dataclass
class StdMsgsString:
    """std_msgs/String message type"""
    data: str = ""
    type: MessageType = MessageType.DATA
    

@dataclass
class StdMsgsInt32:
    """std_msgs/Int32 message type"""
    data: int = 0
    type: MessageType = MessageType.DATA
    

@dataclass
class StdMsgsFloat64:
    """std_msgs/Float64 message type"""
    data: float = 0.0
    type: MessageType = MessageType.DATA
    

@dataclass
class StdMsgsBool:
    """std_msgs/Bool message type"""
    data: bool = False
    type: MessageType = MessageType.DATA
        

@dataclass
class StdMsgsHeader:
    """std_msgs/Header message type"""
    stamp: float = field(default_factory=time.time)
    frame_id: str = ""
    

@dataclass
class GeometryMsgsTwist:
    """geometry_msgs/Twist message type"""
    linear_x: float = 0.0
    linear_y: float = 0.0
    linear_z: float = 0.0
    angular_x: float = 0.0
    angular_y: float = 0.0
    angular_z: float = 0.0
    type: MessageType = MessageType.DATA
        

@dataclass
class GeometryMsgsPose:
    """geometry_msgs/Pose message type"""
    position_x: float = 0.0
    position_y: float = 0.0
    position_z: float = 0.0
    orientation_x: float = 0.0
    orientation_y: float = 0.0
    orientation_z: float = 0.0
    orientation_w: float = 1.0
    type: MessageType = MessageType.DATA
        

@dataclass
class SensorMsgsLaserScan:
    """sensor_msgs/LaserScan message type"""
    header: StdMsgsHeader = field(default_factory=StdMsgsHeader)
    angle_min: float = -1.57
    angle_max: float = 1.57
    angle_increment: float = 0.01
    time_increment: float = 0.0
    scan_time: float = 0.1
    range_min: float = 0.1
    range_max: float = 10.0
    ranges: List[float] = field(default_factory=list)
    intensities: List[float] = field(default_factory=list)
    type: MessageType = MessageType.DATA
        

@dataclass
class SensorMsgsJointState:
    """sensor_msgs/JointState message type"""
    header: StdMsgsHeader = field(default_factory=StdMsgsHeader)
    name: List[str] = field(default_factory=list)
    position: List[float] = field(default_factory=list)
    velocity: List[float] = field(default_factory=list)
    effort: List[float] = field(default_factory=list)
    type: MessageType = MessageType.DATA
        

@dataclass
class NavMsgsOccupancyGrid:
    """nav_msgs/OccupancyGrid message type"""
    header: StdMsgsHeader = field(default_factory=StdMsgsHeader)
    info_map_load_time: float = 0.0
    info_resolution: float = 0.05
    info_width: int = 0
    info_height: int = 0
    info_origin_position_x: float = 0.0
    info_origin_position_y: float = 0.0
    info_origin_position_z: float = 0.0
    info_origin_orientation_x: float = 0.0
    info_origin_orientation_y: float = 0.0
    info_origin_orientation_z: float = 0.0
    info_origin_orientation_w: float = 1.0
    data: List[int] = field(default_factory=list)
    type: MessageType = MessageType.DATA
