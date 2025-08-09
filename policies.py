"""
QoS policies and profiles for ROS2 DEVS simulation.
"""

from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List


class QoSReliabilityPolicy(Enum):
    """QoS Reliability Policy"""
    RELIABLE = "reliable"
    BEST_EFFORT = "best_effort"


class QoSDurabilityPolicy(Enum):
    """QoS Durability Policy"""
    VOLATILE = "volatile"
    TRANSIENT_LOCAL = "transient_local"
    TRANSIENT = "transient"
    PERSISTENT = "persistent"


class QoSHistoryPolicy(Enum):
    """QoS History Policy"""
    KEEP_LAST = "keep_last"
    KEEP_ALL = "keep_all"


class QoSLivelinessPolicy(Enum):
    """QoS Liveliness Policy"""
    AUTOMATIC = "automatic"
    MANUAL_BY_NODE = "manual_by_node"
    MANUAL_BY_TOPIC = "manual_by_topic"


class QoSOwnershipPolicy(Enum):
    """QoS Ownership Policy"""
    SHARED = "shared"
    EXCLUSIVE = "exclusive"


@dataclass
class QoSProfile:
    """QoS Profile for ROS2 topics"""
    reliability: QoSReliabilityPolicy = QoSReliabilityPolicy.RELIABLE
    durability: QoSDurabilityPolicy = QoSDurabilityPolicy.VOLATILE
    history: QoSHistoryPolicy = QoSHistoryPolicy.KEEP_LAST
    depth: int = 10
    liveliness: QoSLivelinessPolicy = QoSLivelinessPolicy.AUTOMATIC
    ownership: QoSOwnershipPolicy = QoSOwnershipPolicy.SHARED
    
    # Timing constraints (in nanoseconds)
    deadline: Optional[int] = None
    lifespan: Optional[int] = None
    liveliness_lease_duration: Optional[int] = None
    
    # DDS partition
    partition: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Validate QoS profile settings"""
        if self.history == QoSHistoryPolicy.KEEP_LAST and self.depth <= 0:
            raise ValueError("KEEP_LAST history policy requires depth > 0")
            
        if self.deadline is not None and self.deadline <= 0:
            raise ValueError("Deadline must be positive")
            
        if self.lifespan is not None and self.lifespan <= 0:
            raise ValueError("Lifespan must be positive")
            
        if self.liveliness_lease_duration is not None and self.liveliness_lease_duration <= 0:
            raise ValueError("Liveliness lease duration must be positive")
    
    def is_compatible_with(self, other: 'QoSProfile') -> bool:
        """Check if this profile is compatible with another"""
        # Reliability compatibility
        if (self.reliability == QoSReliabilityPolicy.RELIABLE and 
            other.reliability == QoSReliabilityPolicy.BEST_EFFORT):
            return False
            
        # Durability compatibility
        if (self.durability == QoSDurabilityPolicy.VOLATILE and 
            other.durability != QoSDurabilityPolicy.VOLATILE):
            return False
            
        # Ownership compatibility
        if self.ownership != other.ownership:
            return False
            
        return True
    
    @classmethod
    def sensor_data(cls) -> 'QoSProfile':
        """Sensor data QoS profile"""
        return cls(
            reliability=QoSReliabilityPolicy.BEST_EFFORT,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=5
        )
    
    @classmethod
    def parameters(cls) -> 'QoSProfile':
        """Parameters QoS profile"""
        return cls(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=1000
        )
    
    @classmethod
    def services_default(cls) -> 'QoSProfile':
        """Services default QoS profile"""
        return cls(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10
        )
    
    @classmethod
    def parameter_events(cls) -> 'QoSProfile':
        """Parameter events QoS profile"""
        return cls(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=1000
        )
    
    @classmethod
    def system_default(cls) -> 'QoSProfile':
        """System default QoS profile"""
        return cls(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10
        )
