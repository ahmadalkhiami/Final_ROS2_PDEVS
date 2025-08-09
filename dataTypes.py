"""
Core types and data structures for ROS2 DEVS simulation.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Callable, Tuple
from enum import Enum, auto
import time
import threading
import uuid


class MessageType(Enum):
    """ROS2 message types"""
    DATA = auto()
    SERVICE_REQUEST = auto()
    SERVICE_RESPONSE = auto()
    ACTION_GOAL = auto()
    ACTION_FEEDBACK = auto()
    ACTION_RESULT = auto()
    PARAMETER_EVENT = auto()
    CLOCK = auto()


class QoSReliabilityPolicy(Enum):
    """DDS Reliability QoS Policy"""
    RELIABLE = "RELIABLE"
    BEST_EFFORT = "BEST_EFFORT"


class QoSDurabilityPolicy(Enum):
    """DDS Durability QoS Policy"""
    VOLATILE = "VOLATILE"
    TRANSIENT_LOCAL = "TRANSIENT_LOCAL"
    TRANSIENT = "TRANSIENT"
    PERSISTENT = "PERSISTENT"


class QoSHistoryPolicy(Enum):
    """DDS History QoS Policy"""
    KEEP_LAST = "KEEP_LAST"
    KEEP_ALL = "KEEP_ALL"


class QoSLivelinessPolicy(Enum):
    """DDS Liveliness QoS Policy"""
    AUTOMATIC = "AUTOMATIC"
    MANUAL_BY_TOPIC = "MANUAL_BY_TOPIC"
    MANUAL_BY_PARTICIPANT = "MANUAL_BY_PARTICIPANT"


class QoSOwnershipPolicy(Enum):
    """DDS Ownership QoS Policy"""
    SHARED = "SHARED"
    EXCLUSIVE = "EXCLUSIVE"


@dataclass
class QoSProfile:
    """Complete DDS QoS Profile"""
    # Reliability
    reliability: QoSReliabilityPolicy = QoSReliabilityPolicy.RELIABLE
    max_blocking_time: float = 0.0  # seconds
    
    # Durability
    durability: QoSDurabilityPolicy = QoSDurabilityPolicy.VOLATILE
    
    # History
    history: QoSHistoryPolicy = QoSHistoryPolicy.KEEP_LAST
    depth: int = 10
    
    # Time-based policies
    deadline: float = float('inf')  # seconds
    lifespan: float = float('inf')  # seconds
    liveliness: QoSLivelinessPolicy = QoSLivelinessPolicy.AUTOMATIC
    liveliness_lease_duration: float = float('inf')  # seconds
    
    # Ownership
    ownership: QoSOwnershipPolicy = QoSOwnershipPolicy.SHARED
    ownership_strength: int = 0
    
    # Resource limits
    max_samples: int = 5000
    max_instances: int = 5000
    max_samples_per_instance: int = 5000
    
    # Time-based filter
    minimum_separation: float = 0.0  # seconds
    
    # Partition
    partition: List[str] = field(default_factory=list)
    
    def to_rmw_qos(self) -> 'RMWQoSProfile':
        """Convert to RMW QoS profile"""
        # This would map DDS QoS to RMW QoS
        return RMWQoSProfile(
            reliability=self.reliability,
            durability=self.durability,
            history=self.history,
            depth=self.depth,
            deadline_ms=self.deadline * 1000,
            lifespan_ms=self.lifespan * 1000
        )


@dataclass
class RMWQoSProfile:
    """RMW-level QoS Profile (simplified from DDS)"""
    reliability: QoSReliabilityPolicy
    durability: QoSDurabilityPolicy
    history: QoSHistoryPolicy
    depth: int
    deadline_ms: float
    lifespan_ms: float


@dataclass
class MessageHeader:
    """Standard message header"""
    timestamp: float
    frame_id: str = ""
    sequence: int = 0


@dataclass
class Message:
    """Base message class with complete lifecycle tracking"""
    # Identity
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: MessageType = MessageType.DATA
    
    # Routing
    topic: str = ""
    source_node: str = ""
    destination_nodes: Set[str] = field(default_factory=set)
    
    # QoS
    qos_profile: Optional[QoSProfile] = None
    
    # Payload
    data: Any = None
    serialized_data: Optional[bytes] = None
    
    # Timestamps for lifecycle tracking
    created_time: float = field(default_factory=time.time)
    published_time: float = 0.0
    serialized_time: float = 0.0
    sent_time: float = 0.0
    received_time: float = 0.0
    deserialized_time: float = 0.0
    delivered_time: float = 0.0
    
    # Delivery tracking
    delivery_attempts: int = 0
    successfully_delivered: bool = False
    qos_violations: List[Tuple[str, str]] = field(default_factory=list)
    
    # DDS specific
    writer_guid: Optional[str] = None
    sequence_number: int = 0
    
    def calculate_latency(self) -> float:
        """Calculate end-to-end latency"""
        if self.delivered_time > 0 and self.published_time > 0:
            return self.delivered_time - self.published_time
        return 0.0
    
    def mark_published(self):
        """Mark message as published"""
        self.published_time = time.time()
    
    def mark_serialized(self):
        """Mark serialization complete"""
        self.serialized_time = time.time()
    
    def mark_sent(self):
        """Mark network send"""
        self.sent_time = time.time()
    
    def mark_received(self):
        """Mark network receive"""
        self.received_time = time.time()
    
    def mark_deserialized(self):
        """Mark deserialization complete"""
        self.deserialized_time = time.time()
    
    def mark_delivered(self):
        """Mark delivery to application"""
        self.delivered_time = time.time()
        self.successfully_delivered = True


@dataclass
class NodeHandle:
    """RCL node handle"""
    name: str
    namespace: str
    handle_id: int
    context_handle: int


@dataclass
class PublisherHandle:
    """RCL publisher handle"""
    node_handle: NodeHandle
    topic: str
    qos_profile: RMWQoSProfile
    handle_id: int


@dataclass
class SubscriptionHandle:
    """RCL subscription handle"""
    node_handle: NodeHandle
    topic: str
    qos_profile: RMWQoSProfile
    handle_id: int
    callback: Optional[Callable] = None


@dataclass
class TimerHandle:
    """RCL timer handle"""
    node_handle: NodeHandle
    period_ns: int
    handle_id: int
    callback: Optional[Callable] = None


@dataclass
class ServiceHandle:
    """RCL service handle"""
    node_handle: NodeHandle
    service_name: str
    service_type: str
    handle_id: int
    callback: Optional[Callable] = None


@dataclass
class GuardConditionHandle:
    """RCL guard condition handle"""
    handle_id: int
    callback: Optional[Callable] = None


class ExecutorType(Enum):
    """Executor types"""
    SINGLE_THREADED = auto()
    MULTI_THREADED = auto()
    STATIC_SINGLE_THREADED = auto()
    EVENTS = auto()


@dataclass
class WaitSet:
    """RCL wait set for executor"""
    subscriptions: List[SubscriptionHandle] = field(default_factory=list)
    timers: List[TimerHandle] = field(default_factory=list)
    services: List[ServiceHandle] = field(default_factory=list)
    guard_conditions: List[GuardConditionHandle] = field(default_factory=list)
    
    def is_empty(self) -> bool:
        """Check if wait set is empty"""
        return not (self.subscriptions or self.timers or 
                   self.services or self.guard_conditions)


class SystemState:
    """Global system state (thread-safe)"""
    def __init__(self):
        self._lock = threading.Lock()
        self.cpu_load: float = 0.0
        self.memory_usage: float = 0.0
        self.network_latency_us: float = 50.0  # microseconds
        self.context_switches: int = 0
        self.active_threads: int = 0
    
    def update(self, **kwargs):
        """Thread-safe state update"""
        with self._lock:
            for key, value in kwargs.items():
                if hasattr(self, key):
                    setattr(self, key, value)
    
    def get_state(self) -> Dict[str, Any]:
        """Get current state snapshot"""
        with self._lock:
            return {
                'cpu_load': self.cpu_load,
                'memory_usage': self.memory_usage,
                'network_latency_us': self.network_latency_us,
                'context_switches': self.context_switches,
                'active_threads': self.active_threads
            }


# Global system state instance
system_state = SystemState()
