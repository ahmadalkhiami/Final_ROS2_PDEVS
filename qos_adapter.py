"""
QoS adaptation between RMW and DDS layers.
Handles QoS policy mapping and compatibility checking.
"""

from typing import Tuple, Optional
from dataclasses import dataclass

from dataTypes import QoSOwnershipPolicy, RMWQoSProfile
from policies import QoSProfile, QoSReliabilityPolicy, QoSDurabilityPolicy, QoSHistoryPolicy, QoSLivelinessPolicy

class QoSAdapter:
    """Adapts QoS policies between RMW and DDS layers"""
    
    @staticmethod
    def rmw_to_dds(rmw_qos: RMWQoSProfile) -> QoSProfile:
        """Convert RMW QoS profile to DDS QoS profile"""
        return QoSProfile(
            reliability=rmw_qos.reliability,
            durability=rmw_qos.durability,
            history=rmw_qos.history,
            depth=rmw_qos.depth,
            deadline=rmw_qos.deadline_ms / 1000.0,  # Convert to seconds
            lifespan=rmw_qos.lifespan_ms / 1000.0,   # Convert to seconds
            liveliness=QoSLivelinessPolicy.AUTOMATIC,
            ownership=QoSOwnershipPolicy.SHARED
        )
        
    @staticmethod
    def dds_to_rmw(dds_qos: QoSProfile) -> RMWQoSProfile:
        """Convert DDS QoS profile to RMW QoS profile"""
        return RMWQoSProfile(
            reliability=dds_qos.reliability,
            durability=dds_qos.durability,
            history=dds_qos.history,
            depth=dds_qos.depth,
            deadline_ms=dds_qos.deadline * 1000.0,  # Convert to milliseconds
            lifespan_ms=dds_qos.lifespan * 1000.0   # Convert to milliseconds
        )
        
    @staticmethod
    def check_compatibility(pub_qos: QoSProfile, sub_qos: QoSProfile) -> Tuple[bool, Optional[str]]:
        """
        Check QoS compatibility between publisher and subscriber.
        Implements ROS2 QoS compatibility rules.
        """
        # Reliability check
        if not QoSAdapter._check_reliability(pub_qos.reliability, sub_qos.reliability):
            return False, "Incompatible reliability QoS"
            
        # Durability check
        if not QoSAdapter._check_durability(pub_qos.durability, sub_qos.durability):
            return False, "Incompatible durability QoS"
            
        # Deadline check
        if not QoSAdapter._check_deadline(pub_qos.deadline, sub_qos.deadline):
            return False, "Incompatible deadline QoS"
            
        # Lifespan check
        if not QoSAdapter._check_lifespan(pub_qos.lifespan, sub_qos.lifespan):
            return False, "Incompatible lifespan QoS"
            
        # Liveliness check
        if not QoSAdapter._check_liveliness(pub_qos.liveliness, sub_qos.liveliness):
            return False, "Incompatible liveliness QoS"
            
        # History check
        if not QoSAdapter._check_history(pub_qos.history, pub_qos.depth, 
                                         sub_qos.history, sub_qos.depth):
            return False, "Incompatible history QoS"
            
        return True, None
        
    @staticmethod
    def _check_reliability(pub: QoSReliabilityPolicy, sub: QoSReliabilityPolicy) -> bool:
        """Check reliability compatibility"""
        # Publisher must offer at least what subscriber requests
        if sub == QoSReliabilityPolicy.RELIABLE and pub == QoSReliabilityPolicy.BEST_EFFORT:
            return False
        return True
        
    @staticmethod
    def _check_durability(pub: QoSDurabilityPolicy, sub: QoSDurabilityPolicy) -> bool:
        """Check durability compatibility"""
        durability_levels = {
            QoSDurabilityPolicy.VOLATILE: 0,
            QoSDurabilityPolicy.TRANSIENT_LOCAL: 1,
            QoSDurabilityPolicy.TRANSIENT: 2,
            QoSDurabilityPolicy.PERSISTENT: 3
        }
        
        pub_level = durability_levels.get(pub, 0)
        sub_level = durability_levels.get(sub, 0)
        
        # Publisher must offer at least the durability requested by subscriber
        return pub_level >= sub_level
        
    @staticmethod
    def _check_deadline(pub_deadline: float, sub_deadline: float) -> bool:
        """Check deadline compatibility"""
        # Publisher deadline must be <= subscriber deadline
        return pub_deadline <= sub_deadline
        
    @staticmethod
    def _check_lifespan(pub_lifespan: float, sub_lifespan: float) -> bool:
        """Check lifespan compatibility"""
        # Publisher lifespan should be >= subscriber expected lifespan
        # This ensures messages don't expire before subscriber can process them
        return pub_lifespan >= sub_lifespan
        
    @staticmethod
    def _check_liveliness(pub: QoSLivelinessPolicy, sub: QoSLivelinessPolicy) -> bool:
        """Check liveliness compatibility"""
        liveliness_levels = {
            QoSLivelinessPolicy.AUTOMATIC: 0,
            QoSLivelinessPolicy.MANUAL_BY_TOPIC: 1,
            QoSLivelinessPolicy.MANUAL_BY_NODE: 2
        }
        
        pub_level = liveliness_levels.get(pub, 0)
        sub_level = liveliness_levels.get(sub, 0)
        
        # Publisher must provide at least the liveliness assertion requested
        return pub_level >= sub_level
        
    @staticmethod
    def _check_history(pub_history: QoSHistoryPolicy, pub_depth: int,
                      sub_history: QoSHistoryPolicy, sub_depth: int) -> bool:
        """Check history compatibility"""
        # If both use KEEP_LAST, publisher depth should be >= subscriber depth
        if pub_history == QoSHistoryPolicy.KEEP_LAST and sub_history == QoSHistoryPolicy.KEEP_LAST:
            return pub_depth >= sub_depth
            
        # If subscriber wants KEEP_ALL, publisher must also offer KEEP_ALL
        if sub_history == QoSHistoryPolicy.KEEP_ALL and pub_history != QoSHistoryPolicy.KEEP_ALL:
            return False
            
        return True


@dataclass
class QoSEndpointInfo:
    """QoS information for an endpoint"""
    topic_name: str
    qos_profile: QoSProfile
    endpoint_type: str  # "publisher" or "subscription"
    node_name: str
    
    
class QoSPolicyValidator:
    """Validates QoS policies for correctness"""
    
    @staticmethod
    def validate_profile(qos: QoSProfile) -> Tuple[bool, Optional[str]]:
        """Validate a QoS profile for internal consistency"""
        # Check deadline is positive
        if qos.deadline is not None and qos.deadline <= 0:
            return False, "Deadline must be positive"
            
        # Check lifespan is positive
        if qos.lifespan is not None and qos.lifespan <= 0:
            return False, "Lifespan must be positive"
            
        # Check depth is positive for KEEP_LAST
        if qos.history == QoSHistoryPolicy.KEEP_LAST and qos.depth <= 0:
            return False, "Depth must be positive for KEEP_LAST history"
            
        # Check liveliness lease duration
        if qos.liveliness_lease_duration is not None and qos.liveliness_lease_duration <= 0:
            return False, "Liveliness lease duration must be positive"
            
        return True, None