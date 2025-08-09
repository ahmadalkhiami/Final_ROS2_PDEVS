"""
QoS compatibility checking for ROS2 DEVS simulation.
Implements DDS QoS request vs offered model.
"""

from typing import Tuple, Optional, List, Dict
from dataclasses import dataclass

from policies import *


@dataclass
class QoSCompatibilityResult:
    """Result of QoS compatibility check"""
    compatible: bool
    incompatible_policies: List[str]
    warnings: List[str]
    
    def get_error_message(self) -> str:
        """Get human-readable error message"""
        if self.compatible:
            return "QoS policies are compatible"
        
        errors = []
        for policy in self.incompatible_policies:
            errors.append(f"Incompatible {policy} QoS policy")
        
        return "; ".join(errors)


class QoSCompatibilityChecker:
    """
    Checks QoS compatibility between publishers and subscribers.
    Implements the DDS request vs offered model.
    """
    
    @staticmethod
    def check_compatibility(pub_qos: QoSProfile, sub_qos: QoSProfile) -> QoSCompatibilityResult:
        """
        Check if publisher and subscriber QoS policies are compatible.
        
        Returns:
            QoSCompatibilityResult with details about compatibility
        """
        incompatible = []
        warnings = []
        
        # Check reliability
        if not QoSCompatibilityChecker._check_reliability(pub_qos, sub_qos):
            incompatible.append("reliability")
            
        # Check durability
        if not QoSCompatibilityChecker._check_durability(pub_qos, sub_qos):
            incompatible.append("durability")
            
        # Check deadline (normalize units: treat None/inf as infinite)
        if not QoSCompatibilityChecker._check_deadline(pub_qos, sub_qos):
            incompatible.append("deadline")
            
        # Check lifespan
        if not QoSCompatibilityChecker._check_lifespan(pub_qos, sub_qos):
            incompatible.append("lifespan")
            
        # Check liveliness
        if not QoSCompatibilityChecker._check_liveliness(pub_qos, sub_qos):
            incompatible.append("liveliness")
            
        # Check history
        if not QoSCompatibilityChecker._check_history(pub_qos, sub_qos):
            incompatible.append("history")
            
        # Check ownership
        if not QoSCompatibilityChecker._check_ownership(pub_qos, sub_qos):
            incompatible.append("ownership")
        # Ownership strength advisory
        if (hasattr(pub_qos, 'ownership_strength') and hasattr(sub_qos, 'ownership_strength') and
            pub_qos.ownership_strength < sub_qos.ownership_strength):
            warnings.append("publisher ownership_strength lower than subscriber expectation")
            
        # Check partition
        if not QoSCompatibilityChecker._check_partition(pub_qos, sub_qos):
            incompatible.append("partition")
            
        # Generate warnings for potential issues
        warnings.extend(QoSCompatibilityChecker._generate_warnings(pub_qos, sub_qos))
        
        return QoSCompatibilityResult(
            compatible=len(incompatible) == 0,
            incompatible_policies=incompatible,
            warnings=warnings
        )
    
    @staticmethod
    def _check_reliability(pub: QoSProfile, sub: QoSProfile) -> bool:
        """Check reliability compatibility"""
        # Subscriber requests RELIABLE, publisher must offer at least RELIABLE
        if sub.reliability == QoSReliabilityPolicy.RELIABLE:
            return pub.reliability == QoSReliabilityPolicy.RELIABLE
        # Subscriber accepts BEST_EFFORT, publisher can offer either
        return True
    
    @staticmethod
    def _check_durability(pub: QoSProfile, sub: QoSProfile) -> bool:
        """Check durability compatibility"""
        durability_levels = {
            QoSDurabilityPolicy.VOLATILE: 0,
            QoSDurabilityPolicy.TRANSIENT_LOCAL: 1,
            QoSDurabilityPolicy.TRANSIENT: 2,
            QoSDurabilityPolicy.PERSISTENT: 3
        }
        
        pub_level = durability_levels.get(pub.durability, 0)
        sub_level = durability_levels.get(sub.durability, 0)
        
        # Publisher must offer at least the durability requested
        return pub_level >= sub_level
    
    @staticmethod
    def _check_deadline(pub: QoSProfile, sub: QoSProfile) -> bool:
        """Check deadline compatibility"""
        # Normalize None/inf -> infinite
        pub_deadline = float('inf') if pub.deadline in (None, float('inf')) else pub.deadline
        sub_deadline = float('inf') if sub.deadline in (None, float('inf')) else sub.deadline
        return pub_deadline <= sub_deadline
    
    @staticmethod
    def _check_lifespan(pub: QoSProfile, sub: QoSProfile) -> bool:
        """Check lifespan compatibility"""
        pub_life = float('inf') if pub.lifespan in (None, float('inf')) else pub.lifespan
        sub_life = float('inf') if sub.lifespan in (None, float('inf')) else sub.lifespan
        return pub_life >= sub_life * 0.9
    
    @staticmethod
    def _check_liveliness(pub: QoSProfile, sub: QoSProfile) -> bool:
        """Check liveliness compatibility"""
        liveliness_levels = {
            QoSLivelinessPolicy.AUTOMATIC: 0,
            QoSLivelinessPolicy.MANUAL_BY_NODE: 1,
            QoSLivelinessPolicy.MANUAL_BY_TOPIC: 2
        }
        
        pub_level = liveliness_levels.get(pub.liveliness, 0)
        sub_level = liveliness_levels.get(sub.liveliness, 0)
        
        # Publisher must provide at least the liveliness requested
        if pub_level < sub_level:
            return False
            
        # Check lease duration - treat None/inf as infinite
        pub_lease = float('inf') if pub.liveliness_lease_duration in (None, float('inf')) else pub.liveliness_lease_duration
        sub_lease = float('inf') if sub.liveliness_lease_duration in (None, float('inf')) else sub.liveliness_lease_duration
        return pub_lease <= sub_lease
    
    @staticmethod
    def _check_history(pub: QoSProfile, sub: QoSProfile) -> bool:
        """Check history compatibility"""
        # If subscriber wants KEEP_ALL, publisher must offer KEEP_ALL
        if sub.history == QoSHistoryPolicy.KEEP_ALL:
            return pub.history == QoSHistoryPolicy.KEEP_ALL
            
        # If both use KEEP_LAST, publisher depth should be >= subscriber depth
        if pub.history == QoSHistoryPolicy.KEEP_LAST and sub.history == QoSHistoryPolicy.KEEP_LAST:
            return pub.depth >= sub.depth
            
        return True
    
    @staticmethod
    def _check_ownership(pub: QoSProfile, sub: QoSProfile) -> bool:
        """Check ownership compatibility"""
        # Ownership must match exactly
        return pub.ownership == sub.ownership
    
    @staticmethod
    def _check_partition(pub: QoSProfile, sub: QoSProfile) -> bool:
        """Check partition compatibility"""
        # If no partitions specified, they match
        if not pub.partition and not sub.partition:
            return True
            
        # At least one partition must match
        pub_partitions = set(pub.partition) if pub.partition else {''}
        sub_partitions = set(sub.partition) if sub.partition else {''}
        
        # Check for intersection (including wildcard matching)
        for pub_part in pub_partitions:
            for sub_part in sub_partitions:
                if (QoSCompatibilityChecker._partition_matches(pub_part, sub_part) or
                    QoSCompatibilityChecker._partition_matches(sub_part, pub_part)):
                    return True
                    
        return False
    
    @staticmethod
    def _partition_matches(pattern: str, name: str) -> bool:
        """Check if partition names match (with wildcard support)"""
        if '*' not in pattern and '*' not in name:
            return pattern == name
            
        # Simple wildcard matching
        if '*' in pattern:
            parts = pattern.split('*')
            if parts[0] and not name.startswith(parts[0]):
                return False
            if parts[-1] and not name.endswith(parts[-1]):
                return False
            return True
            
        return False
    
    @staticmethod
    def _generate_warnings(pub: QoSProfile, sub: QoSProfile) -> List[str]:
        """Generate warnings for potential QoS issues"""
        warnings = []
        
        # Warn about potential deadline violations
        if (pub.deadline is not None and sub.deadline is not None and 
            pub.deadline > sub.deadline * 0.8):
            warnings.append(
                f"Publisher deadline ({pub.deadline}s) is close to subscriber "
                f"deadline ({sub.deadline}s), may cause violations"
            )
            
        # Warn about small history depth
        if pub.history == QoSHistoryPolicy.KEEP_LAST and pub.depth < 5:
            warnings.append(
                f"Publisher history depth ({pub.depth}) is very small, "
                "may lose messages under high load"
            )
            
        # Warn about lifespan vs deadline mismatch
        if (pub.lifespan is not None and pub.deadline is not None and 
            pub.lifespan < pub.deadline * 2):
            warnings.append(
                "Message lifespan is less than 2x deadline, messages may "
                "expire before deadline violations are detected"
            )
            
        # Additional warnings could be added for other resource limits
        # when such attributes are available in QoSProfile
            
        return warnings


def check_endpoint_compatibility(publishers: List[QoSProfile], 
                               subscribers: List[QoSProfile]) -> Dict[Tuple[int, int], QoSCompatibilityResult]:
    """
    Check compatibility between multiple publishers and subscribers.
    
    Returns:
        Dictionary mapping (pub_idx, sub_idx) to compatibility results
    """
    results = {}
    
    for pub_idx, pub_qos in enumerate(publishers):
        for sub_idx, sub_qos in enumerate(subscribers):
            result = QoSCompatibilityChecker.check_compatibility(pub_qos, sub_qos)
            results[(pub_idx, sub_idx)] = result
            
    return results
