"""
QoS profile presets for ROS2 DEVS simulation.
Provides standard ROS2 QoS profiles.
"""


from policies import QoSProfile, QoSReliabilityPolicy, QoSDurabilityPolicy, QoSHistoryPolicy, QoSLivelinessPolicy

class QoSPresetProfiles:
    """Standard ROS2 QoS profiles"""
    
    @staticmethod
    def sensor_data() -> QoSProfile:
        """
        Sensor data profile - best effort, small queue.
        Used for high-frequency sensor data like laser scans.
        """
        return QoSProfile(
            reliability=QoSReliabilityPolicy.BEST_EFFORT,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=5,
            deadline=0.1,  # 100ms
            lifespan=0.5,  # 500ms
            liveliness=QoSLivelinessPolicy.AUTOMATIC,
            liveliness_lease_duration=1.0
        )
    
    @staticmethod
    def parameters() -> QoSProfile:
        """
        Parameters profile - reliable, transient local.
        Used for parameter updates and events.
        """
        return QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=1000,
            deadline=float('inf'),
            lifespan=float('inf'),
            liveliness=QoSLivelinessPolicy.AUTOMATIC,
            liveliness_lease_duration=float('inf')
        )
    
    @staticmethod
    def services() -> QoSProfile:
        """
        Services profile - reliable, volatile.
        Used for service requests and responses.
        """
        return QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,
            deadline=float('inf'),
            lifespan=float('inf'),
            liveliness=QoSLivelinessPolicy.AUTOMATIC,
            liveliness_lease_duration=float('inf')
        )
    
    @staticmethod
    def parameters_events() -> QoSProfile:
        """
        Parameter events profile - reliable, keep all.
        Used for parameter change events.
        """
        return QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_ALL,
            depth=1000,
            deadline=float('inf'),
            lifespan=float('inf'),
            liveliness=QoSLivelinessPolicy.AUTOMATIC,
            liveliness_lease_duration=float('inf')
        )
    
    @staticmethod
    def rosout() -> QoSProfile:
        """
        Rosout profile - reliable, transient local.
        Used for logging messages.
        """
        return QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=1000,
            deadline=float('inf'),
            lifespan=10.0,  # 10 seconds
            liveliness=QoSLivelinessPolicy.AUTOMATIC,
            liveliness_lease_duration=float('inf')
        )
    
    @staticmethod
    def clock() -> QoSProfile:
        """
        Clock profile - best effort, keep last one.
        Used for /clock topic.
        """
        return QoSProfile(
            reliability=QoSReliabilityPolicy.BEST_EFFORT,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=1,
            deadline=float('inf'),
            lifespan=float('inf'),
            liveliness=QoSLivelinessPolicy.AUTOMATIC,
            liveliness_lease_duration=float('inf')
        )
    
    @staticmethod
    def system_default() -> QoSProfile:
        """
        System default profile - reliable, volatile, keep last 10.
        Used as default for most topics.
        """
        return QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,
            deadline=float('inf'),
            lifespan=float('inf'),
            liveliness=QoSLivelinessPolicy.AUTOMATIC,
            liveliness_lease_duration=float('inf')
        )
    
    @staticmethod
    def default() -> QoSProfile:
        """Alias for system_default"""
        return QoSPresetProfiles.system_default()
    
    @staticmethod
    def action_status() -> QoSProfile:
        """
        Action status profile - reliable, transient local.
        Used for action server status topics.
        """
        return QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=1,
            deadline=float('inf'),
            lifespan=float('inf'),
            liveliness=QoSLivelinessPolicy.AUTOMATIC,
            liveliness_lease_duration=float('inf')
        )
    
    @staticmethod
    def map() -> QoSProfile:
        """
        Map profile - reliable, transient local, keep last one.
        Used for map data that should persist.
        """
        return QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=1,
            deadline=float('inf'),
            lifespan=float('inf'),
            liveliness=QoSLivelinessPolicy.AUTOMATIC,
            liveliness_lease_duration=float('inf')
        )
    
    @staticmethod
    def tf_static() -> QoSProfile:
        """
        TF static profile - reliable, transient local, keep all.
        Used for static transform data.
        """
        return QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            history=QoSHistoryPolicy.KEEP_ALL,
            depth=1,
            deadline=float('inf'),
            lifespan=float('inf'),
            liveliness=QoSLivelinessPolicy.AUTOMATIC,
            liveliness_lease_duration=float('inf')
        )
    
    @staticmethod
    def keep_last(depth: int) -> QoSProfile:
        """
        Create a keep-last profile with specified depth.
        """
        profile = QoSPresetProfiles.default()
        profile.history = QoSHistoryPolicy.KEEP_LAST
        profile.depth = depth
        return profile
    
    @staticmethod
    def keep_all() -> QoSProfile:
        """
        Create a keep-all profile.
        """
        profile = QoSPresetProfiles.default()
        profile.history = QoSHistoryPolicy.KEEP_ALL
        profile.depth = 0  # Ignored for KEEP_ALL
        return profile
    
    @staticmethod
    def best_effort() -> QoSProfile:
        """
        Create a best-effort profile.
        """
        profile = QoSPresetProfiles.default()
        profile.reliability = QoSReliabilityPolicy.BEST_EFFORT
        return profile
    
    @staticmethod
    def reliable() -> QoSProfile:
        """
        Create a reliable profile.
        """
        profile = QoSPresetProfiles.default()
        profile.reliability = QoSReliabilityPolicy.RELIABLE
        return profile


def get_profile_by_name(name: str) -> QoSProfile:
    """Get QoS profile by name"""
    profiles = {
        'sensor_data': QoSPresetProfiles.sensor_data,
        'parameters': QoSPresetProfiles.parameters,
        'services': QoSPresetProfiles.services,
        'parameters_events': QoSPresetProfiles.parameters_events,
        'parameter_events': QoSPresetProfiles.parameters_events,  # Alternative name
        'rosout': QoSPresetProfiles.rosout,
        'clock': QoSPresetProfiles.clock,
        'system_default': QoSPresetProfiles.system_default,
        'default': QoSPresetProfiles.default,
        'action_status': QoSPresetProfiles.action_status,
        'map': QoSPresetProfiles.map,
        'tf_static': QoSPresetProfiles.tf_static,
        'keep_last': lambda: QoSPresetProfiles.keep_last(10),  # Default depth of 10
        'keep_all': QoSPresetProfiles.keep_all,
        'best_effort': QoSPresetProfiles.best_effort,
        'reliable': QoSPresetProfiles.reliable
    }
    
    # Convert to lowercase for case-insensitive lookup
    name_lower = name.lower()
    
    if name_lower in profiles:
        return profiles[name_lower]()
    else:
        # Return default profile for unknown names instead of raising error
        return QoSPresetProfiles.system_default()