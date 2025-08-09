"""
RCLCPP Node implementation.
Provides C++ style node API for ROS2.
"""

from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
import threading
import logging

from policies import QoSProfile
from parameter import Parameter, ParameterDescriptor, ParameterType
from callback_group import CallbackGroup, MutuallyExclusiveCallbackGroup


class Node:
    """
    RCLCPP Node - represents a ROS2 node with C++ API semantics.
    """
    
    def __init__(self, node_name: str, namespace: str = "/", 
                 use_intra_process_comms: bool = True,
                 enable_topic_statistics: bool = False):
        self.node_name = node_name
        self.namespace = namespace
        self.handle: Optional[int] = None
        
        # Options
        self.use_intra_process_comms = use_intra_process_comms
        self.enable_topic_statistics = enable_topic_statistics
        
        # Components
        self.publishers: Dict[str, Any] = {}
        self.subscriptions: Dict[str, Any] = {}
        self.timers: Dict[str, Any] = {}
        self.services: Dict[str, Any] = {}
        self.clients: Dict[str, Any] = {}
        
        # Parameters
        self.parameters: Dict[str, Parameter] = {}
        self.parameter_callbacks: List[Callable] = []
        
        # Callback groups
        self.default_callback_group = MutuallyExclusiveCallbackGroup()
        self.callback_groups: List[CallbackGroup] = [self.default_callback_group]
        
        # Logging
        self.logger = self._create_logger()
        
        # Thread safety
        self._lock = threading.Lock()
        
        # Initialize default parameters
        self._initialize_default_parameters()
        
    def _create_logger(self) -> logging.Logger:
        """Create node logger"""
        logger_name = f"{self.namespace}.{self.node_name}".replace('//', '/')
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        return logger
        
    def _initialize_default_parameters(self):
        """Initialize default node parameters"""
        # use_sim_time parameter
        self.declare_parameter(
            "use_sim_time",
            Parameter.from_value("use_sim_time", False),
            ParameterDescriptor(
                name="use_sim_time",
                type=ParameterType.BOOL,
                description="Use simulation time from /clock topic"
            )
        )
        
    def get_name(self) -> str:
        """Get node name"""
        return self.node_name
        
    def get_namespace(self) -> str:
        """Get node namespace"""
        return self.namespace
        
    def get_fully_qualified_name(self) -> str:
        """Get fully qualified node name"""
        if self.namespace == "/":
            return f"/{self.node_name}"
        return f"{self.namespace}/{self.node_name}"
        
    def get_logger(self) -> logging.Logger:
        """Get node logger"""
        return self.logger
        
    # Publisher/Subscription management
    def create_publisher(self, msg_type: type, topic: str, qos: QoSProfile,
                        callback_group: Optional[CallbackGroup] = None):
        """Create a publisher"""
        if callback_group is None:
            callback_group = self.default_callback_group
            
        with self._lock:
            if topic in self.publishers:
                raise RuntimeError(f"Publisher for topic '{topic}' already exists")
                
            publisher = {
                'msg_type': msg_type,
                'topic': topic,
                'qos': qos,
                'callback_group': callback_group
            }
            self.publishers[topic] = publisher
            
        self.logger.info(f"Created publisher on topic '{topic}'")
        return publisher
        
    def create_subscription(self, msg_type: type, topic: str, callback: Callable,
                           qos: QoSProfile, callback_group: Optional[CallbackGroup] = None):
        """Create a subscription"""
        if callback_group is None:
            callback_group = self.default_callback_group
            
        with self._lock:
            if topic in self.subscriptions:
                raise RuntimeError(f"Subscription for topic '{topic}' already exists")
                
            subscription = {
                'msg_type': msg_type,
                'topic': topic,
                'callback': callback,
                'qos': qos,
                'callback_group': callback_group
            }
            self.subscriptions[topic] = subscription
            
        self.logger.info(f"Created subscription on topic '{topic}'")
        return subscription
        
    def create_timer(self, period_sec: float, callback: Callable,
                    callback_group: Optional[CallbackGroup] = None):
        """Create a timer"""
        if callback_group is None:
            callback_group = self.default_callback_group
            
        timer_id = f"timer_{len(self.timers)}"
        
        with self._lock:
            timer = {
                'period_sec': period_sec,
                'callback': callback,
                'callback_group': callback_group,
                'timer_id': timer_id
            }
            self.timers[timer_id] = timer
            
        self.logger.info(f"Created timer with period {period_sec}s")
        return timer
        
    def create_service(self, srv_type: type, service_name: str, callback: Callable,
                      qos: Optional[QoSProfile] = None,
                      callback_group: Optional[CallbackGroup] = None):
        """Create a service"""
        if callback_group is None:
            callback_group = self.default_callback_group
            
        with self._lock:
            if service_name in self.services:
                raise RuntimeError(f"Service '{service_name}' already exists")
                
            service = {
                'srv_type': srv_type,
                'service_name': service_name,
                'callback': callback,
                'qos': qos or QoSProfile(),
                'callback_group': callback_group
            }
            self.services[service_name] = service
            
        self.logger.info(f"Created service '{service_name}'")
        return service
        
    def create_client(self, srv_type: type, service_name: str,
                     qos: Optional[QoSProfile] = None,
                     callback_group: Optional[CallbackGroup] = None):
        """Create a service client"""
        if callback_group is None:
            callback_group = self.default_callback_group
            
        with self._lock:
            client = {
                'srv_type': srv_type,
                'service_name': service_name,
                'qos': qos or QoSProfile(),
                'callback_group': callback_group
            }
            self.clients[service_name] = client
            
        self.logger.info(f"Created client for service '{service_name}'")
        return client
        
    # Parameter management
    def declare_parameter(self, name: str, default_value: Parameter,
                         descriptor: Optional[ParameterDescriptor] = None):
        """Declare a parameter"""
        with self._lock:
            if name in self.parameters:
                raise RuntimeError(f"Parameter '{name}' already declared")
                
            self.parameters[name] = default_value
            
        self.logger.debug(f"Declared parameter '{name}' with default value {default_value.value}")
        return default_value
        
    def get_parameter(self, name: str) -> Parameter:
        """Get parameter value"""
        with self._lock:
            if name not in self.parameters:
                raise KeyError(f"Parameter '{name}' not declared")
            return self.parameters[name]
            
    def set_parameter(self, parameter: Parameter) -> bool:
        """Set parameter value"""
        with self._lock:
            if parameter.name not in self.parameters:
                raise KeyError(f"Parameter '{parameter.name}' not declared")
                
            old_value = self.parameters[parameter.name]
            self.parameters[parameter.name] = parameter
            
            # Notify callbacks
            for callback in self.parameter_callbacks:
                try:
                    callback(parameter.name, old_value, parameter)
                except Exception as e:
                    self.logger.error(f"Parameter callback error: {e}")
                    
        self.logger.debug(f"Set parameter '{parameter.name}' to {parameter.value}")
        return True
        
    def add_parameter_callback(self, callback: Callable):
        """Add parameter change callback"""
        with self._lock:
            self.parameter_callbacks.append(callback)
            
    # Callback group management
    def create_callback_group(self, group_type: str = "MutuallyExclusive") -> CallbackGroup:
        """Create a callback group"""
        if group_type == "MutuallyExclusive":
            group = MutuallyExclusiveCallbackGroup()
        else:
            # Add other types as needed
            group = MutuallyExclusiveCallbackGroup()
            
        with self._lock:
            self.callback_groups.append(group)
            
        return group
        
    def get_callback_groups(self) -> List[CallbackGroup]:
        """Get all callback groups"""
        with self._lock:
            return self.callback_groups.copy()
            
    # Graph introspection
    def get_topic_names_and_types(self) -> List[tuple]:
        """Get all topic names and types"""
        topics = []
        
        with self._lock:
            for topic, pub in self.publishers.items():
                topics.append((topic, [pub['msg_type'].__name__]))
                
            for topic, sub in self.subscriptions.items():
                topics.append((topic, [sub['msg_type'].__name__]))
                
        return topics
        
    def get_publisher_names_and_types_by_node(self, node_name: str, 
                                             namespace: str) -> List[tuple]:
        """Get publishers for a specific node"""
        # In real implementation, would query graph
        return []
        
    def get_subscription_names_and_types_by_node(self, node_name: str,
                                                namespace: str) -> List[tuple]:
        """Get subscriptions for a specific node"""
        # In real implementation, would query graph
        return []
        
    def get_node_names(self) -> List[str]:
        """Get all node names in the graph"""
        # In real implementation, would query graph
        return [self.get_fully_qualified_name()]
        
    def get_node_names_and_namespaces(self) -> List[tuple]:
        """Get all node names and namespaces"""
        # In real implementation, would query graph
        return [(self.node_name, self.namespace)]
        
    def count_publishers(self, topic: str) -> int:
        """Count publishers on a topic"""
        # In real implementation, would query graph
        return 1 if topic in self.publishers else 0
        
    def count_subscribers(self, topic: str) -> int:
        """Count subscribers on a topic"""
        # In real implementation, would query graph  
        return 1 if topic in self.subscriptions else 0
