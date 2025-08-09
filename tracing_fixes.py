"""
Enhanced tracing fixes for ROS2 PDEVS simulation to match real ros2_tracing output.
This file provides corrected tracing methods and patterns.
"""

import time
import random
from typing import Dict, Any, Optional
from tracing import trace_logger, ROS2TraceEvent


class EnhancedROS2TraceLogger:
    """Enhanced tracer that matches real ROS2 tracing patterns exactly"""
    
    def __init__(self):
        self.trace_logger = trace_logger
        self.handle_counter = 0xAAAAE0000000
        self.message_handles = {}
        self.node_handles = {}
        self.pub_handles = {}
        self.sub_handles = {}
        self.rmw_handles = {}
        
    def _next_handle(self, base: int = 0xAAAAE0000000) -> str:
        """Generate realistic handle addresses"""
        self.handle_counter += random.randint(0x1000, 0xFFFFF)
        return f"0x{self.handle_counter:X}"
        
    def _format_ros2_fields(self, **fields) -> str:
        """Format fields exactly like real ROS2 traces"""
        formatted_fields = []
        for key, value in fields.items():
            if isinstance(value, str) and not value.startswith('0x'):
                # String values get double quotes
                formatted_fields.append(f'{key} = "{value}"')
            else:
                formatted_fields.append(f'{key} = {value}')
        
        return "{ " + ", ".join(formatted_fields) + " }"
    
    def log_system_init(self):
        """Log complete system initialization sequence"""
        # 1. RCL initialization
        context_handle = self._next_handle()
        self.trace_logger.log_event(
            "rcl_init", 
            self._format_ros2_fields(
                context_handle=context_handle,
                version="4.1.1"
            )
        )
        
        return context_handle
    
    def log_node_initialization(self, node_name: str, namespace: str = "/"):
        """Log proper node initialization sequence"""
        node_handle = self._next_handle()
        rmw_handle = self._next_handle()
        
        self.node_handles[node_name] = {
            'node_handle': node_handle,
            'rmw_handle': rmw_handle
        }
        
        # Node init event
        self.trace_logger.log_event(
            "rcl_node_init",
            self._format_ros2_fields(
                node_handle=node_handle,
                rmw_handle=rmw_handle,
                node_name=node_name,
                namespace=namespace
            )
        )
        
        return node_handle
    
    def log_publisher_initialization(self, node_name: str, topic_name: str, 
                                   type_name: str = "sensor_msgs/msg/Image",
                                   queue_depth: int = 1000):
        """Log complete publisher initialization sequence"""
        
        # Get or create node handles
        if node_name not in self.node_handles:
            self.log_node_initialization(node_name)
        
        node_handle = self.node_handles[node_name]['node_handle']
        
        # 1. RMW publisher init
        rmw_publisher_handle = self._next_handle()
        gid = self._generate_realistic_gid()
        
        self.trace_logger.log_event(
            "rmw_publisher_init",
            self._format_ros2_fields(
                rmw_publisher_handle=rmw_publisher_handle,
                gid=gid
            )
        )
        
        # 2. Initial rmw_publish (empty)
        message_handle = self._next_handle(0xFFFFD0000000)
        self.trace_logger.log_event(
            "rmw_publish",
            self._format_ros2_fields(message=message_handle)
        )
        
        # 3. RCL publisher init
        publisher_handle = self._next_handle()
        self.trace_logger.log_event(
            "rcl_publisher_init",
            self._format_ros2_fields(
                publisher_handle=publisher_handle,
                node_handle=node_handle,
                rmw_publisher_handle=rmw_publisher_handle,
                topic_name=topic_name,
                queue_depth=queue_depth
            )
        )
        
        self.pub_handles[f"{node_name}:{topic_name}"] = {
            'publisher_handle': publisher_handle,
            'rmw_publisher_handle': rmw_publisher_handle
        }
        
        return publisher_handle
    
    def log_subscription_initialization(self, node_name: str, topic_name: str,
                                      type_name: str = "sensor_msgs/msg/Image",
                                      queue_depth: int = 1000):
        """Log complete subscription initialization sequence"""
        
        if node_name not in self.node_handles:
            self.log_node_initialization(node_name)
            
        node_handle = self.node_handles[node_name]['node_handle']
        
        # 1. RMW subscription init
        rmw_subscription_handle = self._next_handle()
        gid = self._generate_realistic_gid()
        
        self.trace_logger.log_event(
            "rmw_subscription_init",
            self._format_ros2_fields(
                rmw_subscription_handle=rmw_subscription_handle,
                gid=gid
            )
        )
        
        # 2. RCL subscription init
        subscription_handle = self._next_handle()
        self.trace_logger.log_event(
            "rcl_subscription_init",
            self._format_ros2_fields(
                subscription_handle=subscription_handle,
                node_handle=node_handle,
                rmw_subscription_handle=rmw_subscription_handle,
                topic_name=topic_name,
                queue_depth=queue_depth
            )
        )
        
        self.sub_handles[f"{node_name}:{topic_name}"] = {
            'subscription_handle': subscription_handle,
            'rmw_subscription_handle': rmw_subscription_handle
        }
        
        return subscription_handle
    
    def log_publish_sequence(self, node_name: str, topic_name: str, 
                           message_data: Dict[str, Any]):
        """Log complete publish sequence matching real ROS2"""
        
        key = f"{node_name}:{topic_name}"
        if key not in self.pub_handles:
            self.log_publisher_initialization(node_name, topic_name)
        
        handles = self.pub_handles[key]
        node_handle = self.node_handles[node_name]['node_handle']
        
        # Generate message handle
        message_handle = self._next_handle(0xFFFFD0000000)
        self.message_handles[message_handle] = message_data
        
        # 1. RCLCPP publish
        self.trace_logger.log_event(
            "rclcpp_publish",
            self._format_ros2_fields(message=message_handle)
        )
        
        # 2. RCL publish
        self.trace_logger.log_event(
            "rcl_publish", 
            self._format_ros2_fields(
                publisher_handle=handles['publisher_handle'],
                message=message_handle
            )
        )
        
        # 3. RMW publish
        self.trace_logger.log_event(
            "rmw_publish",
            self._format_ros2_fields(message=message_handle)
        )
        
        return message_handle
    
    def log_subscription_callback(self, node_name: str, topic_name: str,
                                message_handle: str, taken: int = 1):
        """Log subscription callback sequence"""
        
        key = f"{node_name}:{topic_name}"
        if key not in self.sub_handles:
            self.log_subscription_initialization(node_name, topic_name)
            
        handles = self.sub_handles[key]
        
        # 1. RMW take
        source_timestamp = time.time()
        self.trace_logger.log_event(
            "rmw_take",
            self._format_ros2_fields(
                rmw_subscription_handle=handles['rmw_subscription_handle'],
                message=message_handle,
                source_timestamp=f"{source_timestamp:.9f}",
                taken=taken
            )
        )
        
        if taken == 1:
            # 2. RCL take
            self.trace_logger.log_event(
                "rcl_take",
                self._format_ros2_fields(message=message_handle)
            )
            
            # 3. RCLCPP take
            self.trace_logger.log_event(
                "rclcpp_take",
                self._format_ros2_fields(message=message_handle)
            )
            
            # 4. Callback start
            callback_handle = self._next_handle()
            self.trace_logger.log_event(
                "callback_start",
                self._format_ros2_fields(
                    callback=callback_handle,
                    is_intra_process=0
                )
            )
            
            return callback_handle
            
        return None
    
    def log_callback_end(self, callback_handle: str):
        """Log callback completion"""
        self.trace_logger.log_event(
            "callback_end",
            self._format_ros2_fields(callback=callback_handle)
        )
    
    def log_executor_wait_for_work(self, timeout: int = 0, context_key: str = "executor"):
        """Log executor wait_for_work event"""
        self.trace_logger.log_event(
            "rclcpp_executor_wait_for_work",
            self._format_ros2_fields(timeout=timeout),
            context_key
        )
    
    def log_executor_get_next_ready(self, context_key: str = "executor"):
        """Log executor get_next_ready event"""
        self.trace_logger.log_event(
            "rclcpp_executor_get_next_ready",
            "{ }",
            context_key
        )
    
    def log_executor_execute(self, handle: str, context_key: str = "executor"):
        """Log executor execute event"""
        self.trace_logger.log_event(
            "rclcpp_executor_execute",
            self._format_ros2_fields(handle=handle),
            context_key
        )
    
    def log_executor_spin_some(self, nodes: int, context_key: str = "executor"):
        """Log executor spin_some event"""
        self.trace_logger.log_event(
            "rclcpp_executor_spin_some",
            self._format_ros2_fields(nodes=nodes),
            context_key
        )
    
    def _generate_realistic_gid(self) -> str:
        """Generate realistic GID array like real ROS2"""
        gid_values = [1, 15, 62, 8] + [random.randint(10, 35) for _ in range(4)]
        gid_values.extend([0] * 8)  # zeros for bytes 8-15
        gid_values.extend([random.randint(1, 20) for _ in range(2)])  # entity bytes
        gid_values.extend([0] * 6)  # trailing zeros
        
        gid_str = "[ " + ", ".join(f"[{i}] = {val}" for i, val in enumerate(gid_values)) + " ]"
        return gid_str


# Create enhanced global tracer instance
enhanced_tracer = EnhancedROS2TraceLogger()