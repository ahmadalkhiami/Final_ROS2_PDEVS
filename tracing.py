"""
Enhanced ROS2-compatible trace logging system for ROS2 DEVS simulation.
Matches real ROS2 tracing format while maintaining flexible architecture.
"""

import time
import json
import os
import platform
import random
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum

from context import context_manager


class TraceFormat(Enum):
    """Trace output formats"""
    ROS2_COMPATIBLE = "ros2_compatible"  # Matches real ROS2 traces
    LTTNG_LIKE = "lttng_like"           # Generic LTTng format
    JSON = "json"                       # JSON format for analysis


@dataclass
class ROS2TraceEvent:
    """ROS2-compatible trace event"""
    timestamp: float
    event_name: str
    context_key: str
    fields: str  # Formatted fields string
    cpu_id: int
    procname: str
    vtid: int
    vpid: int
    
    def to_ros2_format(self, delta: str = "+?.?????????") -> str:
        """Format event in ROS2-compatible format"""
        # Format timestamp like ROS2: HH:MM:SS.nanoseconds
        dt = datetime.fromtimestamp(self.timestamp)
        hours = dt.hour
        minutes = dt.minute
        seconds = dt.second
        nanoseconds = int((self.timestamp % 1) * 1000000000)
        
        timestamp_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}.{nanoseconds:09d}"
        
        # Get hostname (default to student-jetson like original)
        hostname = platform.node() or "student-jetson"
        
        # Format procname with quotes (double quotes like real traces)
        procname_str = f'procname = ""{self.procname}""'
        
        # Add trailing commas to match CSV format
        return (f"[{timestamp_str}] ({delta}) {hostname} ros2:{self.event_name}: "
                f"{{ cpu_id = {self.cpu_id} }}, "
                f"{{ {procname_str}, vtid = {self.vtid}, vpid = {self.vpid} }}, "
                f"{self.fields},,,,,,,,,,,,,,,,,,,,,,")
    
    def to_lttng_format(self, delta: float = 0.0) -> str:
        """Format event in LTTng-like format"""
        dt = datetime.fromtimestamp(self.timestamp)
        timestamp_str = dt.strftime("%H:%M:%S.%f")
        delta_str = f"+{delta*1000:.3f}ms" if delta > 0 else "+0.000ms"
        hostname = platform.node() or "student-jetson"
        
        return (f"[{timestamp_str}] ({delta_str}) {hostname} "
                f"{self.event_name}: {self.fields}")
    
    def to_json(self) -> Dict[str, Any]:
        """Convert event to JSON format"""
        return {
            'timestamp': self.timestamp,
            'event': self.event_name,
            'context': self.context_key,
            'fields': self.fields,
            'cpu_id': self.cpu_id,
            'procname': self.procname,
            'vtid': self.vtid,
            'vpid': self.vpid
        }


class ROS2TraceLogger:
    """Enhanced ROS2-compatible trace logger"""
    
    def __init__(self):
        self.events: List[ROS2TraceEvent] = []
        self.start_time = time.time()
        self.last_timestamp = 0.0
        self.enabled = True
        self.console_output = True
        self.file_output = False
        self.file_path = "ros2_traces.csv"
        self.format = TraceFormat.ROS2_COMPATIBLE
        self.filter_patterns = []
        self.exclude_patterns = []
        
        # ROS2-specific configuration
        self.hostname = platform.node() or "student-jetson"
        self.base_hours = 18  # Start time offset for realistic timestamps
        self.base_minutes = 44
        
    def enable(self):
        """Enable trace logging"""
        self.enabled = True
        
    def disable(self):
        """Disable trace logging"""
        self.enabled = False
        
    def set_console_output(self, enabled: bool):
        """Enable/disable console output"""
        self.console_output = enabled
        
    def enable_file_output(self, file_path: str):
        """Enable trace output to file"""
        self.file_output = True
        self.file_path = file_path
        
    def set_format(self, format_type: TraceFormat):
        """Set output format"""
        self.format = format_type
        
    def set_filter_patterns(self, patterns: List[str]):
        """Set event name patterns to include"""
        self.filter_patterns = patterns
        
    def set_exclude_patterns(self, patterns: List[str]):
        """Set event name patterns to exclude"""
        self.exclude_patterns = patterns
        
    def _format_timestamp(self, current_time: float) -> str:
        """Format timestamp in ROS2 style"""
        hours = int(current_time // 3600) + self.base_hours
        minutes = int((current_time % 3600) // 60) + self.base_minutes
        seconds = int(current_time % 60)
        nanoseconds = int((current_time % 1) * 1000000000)
        
        if nanoseconds >= 1000000000:
            nanoseconds -= 1000000000
            seconds += 1
            
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{nanoseconds:09d}"
    
    def _calculate_delta(self, current_time: float) -> str:
        """Calculate time delta between events"""
        if self.last_timestamp == 0.0:
            delta = "+?.?????????"
        else:
            delta_val = current_time - self.last_timestamp
            delta = f"+{delta_val:.9f}"
            
        self.last_timestamp = current_time
        return delta
        
    def log_event(self, event_name: str, fields: str, context_key: str = None,
                  custom_context: Dict[str, Any] = None):
        """Log a ROS2-compatible trace event"""
        if not self.enabled:
            return
            
        # Check filters
        if self.filter_patterns and not any(p in event_name 
                                          for p in self.filter_patterns):
            return
            
        if any(p in event_name for p in self.exclude_patterns):
            return
        
        # Handle context_key - convert ExecutionContext to string if needed
        context_key_str = str(context_key) if context_key is not None else "global"
        
        # Get execution context
        if custom_context:
            context = custom_context
        elif context_key:
            context = context_manager.get_ros2_context(context_key)
            if not context:
                # Fallback to default context
                context = {
                    'cpu_id': random.choice([0, 1, 2, 3, 4, 5]),
                    'procname': 'python3',
                    'vtid': 8238,
                    'vpid': 8212
                }
        else:
            # Default context - use realistic process names from real traces
            process_names = ['python3', 'dummy_map_serve', 'robot_state_pub', 'dummy_joint_sta', 'dummy_laser']
            procname = random.choice(process_names)
            context = {
                'cpu_id': random.choice([0, 1, 2, 3, 4, 5]),
                'procname': procname,
                'vtid': random.randint(6900, 9100),
                'vpid': random.randint(6900, 9100)
            }
        
        # Occasionally change CPU (matches real behavior)
        if random.random() < 0.03:
            context['cpu_id'] = random.choice([0, 1, 2, 3, 4, 5])
        
        current_time = time.time() - self.start_time
        timestamp = self._format_timestamp(current_time)
        delta = self._calculate_delta(current_time)
        
        # Create event
        event = ROS2TraceEvent(
            timestamp=time.time(),
            event_name=event_name,
            context_key=context_key_str,
            fields=fields,
            cpu_id=context.get('cpu_id', 0),
            procname=context.get('procname', 'default_proc'),
            vtid=context.get('vtid', 6907),
            vpid=context.get('vpid', 6907)
        )
        
        # Store event
        self.events.append(event)
        
        # Console output
        if self.console_output:
            if self.format == TraceFormat.ROS2_COMPATIBLE:
                print(event.to_ros2_format(delta))
            elif self.format == TraceFormat.LTTNG_LIKE:
                print(event.to_lttng_format(current_time))
            else:
                print(json.dumps(event.to_json()))
            
        # File output
        if self.file_output:
            with open(self.file_path, 'a') as f:
                if self.format == TraceFormat.ROS2_COMPATIBLE:
                    f.write(event.to_ros2_format(delta) + '\n')
                elif self.format == TraceFormat.LTTNG_LIKE:
                    f.write(event.to_lttng_format(current_time) + '\n')
                else:
                    f.write(json.dumps(event.to_json()) + '\n')
    
    # Convenience methods for ROS2-specific events
    def log_rcl_init(self, context_handle: str = None, version: str = "4.1.1"):
        """Log rcl_init event"""
        if context_handle is None:
            context_handle = f"0x{random.randint(0xAAAAA0000000, 0xAAAAAFFFFFFF):X}"
        self.log_event("rcl_init", 
                      f'{{ context_handle = {context_handle}, version = "{version}" }}',
                      "system")
    
    def log_rcl_publisher_init(self, publisher_handle: str = None, 
                              node_handle: str = None, topic_name: str = "/topic",
                              qos: str = "default", context_key: str = None):
        """Log rcl_publisher_init event"""
        if publisher_handle is None:
            publisher_handle = f"0x{random.randint(0xFFFFD0000000, 0xFFFFDFFFFFFF):X}"
        if node_handle is None:
            node_handle = f"0x{random.randint(0xAAAAA0000000, 0xAAAAAFFFFFFF):X}"
        self.log_event("rcl_publisher_init",
                      f'{{ publisher_handle = {publisher_handle}, '
                      f'node_handle = {node_handle}, '
                      f'topic_name = "{topic_name}", qos = "{qos}" }}',
                      context_key)
    
    def log_rcl_subscription_init(self, topic_name: str = "/topic", 
                                 qos: str = "default", context_key: str = None):
        """Log rcl_subscription_init event"""
        self.log_event("rcl_subscription_init",
                      f'{{ topic_name = "{topic_name}", qos = "{qos}" }}',
                      context_key)
    
    def log_rclcpp_publish(self, message_id: int, topic: str, context_key: str = None):
        """Log rclcpp_publish event"""
        self.log_event("rclcpp_publish",
                      f'{{ message_id = {message_id}, topic = "{topic}" }}',
                      context_key)
    
    def log_rcl_publish(self, message_id: int, publisher_handle: str = None,
                       node_handle: str = None, context_key: str = None):
        """Log rcl_publish event"""
        if publisher_handle is None:
            publisher_handle = f"0x{random.randint(0xFFFFD0000000, 0xFFFFDFFFFFFF):X}"
        if node_handle is None:
            node_handle = f"0x{random.randint(0xAAAAA0000000, 0xAAAAAFFFFFFF):X}"
        self.log_event("rcl_publish",
                      f'{{ message_id = {message_id}, '
                      f'publisher_handle = {publisher_handle}, '
                      f'node_handle = {node_handle} }}',
                      context_key)
    
    def log_rmw_publish(self, message: str = None, context_key: str = None):
        """Log rmw_publish event"""
        if message is None:
            message = f"0x{random.randint(0xFFFFD0000000, 0xFFFFDFFFFFFF):X}"
        self.log_event("rmw_publish",
                      f'{{ message = {message} }}',
                      context_key)
    
    def log_rmw_take(self, rmw_subscription_handle: str = None, message: str = None,
                    source_timestamp: float = None, taken: int = 1, context_key: str = None):
        """Log rmw_take event"""
        if rmw_subscription_handle is None:
            rmw_subscription_handle = f"0x{random.randint(0xAAAAA0000000, 0xAAAAAFFFFFFF):X}"
        if message is None:
            message = f"0x{random.randint(0xFFFF00000000, 0xFFFFFFFFFFFF):X}"
        if source_timestamp is None:
            source_timestamp = time.time()
        
        self.log_event("rmw_take",
                      f'{{ rmw_subscription_handle = {rmw_subscription_handle}, '
                      f'message = {message}, '
                      f'source_timestamp = {source_timestamp:.9f}, '
                      f'taken = {taken} }}',
                      context_key)
    
    def log_callback_start(self, message_id: int, context_key: str = None):
        """Log callback_start event"""
        self.log_event("callback_start",
                      f'{{ message_id = {message_id} }}',
                      context_key)
    
    def log_callback_end(self, message_id: int, context_key: str = None):
        """Log callback_end event"""
        self.log_event("callback_end",
                      f'{{ message_id = {message_id} }}',
                      context_key)
    
    def log_rclcpp_callback_register(self, symbol: str, context_key: str = None):
        """Log rclcpp_callback_register event"""
        self.log_event("rclcpp_callback_register",
                      f'{{ symbol = "{symbol}" }}',
                      context_key)
    
    def log_rcl_node_init(self, node_name: str, namespace: str = "/", context_key: str = None):
        """Log rcl_node_init event"""
        self.log_event("rcl_node_init",
                      f'{{ node_name = "{node_name}", namespace = "{namespace}" }}',
                      context_key)
    
    def log_rclcpp_executor_wait_for_work(self, timeout: int = 0, context_key: str = None):
        """Log rclcpp_executor_wait_for_work event"""
        self.log_event("rclcpp_executor_wait_for_work", 
                      f'{{ timeout = {timeout} }}', 
                      context_key)
    
    def log_rclcpp_executor_get_next_ready(self, context_key: str = None):
        """Log rclcpp_executor_get_next_ready event"""
        self.log_event("rclcpp_executor_get_next_ready", "{ }", context_key)
    
    def log_rclcpp_executor_execute(self, handle: str = None, context_key: str = None):
        """Log rclcpp_executor_execute event"""
        if handle is None:
            handle = f"0x{random.randint(0xAAAA00000000, 0xAAAAFFFFFFFF):X}"
        self.log_event("rclcpp_executor_execute",
                      f'{{ handle = {handle} }}',
                      context_key)
    
    def log_rclcpp_executor_spin_some(self, nodes: int = 4, context_key: str = None):
        """Log rclcpp_executor_spin_some event"""
        self.log_event("rclcpp_executor_spin_some",
                      f'{{ nodes = {nodes} }}',
                      context_key)
    
    def log_rcl_take(self, message: str = None, context_key: str = None):
        """Log rcl_take event"""
        if message is None:
            message = f"0x{random.randint(0xAAAA00000000, 0xAAAAFFFFFFFF):X}"
        self.log_event("rcl_take",
                      f'{{ message = {message} }}',
                      context_key)
    
    def log_rclcpp_take(self, message: str = None, context_key: str = None):
        """Log rclcpp_take event"""
        if message is None:
            message = f"0x{random.randint(0xAAAA00000000, 0xAAAAFFFFFFFF):X}"
        self.log_event("rclcpp_take",
                      f'{{ message = {message} }}',
                      context_key)
    
    def log_callback_start(self, callback: str = None, is_intra_process: int = 0, context_key: str = None):
        """Log callback_start event"""
        if callback is None:
            callback = f"0x{random.randint(0xAAAA00000000, 0xAAAAFFFFFFFF):X}"
        self.log_event("callback_start",
                      f'{{ callback = {callback}, is_intra_process = {is_intra_process} }}',
                      context_key)
    
    def log_callback_end(self, callback: str = None, context_key: str = None):
        """Log callback_end event"""
        if callback is None:
            callback = f"0x{random.randint(0xAAAA00000000, 0xAAAAFFFFFFFF):X}"
        self.log_event("callback_end",
                      f'{{ callback = {callback} }}',
                      context_key)
    
    def log_rcl_service_init(self, service_handle: str = None, node_handle: str = None, 
                           rmw_service_handle: str = None, service_name: str = "/service", context_key: str = None):
        """Log rcl_service_init event"""
        if service_handle is None:
            service_handle = f"0x{random.randint(0xAAAA00000000, 0xAAAAFFFFFFFF):X}"
        if node_handle is None:
            node_handle = f"0x{random.randint(0xAAAA00000000, 0xAAAAFFFFFFFF):X}"
        if rmw_service_handle is None:
            rmw_service_handle = f"0x{random.randint(0xAAAA00000000, 0xAAAAFFFFFFFF):X}"
        self.log_event("rcl_service_init",
                      f'{{ service_handle = {service_handle}, node_handle = {node_handle}, '
                      f'rmw_service_handle = {rmw_service_handle}, service_name = ""{service_name}"" }}',
                      context_key)
    
    def log_rclcpp_service_callback_added(self, service_handle: str = None, callback: str = None, context_key: str = None):
        """Log rclcpp_service_callback_added event"""
        if service_handle is None:
            service_handle = f"0x{random.randint(0xAAAA00000000, 0xAAAAFFFFFFFF):X}"
        if callback is None:
            callback = f"0x{random.randint(0xAAAA00000000, 0xAAAAFFFFFFFF):X}"
        self.log_event("rclcpp_service_callback_added",
                      f'{{ service_handle = {service_handle}, callback = {callback} }}',
                      context_key)
    
    def log_rmw_publisher_init(self, rmw_publisher_handle: str = None, gid: str = None, context_key: str = None):
        """Log rmw_publisher_init event"""
        if rmw_publisher_handle is None:
            rmw_publisher_handle = f"0x{random.randint(0xAAAA00000000, 0xAAAAFFFFFFFF):X}"
        if gid is None:
            gid = "[ [0] = 1, [1] = 15, [2] = 62, [3] = 8, [4] = 12, [5] = 35, [6] = 117, [7] = 79, [8] = 0, [9] = 0, [10] = 0, [11] = 0, [12] = 0, [13] = 0, [14] = 1, [15] = 3, [16] = 0, [17] = 0, [18] = 0, [19] = 0, [20] = 0, [21] = 0, [22] = 0, [23] = 0 ]"
        self.log_event("rmw_publisher_init",
                      f'{{ rmw_publisher_handle = {rmw_publisher_handle}, gid = {gid} }}',
                      context_key)
    
    def log_rmw_subscription_init(self, rmw_subscription_handle: str = None, gid: str = None, context_key: str = None):
        """Log rmw_subscription_init event"""
        if rmw_subscription_handle is None:
            rmw_subscription_handle = f"0x{random.randint(0xAAAA00000000, 0xAAAAFFFFFFFF):X}"
        if gid is None:
            gid = "[ [0] = 1, [1] = 15, [2] = 62, [3] = 8, [4] = 12, [5] = 35, [6] = 117, [7] = 79, [8] = 0, [9] = 0, [10] = 0, [11] = 0, [12] = 0, [13] = 0, [14] = 2, [15] = 4, [16] = 0, [17] = 0, [18] = 0, [19] = 0, [20] = 0, [21] = 0, [22] = 0, [23] = 0 ]"
        self.log_event("rmw_subscription_init",
                      f'{{ rmw_subscription_handle = {rmw_subscription_handle}, gid = {gid} }}',
                      context_key)
    
    # Context registration methods (compatible with original model)
    def register_system_context(self) -> str:
        """Register system initialization context"""
        context = context_manager.register_component(
            "system_init", "system", "system"
        )
        return "system_init"
    
    def register_node_context(self, node_name: str, process_name: str = None) -> str:
        """Register execution context for a ROS2 node"""
        context_key = f"node_{node_name}"
        context_manager.register_component(
            context_key, "node", process_name or node_name
        )
        return context_key
    
    def register_publisher_context(self, node_name: str, topic_name: str) -> str:
        """Register execution context for a publisher"""
        context_key = f"pub_{node_name}_{topic_name}"
        context_manager.register_component(
            context_key, "publisher", node_name
        )
        return context_key
    
    def register_subscriber_context(self, node_name: str, topic_name: str) -> str:
        """Register execution context for a subscriber"""
        context_key = f"sub_{node_name}_{topic_name}"
        context_manager.register_component(
            context_key, "subscriber", node_name
        )
        return context_key
    
    def register_timer_context(self, node_name: str, timer_name: str) -> str:
        """Register execution context for a timer"""
        context_key = f"timer_{node_name}_{timer_name}"
        context_manager.register_component(
            context_key, "timer", node_name
        )
        return context_key
    
    def register_middleware_context(self, component_name: str, layer: str = "rmw") -> str:
        """Register execution context for middleware components"""
        context_key = f"{layer}_{component_name}"
        context_manager.register_component(
            context_key, layer, "system"
        )
        return context_key
    
    def register_executor_context(self, executor_name: str = "main_executor") -> str:
        """Register execution context for executor"""
        context_key = f"executor_{executor_name}"
        context_manager.register_component(
            context_key, "executor", f"{executor_name}_executor"
        )
        return context_key
    
    # Utility methods
    def get_events(self, start_time: Optional[float] = None,
                  end_time: Optional[float] = None) -> List[ROS2TraceEvent]:
        """Get events in time range"""
        if start_time is None:
            start_time = self.start_time
        if end_time is None:
            end_time = time.time()
            
        return [e for e in self.events 
                if start_time <= e.timestamp <= end_time]
                
    def get_events_by_name(self, event_name: str) -> List[ROS2TraceEvent]:
        """Get events by name"""
        return [e for e in self.events if e.event_name == event_name]
        
    def get_events_by_context(self, context_key: str) -> List[ROS2TraceEvent]:
        """Get events by context"""
        return [e for e in self.events if e.context_key == context_key]
        
    def clear(self):
        """Clear all events"""
        self.events.clear()
        self.start_time = time.time()
        self.last_timestamp = 0.0
        
    def save_traces(self, filename: str = None):
        """Save traces in ROS2-compatible format"""
        if filename is None:
            filename = self.file_path
            
        with open(filename, 'w') as f:
            for event in self.events:
                current_time = event.timestamp - self.start_time
                delta = self._calculate_delta(current_time)
                f.write(event.to_ros2_format(delta) + '\n')
                
    def save_json(self, file_path: str):
        """Save traces in JSON format for analysis"""
        data = {
            'metadata': {
                'start_time': self.start_time,
                'duration': time.time() - self.start_time,
                'total_events': len(self.events),
                'format': self.format.value
            },
            'events': [e.to_json() for e in self.events],
            'statistics': self.get_statistics()
        }
        
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
            
    def get_statistics(self) -> Dict[str, Any]:
        """Get trace statistics"""
        if not self.events:
            return {
                'total_events': 0,
                'duration': 0,
                'events_per_second': 0
            }
            
        start = min(e.timestamp for e in self.events)
        end = max(e.timestamp for e in self.events)
        duration = end - start
        
        # Count event types
        event_counts = {}
        for event in self.events:
            event_counts[event.event_name] = event_counts.get(event.event_name, 0) + 1
        
        return {
            'total_events': len(self.events),
            'duration': duration,
            'events_per_second': len(self.events) / duration if duration > 0 else 0,
            'event_types': len(set(e.event_name for e in self.events)),
            'contexts': len(set(e.context_key for e in self.events)),
            'event_counts': event_counts
        }


# Global trace logger instance
trace_logger = ROS2TraceLogger()