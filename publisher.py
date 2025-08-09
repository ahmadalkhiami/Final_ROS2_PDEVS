"""
Application-level publisher implementation.
"""

from pypdevs.DEVS import AtomicDEVS
from pypdevs.infinity import INFINITY
import random
import time
from typing import Optional, Dict, Any

from message import Message, MessageType, MessageHeader
from policies import QoSProfile
from tracing import trace_logger
from context import context_manager


class Publisher(AtomicDEVS):
    """
    Application-level publisher that generates and publishes messages.
    """
    
    def __init__(self, name: str, node_name: str, topic: str, 
                 qos_profile: Optional[QoSProfile] = None,
                 publish_rate_hz: float = 10.0,
                 message_generator: Optional[callable] = None):
        AtomicDEVS.__init__(self, name)
        
        # Configuration
        self.node_name = node_name
        self.topic = topic
        self.qos_profile = qos_profile or QoSProfile()
        self.publish_period = 1.0 / publish_rate_hz
        self.message_generator = message_generator or self._default_message_generator
        
        # State
        self.state = {
            'phase': 'initializing',
            'initialized': False,
            'message_counter': 0,
            'publisher_handle': None,
            'last_publish_time': 0.0,
            'active': True
        }
        
        # Ports
        self.rclcpp_out = self.addOutPort("to_rclcpp")
        self.control_in = self.addInPort("control")
        
        # Register context
        self.context_key = context_manager.register_component(
            f"pub_{node_name}_{topic}",
            "publisher",
            node_name
        )
        
    def __lt__(self, other):
        """Compare publishers by name for DEVS simulator"""
        return self.name < other.name

    def timeAdvance(self):
        if self.state['phase'] == 'initializing':
            return 0.1  # Wait for initialization
            
        elif self.state['phase'] == 'ready' and self.state['active']:
            # Calculate next publish time
            time_since_last = time.time() - self.state['last_publish_time']
            if time_since_last >= self.publish_period:
                return 0.0  # Publish immediately
            else:
                return self.publish_period - time_since_last
                
        return INFINITY
        
    def outputFnc(self):
        if self.state['phase'] == 'initializing' and not self.state['initialized']:
            # Request publisher creation
            trace_logger.log_event(
                "app_publisher_init",
                {
                    "node": self.node_name,
                    "topic": self.topic,
                    "rate_hz": 1.0 / self.publish_period
                },
                self.context_key
            )
            
            return {self.rclcpp_out: {
                'type': 'create_publisher',
                'node_name': self.node_name,
                'topic': self.topic,
                'qos': self.qos_profile
            }}
            
        elif self.state['phase'] == 'ready' and self.state['active']:
            # Generate and publish message
            msg_data = self.message_generator(self.state['message_counter'])
            
            msg = Message(
                header=MessageHeader(),
                message_type=MessageType.DATA,
                topic=self.topic,
                data=msg_data
            )
            
            msg.mark_published()
            self.state['message_counter'] += 1
            self.state['last_publish_time'] = time.time()
            
            trace_logger.log_event(
                "app_publish",
                {
                    "message_id": msg.id,
                    "topic": self.topic,
                    "sequence": self.state['message_counter']
                },
                self.context_key
            )
            
            return {self.rclcpp_out: {
                'type': 'publish',
                'publisher_handle': self.state['publisher_handle'],
                'message': msg
            }}
            
        return {}
        
    def intTransition(self):
        if self.state['phase'] == 'initializing':
            self.state['initialized'] = True
            self.state['phase'] = 'ready'
            self.state['last_publish_time'] = time.time()
            
        return self.state
        
    def extTransition(self, inputs):
        # Handle control commands
        if self.control_in in inputs:
            cmd = inputs[self.control_in]
            if isinstance(cmd, dict):
                if cmd.get('command') == 'start':
                    self.state['active'] = True
                elif cmd.get('command') == 'stop':
                    self.state['active'] = False
                elif cmd.get('command') == 'set_rate':
                    self.publish_period = 1.0 / cmd['rate_hz']
                elif cmd.get('type') == 'publisher_created':
                    self.state['publisher_handle'] = cmd.get('publisher_handle')
                    
        return self.state
        
    def _default_message_generator(self, sequence: int) -> Dict[str, Any]:
        """Default message generator"""
        return {
            'sequence': sequence,
            'timestamp': time.time(),
            'data': f"Hello from {self.node_name} #{sequence}"
        }
        
    def set_message_generator(self, generator: callable):
        """Set custom message generator"""
        self.message_generator = generator
        
    def get_statistics(self) -> Dict[str, Any]:
        """Get publisher statistics"""
        return {
            'messages_published': self.state['message_counter'],
            'active': self.state['active'],
            'publish_rate_hz': 1.0 / self.publish_period,
            'last_publish_time': self.state.get('last_publish_time', 0.0)
        }


class ImagePublisher(Publisher):
    """Specialized publisher for image data"""
    
    def __init__(self, name: str, node_name: str, topic: str = "/image_raw",
                 width: int = 640, height: int = 480, fps: float = 30.0):
        super().__init__(name, node_name, topic, publish_rate_hz=fps)
        self.width = width
        self.height = height
        
    def _default_message_generator(self, sequence: int) -> Dict[str, Any]:
        """Generate image message"""
        # Simulate image data
        image_size = self.width * self.height * 3  # RGB
        
        return {
            'header': {
                'sequence': sequence,
                'timestamp': time.time(),
                'frame_id': 'camera_frame'
            },
            'height': self.height,
            'width': self.width,
            'encoding': 'rgb8',
            'is_bigendian': False,
            'step': self.width * 3,
            'data_size': image_size,  # Simulate data without actual bytes
            'data': [0] * image_size  # Simulate image data as list of zeros
        }


class PointCloudPublisher(Publisher):
    """Specialized publisher for point cloud data"""
    
    def __init__(self, name: str, node_name: str, topic: str = "/points",
                 points_per_scan: int = 65536, scan_rate_hz: float = 10.0):
        super().__init__(name, node_name, topic, publish_rate_hz=scan_rate_hz)
        self.points_per_scan = points_per_scan
        
    def _default_message_generator(self, sequence: int) -> Dict[str, Any]:
        """Generate point cloud message"""
        return {
            'header': {
                'sequence': sequence,
                'timestamp': time.time(),
                'frame_id': 'lidar_frame'
            },
            'height': 1,
            'width': self.points_per_scan,
            'fields': ['x', 'y', 'z', 'intensity'],
            'is_bigendian': False,
            'point_step': 16,  # 4 floats * 4 bytes
            'row_step': self.points_per_scan * 16,
            'is_dense': True,
            'data_size': self.points_per_scan * 16,
            'data': [0.0] * (self.points_per_scan * 4)  # Simulate point cloud data
        }