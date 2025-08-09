"""
Application-level subscriber implementation.
"""

from pypdevs.DEVS import AtomicDEVS
from pypdevs.infinity import INFINITY
import time
from typing import Optional, Dict, Any, List, Callable
from collections import deque

from dataTypes import Message, QoSProfile
from tracing import trace_logger
from context import context_manager
from configuration import config


class Subscriber(AtomicDEVS):
    """
    Application-level subscriber that receives and processes messages.
    """
    
    def __init__(self, name: str, node_name: str, topic: str,
                 qos_profile: Optional[QoSProfile] = None,
                 callback: Optional[Callable[[Message], None]] = None,
                 queue_size: int = 10):
        AtomicDEVS.__init__(self, name)
        
        # Configuration
        self.node_name = node_name
        self.topic = topic
        self.qos_profile = qos_profile or QoSProfile()
        self.callback = callback or self._default_callback
        self.queue_size = queue_size
        
        # State
        self.state = {
            'phase': 'initializing',
            'initialized': False,
            'subscription_handle': None,
            'message_queue': deque(maxlen=queue_size),
            'current_message': None,
            'message_count': 0,
            'processing_time_sum': 0.0,
            'active': True
        }
        
        # Ports
        self.rclcpp_out = self.addOutPort("to_rclcpp")
        self.rclcpp_in = self.addInPort("from_rclcpp")
        self.control_in = self.addInPort("control")
        
        # Register context
        self.context_key = context_manager.register_component(
            f"sub_{node_name}_{topic}",
            "subscriber",
            node_name
        )
        
    def __lt__(self, other):
        """Compare subscribers by name for DEVS simulator"""
        return self.name < other.name
        
    def timeAdvance(self):
        if self.state['phase'] == 'initializing':
            return 0.1
            
        elif self.state['phase'] == 'processing':
            # Simulate callback processing time
            processing_time = self._estimate_processing_time()
            return processing_time
            
        elif self.state['phase'] == 'ready' and self.state['message_queue']:
            return 0.0  # Process immediately
            
        return INFINITY
        
    def outputFnc(self):
        if self.state['phase'] == 'initializing' and not self.state['initialized']:
            # Request subscription creation
            trace_logger.log_event(
                "app_subscriber_init",
                {
                    "node": self.node_name,
                    "topic": self.topic,
                    "queue_size": self.queue_size
                },
                self.context_key
            )
            
            return {self.rclcpp_out: {
                'type': 'create_subscription',
                'node_name': self.node_name,
                'topic': self.topic,
                'qos': self.qos_profile,
                'callback': self._on_message_received
            }}
            
        elif self.state['phase'] == 'processing' and self.state['current_message']:
            # Log callback completion
            msg = self.state['current_message']
            
            trace_logger.log_event(
                "callback_end",
                {
                    "message_id": msg.id,
                    "processing_time_ms": self._estimate_processing_time() * 1000
                },
                self.context_key
            )
            
        return {}
        
    def intTransition(self):
        if self.state['phase'] == 'initializing':
            self.state['initialized'] = True
            self.state['phase'] = 'ready'
            
        elif self.state['phase'] == 'processing':
            # Update statistics
            processing_time = self._estimate_processing_time()
            self.state['processing_time_sum'] += processing_time
            
            self.state['current_message'] = None
            self.state['phase'] = 'ready'
            
        elif self.state['phase'] == 'ready' and self.state['message_queue']:
            # Start processing next message
            self.state['current_message'] = self.state['message_queue'].popleft()
            self.state['phase'] = 'processing'
            
            trace_logger.log_event(
                "callback_start",
                {
                    "message_id": self.state['current_message'].id,
                    "queue_depth": len(self.state['message_queue'])
                },
                self.context_key
            )
            
            # Execute callback
            try:
                self.callback(self.state['current_message'])
            except Exception as e:
                trace_logger.log_event(
                    "app_callback_error",
                    {
                        "message_id": self.state['current_message'].id,
                        "error": str(e)
                    },
                    self.context_key
                )
                
        return self.state
        
    def extTransition(self, inputs):
        # Handle messages from RCLCPP
        if self.rclcpp_in in inputs:
            data = inputs[self.rclcpp_in]
            if isinstance(data, Message) and self.state['active']:
                self._on_message_received(data)
                
        # Handle control commands
        if self.control_in in inputs:
            cmd = inputs[self.control_in]
            if isinstance(cmd, dict):
                if cmd.get('command') == 'start':
                    self.state['active'] = True
                elif cmd.get('command') == 'stop':
                    self.state['active'] = False
                elif cmd.get('command') == 'clear_queue':
                    self.state['message_queue'].clear()
                    
        return self.state
        
    def _on_message_received(self, msg: Message):
        """Handle received message"""
        if self.state['active']:
            self.state['message_queue'].append(msg)
            self.state['message_count'] += 1
            
            # Check for dropped messages due to queue overflow
            if len(self.state['message_queue']) == self.queue_size:
                trace_logger.log_event(
                    "message_dropped",
                    {
                        "topic": self.topic,
                        "reason": "queue_full",
                        "queue_size": self.queue_size
                    },
                    self.context_key
                )
                
    def _default_callback(self, msg: Message):
        """Default message callback"""
        # Simple logging callback
        trace_logger.log_event(
            "app_message_received",
            {
                "topic": msg.topic,
                "data": str(msg.data)[:100]  # Truncate for logging
            },
            self.context_key
        )
        
    def _estimate_processing_time(self) -> float:
        """Estimate callback processing time"""
        # Base processing time with some randomness
        base_time = 0.001  # 1ms base
        
        # Add complexity based on message type
        if self.state['current_message']:
            data = self.state['current_message'].data
            if isinstance(data, dict):
                # More complex for structured data
                if 'data_size' in data:
                    # Scale with data size
                    size_factor = data['data_size'] / 1000000  # MB
                    base_time += size_factor * 0.001
                    
        # Add system load factor
        load_factor = 1.0 + config.system_state.cpu_load
        
        return base_time * load_factor
        
    def get_statistics(self) -> Dict[str, Any]:
        """Get subscriber statistics"""
        avg_processing_time = 0.0
        if self.state['message_count'] > 0:
            avg_processing_time = self.state['processing_time_sum'] / self.state['message_count']
            
        return {
            'messages_received': self.state['message_count'],
            'queue_depth': len(self.state['message_queue']),
            'average_processing_time_ms': avg_processing_time * 1000,
            'active': self.state['active']
        }


class ImageSubscriber(Subscriber):
    """Specialized subscriber for image data"""
    
    def __init__(self, name: str, node_name: str, topic: str = "/image_raw",
                 process_every_n: int = 1):
        super().__init__(name, node_name, topic)
        self.process_every_n = process_every_n
        self.frame_counter = 0
        
    def _default_callback(self, msg: Message):
        """Process image message"""
        self.frame_counter += 1
        
        # Only process every N frames
        if self.frame_counter % self.process_every_n == 0:
            data = msg.data
            if isinstance(data, dict) and 'width' in data and 'height' in data:
                trace_logger.log_event(
                    "app_image_processed",
                    {
                        "frame": self.frame_counter,
                        "resolution": f"{data['width']}x{data['height']}",
                        "encoding": data.get('encoding', 'unknown')
                    },
                    self.context_key
                )


class SynchronizedSubscriber(AtomicDEVS):
    """
    Subscriber that synchronizes messages from multiple topics.
    Implements message filters like in ROS2.
    """
    
    def __init__(self, name: str, node_name: str, topics: List[str],
                 sync_callback: Optional[Callable] = None,
                 queue_size: int = 10,
                 slop: float = 0.1):  # Time tolerance in seconds
        AtomicDEVS.__init__(self, name)
        
        self.node_name = node_name
        self.topics = topics
        self.sync_callback = sync_callback or self._default_sync_callback
        self.queue_size = queue_size
        self.slop = slop
        
        # State
        self.state = {
            'phase': 'initializing',
            'initialized': False,
            'topic_queues': {topic: deque(maxlen=queue_size) for topic in topics},
            'sync_count': 0
        }
        
        # Ports
        self.rclcpp_out = self.addOutPort("to_rclcpp")
        self.rclcpp_in = self.addInPort("from_rclcpp")
        
        # Register context
        self.context_key = context_manager.register_component(
            f"sync_sub_{node_name}",
            "synchronized_subscriber",
            node_name
        )
        
    def timeAdvance(self):
        if self.state['phase'] == 'initializing':
            return 0.1
        elif self.state['phase'] == 'ready' and self._can_synchronize():
            return 0.001  # Process synchronized messages
        return INFINITY
        
    def outputFnc(self):
        if self.state['phase'] == 'initializing' and not self.state['initialized']:
            # Create subscriptions for all topics
            requests = []
            for topic in self.topics:
                requests.append({
                    'type': 'create_subscription',
                    'node_name': self.node_name,
                    'topic': topic,
                    'callback': lambda msg, t=topic: self._on_message(msg, t)
                })
            
            # Return first request (others will be queued)
            if requests:
                return {self.rclcpp_out: requests[0]}
                
        return {}
        
    def intTransition(self):
        if self.state['phase'] == 'initializing':
            self.state['initialized'] = True
            self.state['phase'] = 'ready'
            
        elif self.state['phase'] == 'ready' and self._can_synchronize():
            # Get synchronized messages
            sync_msgs = self._get_synchronized_messages()
            if sync_msgs:
                self.sync_callback(sync_msgs)
                self.state['sync_count'] += 1
                
        return self.state
        
    def _on_message(self, msg: Message, topic: str):
        """Handle message for a specific topic"""
        if topic in self.state['topic_queues']:
            self.state['topic_queues'][topic].append(msg)
            
    def _can_synchronize(self) -> bool:
        """Check if we have messages in all queues that can be synchronized"""
        return all(len(queue) > 0 for queue in self.state['topic_queues'].values())
        
    def _get_synchronized_messages(self) -> Optional[Dict[str, Message]]:
        """Get synchronized messages within time tolerance"""
        if not self._can_synchronize():
            return None
            
        # Get oldest message from each queue
        candidates = {}
        for topic, queue in self.state['topic_queues'].items():
            if queue:
                candidates[topic] = queue[0]
                
        # Check if all messages are within time tolerance
        timestamps = [msg.published_time for msg in candidates.values()]
        if max(timestamps) - min(timestamps) <= self.slop:
            # Remove synchronized messages from queues
            for topic in self.topics:
                self.state['topic_queues'][topic].popleft()
            return candidates
            
        # Remove oldest message and try again
        oldest_topic = min(candidates.keys(), 
                          key=lambda t: candidates[t].published_time)
        self.state['topic_queues'][oldest_topic].popleft()
        
        return None
        
    def _default_sync_callback(self, messages: Dict[str, Message]):
        """Default synchronized callback"""
        trace_logger.log_event(
            "app_synchronized_callback",
            {
                "topics": list(messages.keys()),
                "sync_count": self.state['sync_count']
            },
            self.context_key
        )