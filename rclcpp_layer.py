"""
RCLCPP layer implementation.
Provides C++ client library functionality.
"""

from pypdevs.DEVS import AtomicDEVS
from pypdevs.infinity import INFINITY
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
import random

from tracing import trace_logger
from context import context_manager
from configuration import config
from policies import QoSProfile

@dataclass
class PublisherInfo:
    """Information about a publisher"""
    node_name: str = ""
    topic: str = ""
    qos_profile: Optional[QoSProfile] = None
    handle: Optional[int] = None


@dataclass
class SubscriptionInfo:
    """Information about a subscription"""
    node_name: str = ""
    topic: str = ""
    qos_profile: Optional[QoSProfile] = None
    callback: Optional[Callable] = None
    handle: Optional[int] = None


@dataclass
class RCLCPPInterface:
    """Interface for RCLCPP layer operations"""
    node_name: str = ""
    interface_type: str = ""  # 'publisher', 'subscription', 'service', 'client'
    topic_or_service: str = ""
    qos_profile: Optional[QoSProfile] = None
    callback: Optional[Callable] = None
    handle: Optional[int] = None


class RCLCPPLayer(AtomicDEVS):
    """
    RCLCPP Layer - C++ client library interface.
    Manages high-level ROS2 operations.
    """
    
    def __init__(self, name: str = "RCLCPPLayer"):
        AtomicDEVS.__init__(self, name)
        
        # State
        self.state = {
            'phase': 'idle',
            'initialized': False,
            'nodes': {},  # node_name -> node_info
            'pending_operations': [],
            'executor_active': False,
            'pending_publishers': [],  # Publishers waiting for node handle
            'pending_subscriptions': [],  # Subscriptions waiting for node handle
            'rcl_initialized': False
        }
        
        # Ports - Application interface
        self.app_pub_in = self.addInPort("app_pub_in")
        self.app_sub_out = self.addOutPort("app_sub_out")
        
        # Ports - RCL interface  
        self.rcl_cmd_out = self.addOutPort("rcl_cmd_out")
        self.rcl_data_in = self.addInPort("rcl_data_in")
        # Waitset/executor work in from RCL
        self.exec_work_in = self.addInPort("exec_work_in")
        # Executor completion events
        self.exec_complete_in = self.addInPort("exec_complete_in")
        
        # Graph discovery port
        self.graph_event_in = self.addInPort("graph_event_in")
        
        # Register context
        self.context_key = context_manager.register_component(
            "rclcpp_layer",
            "rclcpp",
            "ros2_core"
        )
        
    def __lt__(self, other):
        """Compare layers by name for DEVS simulator"""
        return self.name < other.name
        
    def timeAdvance(self):
        if self.state['phase'] == 'idle' and not self.state['initialized']:
            return 0.01
            
        elif self.state['pending_operations']:
            # Process operations with minimal delay
            return 0.0001
            
        elif self.state['executor_active']:
            # Executor spin period
            return config.executor.spin_period_us / 1e6
            
        return INFINITY
        
    def outputFnc(self):
        if self.state['phase'] == 'idle' and not self.state['initialized']:
            trace_logger.log_event("rclcpp_init", {}, self.context_key)
            if not self.state['rcl_initialized']:
                trace_logger.log_event("rcl_init", {"version": "sim"}, self.context_key)
                self.state['rcl_initialized'] = True
            
        elif self.state['pending_operations']:
            op = self.state['pending_operations'][0]
            
            # Process based on operation type
            if op['type'] == 'create_node':
                return self._create_node(op)
                
            elif op['type'] == 'create_publisher':
                # Create publisher immediately; create node record on demand
                return self._create_publisher(op)
                
            elif op['type'] == 'create_subscription':
                # Create subscription immediately; create node record on demand
                return self._create_subscription(op)
                
            elif op['type'] == 'publish':
                return self._publish(op)
                
        elif self.state['executor_active']:
            # Drive pending app deliveries (deliver_to_app operations)
            for i in range(len(self.state['pending_operations'])):
                if self.state['pending_operations'][i].get('type') == 'deliver_to_app':
                    msg = self.state['pending_operations'][i]['message']
                    return {self.app_sub_out: msg}
            # Otherwise emit spin event
            trace_logger.log_event(
                "rclcpp_executor_spin_some",
                {"nodes": len(self.state['nodes'])},
                self.context_key
            )
            
        return {}
        
    def intTransition(self):
        if self.state['phase'] == 'idle' and not self.state['initialized']:
            self.state['initialized'] = True
            self.state['executor_active'] = True
            
        elif self.state['pending_operations']:
            self.state['pending_operations'].pop(0)
            
        return self.state
        
    def extTransition(self, inputs):
        # Handle application publisher input
        if self.app_pub_in in inputs:
            app_msg = inputs[self.app_pub_in]
            if isinstance(app_msg, dict):
                # Auto-resolve publisher_handle for publish ops if missing
                if app_msg.get('type') == 'publish' and not app_msg.get('publisher_handle'):
                    node_name = app_msg.get('node_name')
                    topic = getattr(app_msg.get('message'), 'topic', None) or app_msg.get('topic')
                    if node_name and topic and node_name in self.state['nodes']:
                        nh = self.state['nodes'][node_name]
                        pub = nh['publishers'].get(topic)
                        if pub and pub.get('handle'):
                            app_msg['publisher_handle'] = pub['handle']
                self.state['pending_operations'].append(app_msg)
                
        # Handle RCL data input
        if self.rcl_data_in in inputs:
            rcl_data = inputs[self.rcl_data_in]
            self._handle_rcl_data(rcl_data)
            
        # Handle graph events
        if self.graph_event_in in inputs:
            graph_event = inputs[self.graph_event_in]
            self._handle_graph_event(graph_event)

        # Handle executor work items from RCL
        if self.exec_work_in in inputs:
            work = inputs[self.exec_work_in]
            # For subscription work, forward to app_sub_out immediately (simulate executor delivery)
            if isinstance(work, dict) and work.get('type') == 'subscription' and work.get('message') is not None:
                return {
                    'state': self.state,
                    self.app_sub_out: work.get('message')
                }

        # Handle executor completion events
        if self.exec_complete_in in inputs:
            result = inputs[self.exec_complete_in]
            if isinstance(result, dict):
                # Log a completion event for richer tracing
                fields = {
                    'handle': f"0x{result.get('handle', 0):X}"
                }
                if 'message_id' in result:
                    fields['message_id'] = result['message_id']
                trace_logger.log_event(
                    "rclcpp_executor_callback_complete",
                    fields,
                    self.context_key
                )
            
        return self.state
        
    def _create_node(self, op: Dict) -> Dict:
        """Create a node"""
        node_name = op['node_name']
        
        if node_name not in self.state['nodes']:
            self.state['nodes'][node_name] = {
                'publishers': {},
                'subscriptions': {},
                'services': {},
                'timers': {}
            }
            
        trace_logger.log_event(
            "rclcpp_node_init",
            {"node_name": node_name},
            self.context_key
        )
        # Emit rcl node init too for parity
        trace_logger.log_event(
            "rcl_node_init",
            {"node_name": node_name, "namespace": "/"},
            self.context_key
        )
        
        # Forward to RCL
        return {self.rcl_cmd_out: {
            'type': 'create_node',
            'node_name': node_name
        }}
        
    def _create_publisher(self, op: Dict) -> Dict:
        """Create a publisher"""
        node_name = op['node_name']
        topic = op['topic']
        if node_name not in self.state['nodes']:
            self.state['nodes'][node_name] = {
                'publishers': {}, 'subscriptions': {}, 'services': {}, 'timers': {}
            }
        node_info = self.state['nodes'][node_name]
        
        # Register publisher
        node_info['publishers'][topic] = {
            'qos': op.get('qos'),
            'handle': None
        }
        
        trace_logger.log_event(
            "rclcpp_publisher_init",
            {
                "node_name": node_name,
                "topic": topic
            },
            self.context_key
        )
        # rcl layer publisher init
        trace_logger.log_event(
            "rcl_publisher_init",
            {
                "topic_name": topic
            },
            self.context_key
        )
        
        # Forward to RCL
        return {self.rcl_cmd_out: {
            'type': 'create_publisher',
            'node_handle': node_info.get('handle'),
            'topic': topic,
            'qos': op.get('qos')
        }}
        
    def _create_subscription(self, op: Dict) -> Dict:
        """Create a subscription"""
        node_name = op['node_name']
        topic = op['topic']
        if node_name not in self.state['nodes']:
            self.state['nodes'][node_name] = {
                'publishers': {}, 'subscriptions': {}, 'services': {}, 'timers': {}
            }
        node_info = self.state['nodes'][node_name]
        
        # Register subscription
        node_info['subscriptions'][topic] = {
            'qos': op.get('qos'),
            'callback': op.get('callback'),
            'handle': None
        }
        
        trace_logger.log_event(
            "rclcpp_subscription_init",
            {
                "node_name": node_name,
                "topic": topic
            },
            self.context_key
        )
        # rclcpp registers callback symbol
        cb = op.get('callback')
        if cb is not None:
            trace_logger.log_event(
                "rclcpp_subscription_callback_added",
                {"topic": topic},
                self.context_key
            )
            trace_logger.log_event(
                "rclcpp_callback_register",
                {"symbol": str(cb)},
                self.context_key
            )
        # rcl layer subscription init
        trace_logger.log_event(
            "rcl_subscription_init",
            {"topic_name": topic},
            self.context_key
        )
        
        # Forward to RCL
        return {self.rcl_cmd_out: {
            'type': 'create_subscription',
            'node_handle': node_info.get('handle'),
            'topic': topic,
            'qos': op.get('qos'),
            'callback': op.get('callback')
        }}
        
    def _publish(self, op: Dict) -> Dict:
        """Publish a message"""
        message = op['message']
        publisher_handle = op['publisher_handle']
        
        trace_logger.log_event(
            "rclcpp_publish",
            {
                "message_id": message.id,
                "topic": message.topic
            },
            self.context_key
        )
        # rcl layer publish
        trace_logger.log_event(
            "rcl_publish",
            {
                "message_id": message.id
            },
            self.context_key
        )
        
        # Forward to RCL
        return {self.rcl_cmd_out: {
            'type': 'publish',
            'publisher_handle': publisher_handle,
            'message': message
        }}
        
    def _handle_rcl_data(self, data: Dict):
        """Handle data from RCL layer"""
        if data.get('type') == 'node_created':
            # Store node handle
            node_name = data['node_name']
            node_handle = data['node_handle']
            if node_name in self.state['nodes']:
                self.state['nodes'][node_name]['handle'] = node_handle
                
                # Process pending publishers for this node
                for pub in self.state['pending_publishers'][:]:
                    if pub['node_name'] == node_name:
                        self.state['pending_operations'].append(pub)
                        self.state['pending_publishers'].remove(pub)
                        
                # Process pending subscriptions for this node
                for sub in self.state['pending_subscriptions'][:]:
                    if sub['node_name'] == node_name:
                        self.state['pending_operations'].append(sub)
                        self.state['pending_subscriptions'].remove(sub)
                        
        elif data.get('type') == 'publisher_created':
            # Store publisher handle and forward to application
            publisher_handle = data['publisher_handle']
            topic = data['topic']
            
            # Find the node that owns this publisher
            for node_name, node_info in self.state['nodes'].items():
                if topic in node_info['publishers']:
                    node_info['publishers'][topic]['handle'] = publisher_handle
                    # Forward to application
                    self.state['pending_operations'].append({
                        'type': 'publisher_created',
                        'publisher_handle': publisher_handle,
                        'topic': topic
                    })
                    break
                        
        elif data.get('type') == 'message_delivery':
            # Deliver message to application
            message = data['message']
            
            trace_logger.log_event(
                "rclcpp_take",
                {"message_id": message.id, "topic": message.topic},
                self.context_key
            )
            
            # Send to application subscriber(s)
            # Fan-out to all subscribers of this topic
            delivered = False
            for node_name, node_info in self.state['nodes'].items():
                if message.topic in node_info['subscriptions']:
                    # deliver to application through app_sub_out
                    self.state['pending_operations'].append({
                        'type': 'deliver_to_app',
                        'message': message
                    })
                    delivered = True
            # If no local subscriber in this rclcpp, still emit deliver_to_app once
            if not delivered:
                self.state['pending_operations'].append({
                    'type': 'deliver_to_app', 'message': message
                })
            
    def _handle_graph_event(self, event: Dict):
        """Handle graph discovery event"""
        trace_logger.log_event(
            "rclcpp_graph_event",
            {
                "event_type": event.get('event_type', 'unknown'),
                "entity": event.get('entity_name', 'unknown')
            },
            self.context_key
        )