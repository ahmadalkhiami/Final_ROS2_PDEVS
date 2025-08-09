"""
RMW (ROS Middleware) layer implementation.
Interfaces between RCL and DDS layers.
"""

from pypdevs.DEVS import AtomicDEVS, CoupledDEVS
from pypdevs.infinity import INFINITY
import random
import time
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass
import uuid
from dataTypes import Message, QoSDurabilityPolicy, QoSReliabilityPolicy, RMWQoSProfile, QoSHistoryPolicy
from policies import QoSProfile as DDSQoSProfile, QoSReliabilityPolicy as DDSReliability, QoSDurabilityPolicy as DDSDurability, QoSHistoryPolicy as DDSHistory
from participant import DDSParticipant
from transport import TransportMultiplexer
from tracing import trace_logger
from context import context_manager
from configuration import config
from transport import TransportMessage, TransportType


@dataclass
class RMWPublisher:
    """RMW publisher implementation"""
    handle: int
    topic: str
    type_name: str
    qos: RMWQoSProfile
    dds_writer_guid: Optional[str] = None
    node_name: str = ""


@dataclass
class RMWSubscription:
    """RMW subscription implementation"""
    handle: int
    topic: str
    type_name: str
    qos: RMWQoSProfile
    dds_reader_guid: Optional[str] = None
    node_name: str = ""
    callback: Optional[callable] = None


class RMWLayer(CoupledDEVS):
    """
    RMW Layer - Interfaces between RCL and DDS.
    This is now a coupled DEVS containing RMW implementation and DDS participant.
    """
    
    def __init__(self, name: str = "RMWLayer"):
        CoupledDEVS.__init__(self, name)
        
        # Create DDS participant
        self.dds_participant = self.addSubModel(
            DDSParticipant("RMWParticipant", config.dds.domain_id)
        )
        
        # Create RMW implementation
        self.rmw_impl = self.addSubModel(RMWImplementation())
        # Wire direct reference so RMWImplementation can call participant helpers
        self.rmw_impl.dds_participant = self.dds_participant

        # Add transport multiplexer to simulate network transports
        self.transport_mux = self.addSubModel(TransportMultiplexer())
        
        # External ports for RCL interface
        self.rcl_pub_in = self.addInPort("rcl_pub_in")
        self.rcl_sub_out = self.addOutPort("rcl_sub_out")
        self.graph_event_out = self.addOutPort("graph_event_out")
        
        # External ports for DDS interface
        self.dds_data_in = self.addInPort("dds_data_in")
        self.dds_data_out = self.addOutPort("dds_data_out")
        
        # Connect RMW to DDS through transport for outbound data
        self.connectPorts(self.rmw_impl.dds_out, self.dds_participant.data_in)
        # Participant outbound goes through transport multiplexer to RMW
        self.connectPorts(self.dds_participant.data_out, self.transport_mux.data_in)
        self.connectPorts(self.transport_mux.data_out, self.rmw_impl.dds_in)
        
        # Connect DDS participant to external ports
        self.connectPorts(self.dds_data_in, self.dds_participant.data_in)
        self.connectPorts(self.dds_participant.data_out, self.dds_data_out)
        
        # Connect external ports to RMW implementation
        self.connectPorts(self.rcl_pub_in, self.rmw_impl.rcl_pub_in)
        self.connectPorts(self.rmw_impl.rcl_sub_out, self.rcl_sub_out)
        self.connectPorts(self.rmw_impl.graph_event_out, self.graph_event_out)
        
    def __lt__(self, other):
        """Compare layers by name for DEVS simulator"""
        return self.name < other.name


class RMWImplementation(AtomicDEVS):
    """RMW implementation"""
    
    def __init__(self, name: str = "RMWImpl"):
        AtomicDEVS.__init__(self, name)
        
        # State
        self.state = {
            'phase': 'idle',
            'initialized': False,
            'publishers': {},  # handle -> RMWPublisher
            'subscriptions': {},  # handle -> RMWSubscription
            'pending_operations': [],
            'handle_counter': 1000
        }
        
        # Ports - RCL interface
        self.rcl_pub_in = self.addInPort("rcl_pub_in")
        self.rcl_sub_out = self.addOutPort("rcl_sub_out")
        
        # Ports - DDS interface
        self.dds_out = self.addOutPort("dds_out")
        self.dds_in = self.addInPort("dds_in")
        
        # Graph events
        self.graph_event_out = self.addOutPort("graph_event_out")
        
        # Register context
        self.context_key = context_manager.register_component(
            "rmw_impl",
            "rmw",
            "ros2_core"
        )
        
    def __lt__(self, other):
        """Compare implementations by name for DEVS simulator"""
        return self.name < other.name
        
    def timeAdvance(self):
        if self.state['phase'] == 'idle' and not self.state['initialized']:
            return 0.01
            
        elif self.state['pending_operations']:
            # Process operations with minimal delay
            return 0.0001
            
        return INFINITY
        
    def outputFnc(self):
        if self.state['phase'] == 'idle' and not self.state['initialized']:
            trace_logger.log_event(
                "rmw_init",
                {},
                self.context_key
            )
            
        elif self.state['pending_operations']:
            op = self.state['pending_operations'][0]
            
            # Process based on operation type
            if op['type'] == 'create_publisher':
                return self._create_publisher(op)
                
            elif op['type'] == 'create_subscription':
                return self._create_subscription(op)
                
            elif op['type'] == 'publish':
                return self._publish(op)
                
        return {}
        
    def intTransition(self):
        if self.state['phase'] == 'idle' and not self.state['initialized']:
            self.state['initialized'] = True
            
        elif self.state['pending_operations']:
            self.state['pending_operations'].pop(0)
            
        return self.state
        
    def extTransition(self, inputs):
        # Handle RCL commands
        if self.rcl_pub_in in inputs:
            rcl_cmd = inputs[self.rcl_pub_in]
            if isinstance(rcl_cmd, dict):
                self.state['pending_operations'].append(rcl_cmd)
                
        # Handle DDS responses/data
        if self.dds_in in inputs:
            dds_msg = inputs[self.dds_in]
            # Accept both response-style dicts and participant data dicts
            if isinstance(dds_msg, dict) and 'data' in dds_msg and 'topic' in dds_msg:
                self._handle_dds_data(dds_msg)
            else:
                self._handle_dds_response(dds_msg)
            
        return self.state
        
    def _create_publisher(self, op: Dict) -> Dict:
        """Create publisher"""
        handle = self._next_handle()
        
        # Create publisher info
        pub = RMWPublisher(
            handle=handle,
            topic=op['topic'],
            type_name=op.get('type_name', 'std_msgs/String'),
            qos=self._coerce_to_rmw_qos(op.get('qos')),
            node_name=op.get('node_name', '')
        )
        
        self.state['publishers'][handle] = pub
        
        # Create DDS writer
        writer = self.dds_participant.create_writer(
            pub.topic,
            pub.type_name,
            self._coerce_to_dds_qos(pub.qos)
        )
        pub.dds_writer_guid = writer.guid
        
        trace_logger.log_event(
            "rmw_publisher_init",
            {
                "handle": handle,
                "topic": pub.topic,
                "node": pub.node_name
            },
            self.context_key
        )
        trace_logger.log_event(
            "rmw_publisher_init",
            {
                "handle": handle,
                "topic": pub.topic,
                "node": pub.node_name
            },
            self.context_key
        )
        
        # Generate graph event
        self._generate_graph_event(
            "publisher_created",
            pub.topic,
            pub.node_name
        )
        
        return {}
        
    def _create_subscription(self, op: Dict) -> Dict:
        """Create subscription"""
        handle = self._next_handle()
        
        # Create subscription info
        sub = RMWSubscription(
            handle=handle,
            topic=op['topic'],
            type_name=op.get('type_name', 'std_msgs/String'),
            qos=self._coerce_to_rmw_qos(op.get('qos')),
            node_name=op.get('node_name', ''),
            callback=op.get('callback')
        )
        
        self.state['subscriptions'][handle] = sub
        
        # Create DDS reader
        reader = self.dds_participant.create_reader(
            sub.topic,
            sub.type_name,
            self._coerce_to_dds_qos(sub.qos),
            lambda msg: self._on_dds_data_available(sub, msg)
        )
        sub.dds_reader_guid = reader.guid
        
        trace_logger.log_event(
            "rmw_subscription_init",
            {
                "handle": handle,
                "topic": sub.topic,
                "node": sub.node_name
            },
            self.context_key
        )
        
        # Generate graph event
        self._generate_graph_event(
            "subscription_created",
            sub.topic,
            sub.node_name
        )
        
        return {}
        
    def _publish(self, op: Dict) -> Dict:
        """Publish message via canonical CDR serialization and DDS write"""
        msg = op['message']
        pub = self._find_publisher_for_topic(msg.topic)

        if not pub:
            return {}

        # Canonical serialization path: use CDR serializer for payload
        size_bytes = 0
        try:
            from serialization import type_registry
            # Default to core Message type if untyped; serialize the envelope
            payload = msg
            cdr = type_registry.get_type_support("message/Message")
            if cdr is not None:
                serialized = cdr.serialize(payload)
                size_bytes = len(serialized)
                # Attach serialized bytes for transport layers that care
                msg.serialized_data = serialized
        except Exception:
            # Fall back: retain msg as-is
            pass

        # Instruct DDS participant to write data through the local writer
        if hasattr(self, 'dds_participant') and pub.dds_writer_guid:
            self.dds_participant.write_data(pub.dds_writer_guid, msg)

        trace_logger.log_event(
            "rmw_publish",
            {
                "message_id": msg.id,
                "topic": msg.topic,
                "size": size_bytes
            },
            self.context_key
        )

        return {}
        
    def _find_publisher_for_topic(self, topic: str) -> Optional[RMWPublisher]:
        """Find publisher for topic"""
        for pub in self.state['publishers'].values():
            if pub.topic == topic:
                return pub
        return None
        
    def _handle_dds_response(self, response: Dict):
        """Handle response from DDS layer"""
        if response.get('type') == 'data':
            # Find matching subscription
            topic = response.get('topic')
            for sub in self.state['subscriptions'].values():
                if sub.topic == topic:
                    # Check QoS compatibility
                    msg = response['message']
                    compatible, reason = self._check_qos_delivery(msg, sub)
                    
                    if compatible:
                        # Deliver to subscription
                        self._on_dds_data_available(sub, msg)
                    else:
                        trace_logger.log_event(
                            "rmw_qos_incompatible",
                            {
                                "topic": topic,
                                "reason": reason
                            },
                            self.context_key
                        )
                        
    def _handle_dds_data(self, dds_msg: Dict):
        """Handle data from DDS layer (participant -> RMW shape)"""
        # Expected shape from DDSParticipant: {'writer_guid', 'sequence_number', 'topic', 'data', 'timestamp'}
        if isinstance(dds_msg, dict) and 'topic' in dds_msg and 'data' in dds_msg:
            topic = dds_msg['topic']
            data_msg = dds_msg['data']
            # Deliver to all matching subscriptions on topic
            for sub in self.state['subscriptions'].values():
                if sub.topic == topic:
                    self._on_dds_data_available(sub, data_msg)
                    
    def _on_dds_data_available(self, subscription: RMWSubscription, msg: Message):
        """Handle received DDS data"""
        trace_logger.log_event(
            "rmw_take",
            {
                "message_id": msg.id,
                "topic": subscription.topic
            },
            self.context_key
        )
        # rcl_take parity
        trace_logger.log_event(
            "rcl_take",
            {
                "message_id": msg.id,
                "topic": subscription.topic
            },
            self.context_key
        )
        
        # Invoke user subscription callback if provided
        if subscription.callback:
            try:
                subscription.callback(msg)
            except Exception as e:
                trace_logger.log_event(
                    "rmw_subscription_callback_error",
                    {"error": str(e), "topic": subscription.topic},
                    self.context_key
                )

        # Emit rcl_take event before RCL mapping for trace parity
        trace_logger.log_event(
            "rcl_take",
            {
                "message_id": msg.id,
                "topic": subscription.topic
            },
            self.context_key
        )

        # Forward to RCL
        return {self.rcl_sub_out: {
            'type': 'message_delivery',
            'subscription_handle': subscription.handle,
            'message': msg
        }}
        
    def _check_qos_delivery(self, msg: Message, sub: RMWSubscription) -> Tuple[bool, str]:
        """Check if message can be delivered based on QoS"""
        if not msg.qos_profile or not sub.qos:
            return True, ""
            
        # Check reliability
        if (sub.qos.reliability == QoSReliabilityPolicy.RELIABLE and
            msg.qos_profile.reliability != QoSReliabilityPolicy.RELIABLE):
            return False, "reliability mismatch"
            
        # Check durability
        if (sub.qos.durability == QoSDurabilityPolicy.TRANSIENT_LOCAL and
            msg.qos_profile.durability == QoSDurabilityPolicy.VOLATILE):
            return False, "durability mismatch"
            
        return True, ""
        
    def _generate_graph_event(self, event_type: str, topic: str, node_name: str):
        """Generate graph discovery event"""
        event = {
            'type': event_type,
            'topic': topic,
            'node': node_name,
            'timestamp': time.time()
        }
        return {self.graph_event_out: event}

    def _to_dds_qos(self, rmw_qos: RMWQoSProfile) -> DDSQoSProfile:
        """Convert RMW QoS (dataTypes) to DDS QoS (policies)."""
        # Map enums by value strings
        rel_value = rmw_qos.reliability.value if hasattr(rmw_qos.reliability, 'value') else str(rmw_qos.reliability)
        dur_value = rmw_qos.durability.value if hasattr(rmw_qos.durability, 'value') else str(rmw_qos.durability)
        hist_value = rmw_qos.history.value if hasattr(rmw_qos.history, 'value') else str(rmw_qos.history)

        rel_map = {
            'RELIABLE': DDSReliability.RELIABLE,
            'BEST_EFFORT': DDSReliability.BEST_EFFORT,
        }
        dur_map = {
            'VOLATILE': DDSDurability.VOLATILE,
            'TRANSIENT_LOCAL': DDSDurability.TRANSIENT_LOCAL,
            'TRANSIENT': DDSDurability.TRANSIENT,
            'PERSISTENT': DDSDurability.PERSISTENT,
        }
        hist_map = {
            'KEEP_LAST': DDSHistory.KEEP_LAST,
            'KEEP_ALL': DDSHistory.KEEP_ALL,
        }

        # Convert ms (RMW) to ns (DDS policies) where finite
        deadline_ns = None if rmw_qos.deadline_ms in (None, float('inf')) else int(rmw_qos.deadline_ms * 1e6)
        lifespan_ns = None if rmw_qos.lifespan_ms in (None, float('inf')) else int(rmw_qos.lifespan_ms * 1e6)

        return DDSQoSProfile(
            reliability=rel_map.get(rel_value, DDSReliability.RELIABLE),
            durability=dur_map.get(dur_value, DDSDurability.VOLATILE),
            history=hist_map.get(hist_value, DDSHistory.KEEP_LAST),
            depth=rmw_qos.depth,
            deadline=deadline_ns,
            lifespan=lifespan_ns,
            partition=[],
        )

    def _coerce_to_dds_qos(self, qos: Any) -> DDSQoSProfile:
        """Accept either RMWQoSProfile or DDS QoSProfile and return DDS QoSProfile."""
        if isinstance(qos, DDSQoSProfile):
            return qos
        if isinstance(qos, RMWQoSProfile):
            return self._to_dds_qos(qos)
        # Fallback default DDS QoS
        return DDSQoSProfile()

    def _coerce_to_rmw_qos(self, qos: Any) -> RMWQoSProfile:
        """Accept either DDS QoSProfile or RMWQoSProfile and return RMWQoSProfile."""
        if isinstance(qos, RMWQoSProfile):
            return qos
        if isinstance(qos, DDSQoSProfile):
            # Map policies enums to dataTypes enums
            rel_map = {
                'reliable': QoSReliabilityPolicy.RELIABLE,
                'best_effort': QoSReliabilityPolicy.BEST_EFFORT,
            }
            dur_map = {
                'volatile': QoSDurabilityPolicy.VOLATILE,
                'transient_local': QoSDurabilityPolicy.TRANSIENT_LOCAL,
                'transient': QoSDurabilityPolicy.TRANSIENT,
                'persistent': QoSDurabilityPolicy.PERSISTENT,
            }
            history_map = {
                'keep_last': QoSHistoryPolicy.KEEP_LAST,
                'keep_all': QoSHistoryPolicy.KEEP_ALL,
            }
            reliability = rel_map.get(qos.reliability.value if hasattr(qos.reliability, 'value') else str(qos.reliability), QoSReliabilityPolicy.RELIABLE)
            durability = dur_map.get(qos.durability.value if hasattr(qos.durability, 'value') else str(qos.durability), QoSDurabilityPolicy.VOLATILE)
            history = history_map.get(qos.history.value if hasattr(qos.history, 'value') else str(qos.history), QoSHistoryPolicy.KEEP_LAST)
            depth = getattr(qos, 'depth', 10)
            # policies QoS uses ns; convert to ms; None means infinite
            deadline_attr = getattr(qos, 'deadline', None)
            lifespan_attr = getattr(qos, 'lifespan', None)
            deadline_ms = float('inf') if (deadline_attr is None) else (deadline_attr / 1e6)
            lifespan_ms = float('inf') if (lifespan_attr is None) else (lifespan_attr / 1e6)
            return RMWQoSProfile(
                reliability=reliability,
                durability=durability,
                history=history,
                depth=depth,
                deadline_ms=deadline_ms,
                lifespan_ms=lifespan_ms
            )
        # Fallback default RMW QoS
        return RMWQoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,
            deadline_ms=float('inf'),
            lifespan_ms=float('inf')
        )
        
    def _next_handle(self) -> int:
        """Generate next handle"""
        handle = self.state['handle_counter']
        self.state['handle_counter'] += 1
        return handle
        
    def get_publisher_count(self, topic: str) -> int:
        """Get number of publishers for topic"""
        return sum(1 for p in self.state['publishers'].values() if p.topic == topic)
        
    def get_subscription_count(self, topic: str) -> int:
        """Get number of subscriptions for topic"""
        return sum(1 for s in self.state['subscriptions'].values() if s.topic == topic)