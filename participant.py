"""
DDS Participant implementation for ROS2 DEVS simulation.
Implements DDS domain participant with proper discovery.
"""

from pypdevs.DEVS import AtomicDEVS, CoupledDEVS
from pypdevs.infinity import INFINITY
import random
import time
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, field
import uuid

from message import Message
from tracing import trace_logger
from context import context_manager
from configuration import config
from discovery import ParticipantInfo, EndpointInfo, DiscoveryMessage
from policies import QoSProfile


@dataclass
class DDSEntity:
    """Base class for DDS entities"""
    guid: str = field(default_factory=lambda: str(uuid.uuid4()))
    qos: QoSProfile = field(default_factory=QoSProfile)
    
    
@dataclass
class DataWriter(DDSEntity):
    """DDS DataWriter"""
    topic_name: str = ""
    type_name: str = ""
    partition: List[str] = field(default_factory=list)
    
    
@dataclass
class DataReader(DDSEntity):
    """DDS DataReader"""
    topic_name: str = ""
    type_name: str = ""
    partition: List[str] = field(default_factory=list)
    callback: Optional[callable] = None


class DDSParticipant(AtomicDEVS):
    """
    DDS Domain Participant - Core DDS entity that manages discovery and endpoints.
    This fixes the missing DDS layer issue.
    """
    
    def __init__(self, name: str = "DDSParticipant", domain_id: int = 0):
        AtomicDEVS.__init__(self, name)
        
        # Identity
        self.domain_id = domain_id
        self.participant_guid = str(uuid.uuid4())
        self.participant_qos = QoSProfile()
        
        # State
        self.state = {
            'phase': 'initializing',
            'initialized': False,
            'discovered_participants': {},  # guid -> ParticipantInfo
            'discovered_endpoints': {},     # guid -> EndpointInfo
            'local_writers': {},           # topic -> List[DataWriter]
            'local_readers': {},           # topic -> List[DataReader]
            'matched_endpoints': {},       # local_guid -> Set[remote_guid]
            'pending_messages': [],
            'pending_discoveries': [],
            'last_discovery_time': 0.0,
            'last_heartbeat_time': 0.0,
            'sequence_numbers': {}         # writer_guid -> sequence_number
        }
        
        # Ports
        self.discovery_in = self.addInPort("discovery_in")
        self.discovery_out = self.addOutPort("discovery_out")
        self.data_in = self.addInPort("data_in")
        self.data_out = self.addOutPort("data_out")
        self.rmw_command_in = self.addInPort("rmw_commands")
        self.rmw_response_out = self.addOutPort("rmw_responses")
        
        # Configuration
        self.discovery_period = config.dds.discovery_period_ms / 1000.0
        self.heartbeat_period = 0.1  # 100ms heartbeat
        self.lease_duration = config.dds.participant_lease_duration_ms / 1000.0
        
        # Register context
        self.context_key = context_manager.register_component(
            f"dds_participant_{self.participant_guid[:8]}",
            "dds_participant",
            f"dds_domain_{domain_id}"
        )
        
    def __lt__(self, other):
        """Compare participants by name for DEVS simulator"""
        return self.name < other.name
        
    def timeAdvance(self):
        """Time advance function"""
        current_time = time.time()
        
        if self.state['phase'] == 'initializing':
            return 0.01  # Quick initialization
            
        elif self.state['phase'] == 'active':
            # Check what needs to be done
            time_since_discovery = current_time - self.state['last_discovery_time']
            time_since_heartbeat = current_time - self.state['last_heartbeat_time']
            
            if time_since_discovery >= self.discovery_period:
                self.state['phase'] = 'discovering'
                return 0.001  # Quick discovery
                
            elif time_since_heartbeat >= self.heartbeat_period:
                self.state['phase'] = 'heartbeat'
                return 0.001  # Quick heartbeat
                
            elif self.state['pending_messages']:
                # Respect transport-like latency based on payload size
                self.state['phase'] = 'sending_data'
                next_latency = self._estimate_send_latency(self.state['pending_messages'][0])
                return next_latency
                
            else:
                # Wait until next activity
                next_discovery = self.discovery_period - time_since_discovery
                next_heartbeat = self.heartbeat_period - time_since_heartbeat
                return min(next_discovery, next_heartbeat)
                
        elif self.state['phase'] in ['discovering', 'heartbeat', 'sending_data']:
            return 0.001
            
        return INFINITY
        
    def outputFnc(self):
        """Output function"""
        if self.state['phase'] == 'initializing' and not self.state['initialized']:
            # Log participant creation
            trace_logger.log_event(
                "dds_participant_created",
                {
                    "participant_guid": self.participant_guid,
                    "domain_id": self.domain_id
                },
                self.context_key
            )
            
        elif self.state['phase'] == 'discovering':
            # Send discovery message
            discovery_msg = self._create_discovery_message()
            
            trace_logger.log_event(
                "dds_discovery_send",
                {
                    "participant_guid": self.participant_guid,
                    "num_writers": len(self._get_all_writers()),
                    "num_readers": len(self._get_all_readers())
                },
                self.context_key
            )
            
            return {self.discovery_out: discovery_msg}
            
        elif self.state['phase'] == 'heartbeat':
            # Send heartbeat
            heartbeat = self._create_heartbeat_message()
            
            trace_logger.log_event(
                "dds_heartbeat_send",
                {"participant_guid": self.participant_guid},
                self.context_key
            )
            
            return {self.discovery_out: heartbeat}
            
        elif self.state['phase'] == 'sending_data' and self.state['pending_messages']:
            # Send pending data message
            msg = self.state['pending_messages'].pop(0)
            
            trace_logger.log_event(
                "dds_data_send",
                {
                    "writer_guid": msg.get('writer_guid'),
                    "sequence_number": msg.get('sequence_number'),
                    "topic": msg.get('topic'),
                    "message_id": getattr(msg.get('data'), 'id', '')
                },
                self.context_key
            )
            
            return {self.data_out: msg}
            
        return {}
        
    def intTransition(self):
        """Internal transition"""
        current_time = time.time()
        
        if self.state['phase'] == 'initializing':
            self.state['initialized'] = True
            self.state['phase'] = 'active'
            self.state['last_discovery_time'] = current_time
            self.state['last_heartbeat_time'] = current_time
            
        elif self.state['phase'] == 'discovering':
            self.state['last_discovery_time'] = current_time
            self.state['phase'] = 'active'
            
        elif self.state['phase'] == 'heartbeat':
            self.state['last_heartbeat_time'] = current_time
            self.state['phase'] = 'active'
            
        elif self.state['phase'] == 'sending_data':
            self.state['phase'] = 'active'
            
        return self.state
        
    def extTransition(self, inputs):
        """External transition"""
        # Handle discovery messages
        if self.discovery_in in inputs:
            discovery_msg = inputs[self.discovery_in]
            self._process_discovery_message(discovery_msg)
            
        # Handle data messages
        if self.data_in in inputs:
            data_msg = inputs[self.data_in]
            self._process_data_message(data_msg)
            
        # Handle RMW commands
        if self.rmw_command_in in inputs:
            command = inputs[self.rmw_command_in]
            self._process_rmw_command(command)
            
        return self.state
        
    def create_writer(self, topic: str, type_name: str, 
                     writer_qos: QoSProfile) -> DataWriter:
        """Create a DataWriter"""
        writer = DataWriter(
            topic_name=topic,
            type_name=type_name,
            qos=writer_qos,
            partition=writer_qos.partition
        )
        
        if topic not in self.state['local_writers']:
            self.state['local_writers'][topic] = []
        self.state['local_writers'][topic].append(writer)
        
        # Initialize sequence number
        self.state['sequence_numbers'][writer.guid] = 0
        
        trace_logger.log_event(
            "dds_create_writer",
            {
                "writer_guid": writer.guid,
                "topic": topic,
                "reliability": writer.qos.reliability.value
            },
            self.context_key
        )
        
        # Trigger discovery
        self.state['phase'] = 'discovering'
        
        return writer
        
    def create_reader(self, topic: str, type_name: str,
                     reader_qos: QoSProfile,
                     callback: Optional[callable] = None) -> DataReader:
        """Create a DataReader"""
        reader = DataReader(
            topic_name=topic,
            type_name=type_name,
            qos=reader_qos,
            partition=reader_qos.partition,
            callback=callback
        )
        
        if topic not in self.state['local_readers']:
            self.state['local_readers'][topic] = []
        self.state['local_readers'][topic].append(reader)
        
        trace_logger.log_event(
            "dds_create_reader",
            {
                "reader_guid": reader.guid,
                "topic": topic,
                "reliability": reader.qos.reliability.value
            },
            self.context_key
        )
        
        # Trigger discovery
        self.state['phase'] = 'discovering'
        
        return reader
        
    def write_data(self, writer_guid: str, data: Message):
        """Write data through a DataWriter"""
        # Find writer
        writer = None
        for writers in self.state['local_writers'].values():
            for w in writers:
                if w.guid == writer_guid:
                    writer = w
                    break
                    
        if not writer:
            return False
            
        # Create DDS data message
        seq_num = self.state['sequence_numbers'][writer_guid]
        self.state['sequence_numbers'][writer_guid] += 1
        
        dds_msg = {
            'writer_guid': writer_guid,
            'sequence_number': seq_num,
            'topic': writer.topic_name,
            'data': data,
            'timestamp': time.time(),
            'message_id': getattr(data, 'id', '')
        }
        
        # Check matched readers
        if writer_guid in self.state['matched_endpoints']:
            # Queue for sending
            self.state['pending_messages'].append(dds_msg)
            return True
        else:
            # No matched readers
            trace_logger.log_event(
                "message_dropped",
                {"writer_guid": writer_guid, "topic": writer.topic_name, "reason": "no_readers", "message_id": getattr(data, 'id', '')},
                self.context_key
            )
            return False
            
    def _create_discovery_message(self) -> DiscoveryMessage:
        """Create participant discovery message"""
        # Gather all endpoints
        endpoints = []
        
        for topic, writers in self.state['local_writers'].items():
            for writer in writers:
                endpoints.append(EndpointInfo(
                    guid=writer.guid,
                    topic=writer.topic_name,
                    type_name=writer.type_name,
                    kind='writer',
                    qos=writer.qos,
                    partition=writer.partition
                ))
                
        for topic, readers in self.state['local_readers'].items():
            for reader in readers:
                endpoints.append(EndpointInfo(
                    guid=reader.guid,
                    topic=reader.topic_name,
                    type_name=reader.type_name,
                    kind='reader',
                    qos=reader.qos,
                    partition=reader.partition
                ))
                
        return DiscoveryMessage(
            participant_guid=self.participant_guid,
            domain_id=self.domain_id,
            endpoints=endpoints,
            lease_duration=self.lease_duration
        )
        
    def _create_heartbeat_message(self) -> Dict:
        """Create heartbeat message"""
        return {
            'type': 'heartbeat',
            'participant_guid': self.participant_guid,
            'timestamp': time.time(),
            'alive_writers': [w.guid for writers in self.state['local_writers'].values() 
                            for w in writers],
            'alive_readers': [r.guid for readers in self.state['local_readers'].values() 
                            for r in readers]
        }
        
    def _process_discovery_message(self, msg: DiscoveryMessage):
        """Process received discovery message"""
        if msg.participant_guid == self.participant_guid:
            return  # Ignore own discovery
            
        # Update discovered participants
        self.state['discovered_participants'][msg.participant_guid] = ParticipantInfo(
            guid=msg.participant_guid,
            domain_id=msg.domain_id,
            lease_expiry=time.time() + msg.lease_duration
        )
        
        # Process endpoints and check for matches
        for endpoint in msg.endpoints:
            self.state['discovered_endpoints'][endpoint.guid] = endpoint
            self._check_endpoint_matching(endpoint)
            
        trace_logger.log_event(
            "dds_discovery_received",
            {
                "from_participant": msg.participant_guid,
                "num_endpoints": len(msg.endpoints)
            },
            self.context_key
        )
        
    def _check_endpoint_matching(self, remote_endpoint: EndpointInfo):
        """Check if remote endpoint matches any local endpoint"""
        if remote_endpoint.kind == 'writer':
            # Check against local readers
            if remote_endpoint.topic in self.state['local_readers']:
                for reader in self.state['local_readers'][remote_endpoint.topic]:
                    if self._qos_match(remote_endpoint.qos, reader.qos):
                        # Match found
                        if reader.guid not in self.state['matched_endpoints']:
                            self.state['matched_endpoints'][reader.guid] = set()
                        self.state['matched_endpoints'][reader.guid].add(remote_endpoint.guid)
                        
                        trace_logger.log_event(
                            "dds_endpoint_matched",
                            {
                                "local_reader": reader.guid,
                                "remote_writer": remote_endpoint.guid,
                                "topic": remote_endpoint.topic
                            },
                            self.context_key
                        )
                        
        elif remote_endpoint.kind == 'reader':
            # Check against local writers
            if remote_endpoint.topic in self.state['local_writers']:
                for writer in self.state['local_writers'][remote_endpoint.topic]:
                    if self._qos_match(writer.qos, remote_endpoint.qos):
                        # Match found
                        if writer.guid not in self.state['matched_endpoints']:
                            self.state['matched_endpoints'][writer.guid] = set()
                        self.state['matched_endpoints'][writer.guid].add(remote_endpoint.guid)
                        
                        trace_logger.log_event(
                            "dds_endpoint_matched",
                            {
                                "local_writer": writer.guid,
                                "remote_reader": remote_endpoint.guid,
                                "topic": remote_endpoint.topic
                            },
                            self.context_key
                        )
    
    def _qos_match(self, writer_qos: QoSProfile, reader_qos: QoSProfile) -> bool:
        """Check QoS compatibility between writer and reader"""
        # Request vs Offered model
        
        # Reliability: Reader can request RELIABLE, writer must offer at least that
        if (reader_qos.reliability.value == "reliable" and 
            writer_qos.reliability.value == "best_effort"):
            return False
            
        # Durability: Reader can request higher durability
        durability_levels = {
            "volatile": 0,
            "transient_local": 1,
            "transient": 2,
            "persistent": 3
        }
        
        writer_level = durability_levels.get(writer_qos.durability.value, 0)
        reader_level = durability_levels.get(reader_qos.durability.value, 0)
        
        if reader_level > writer_level:
            return False
            
        # Deadline: Writer deadline must be <= reader deadline
        if writer_qos.deadline is not None and reader_qos.deadline is not None:
            if writer_qos.deadline > reader_qos.deadline:
                return False
            
        # Partition matching
        if writer_qos.partition and reader_qos.partition:
            # At least one partition must match; simple intersection check
            if not set(writer_qos.partition) & set(reader_qos.partition):
                return False
        
        return True
    
    def _process_data_message(self, msg: Dict):
        """Process received data message"""
        writer_guid = msg['writer_guid']
        
        # Find matching local readers
        for readers in self.state['local_readers'].values():
            for reader in readers:
                if (reader.guid in self.state['matched_endpoints'] and 
                    writer_guid in self.state['matched_endpoints'][reader.guid]):
                    
                    # Deliver to reader
                    if reader.callback:
                        # Simulate deserialization hook (optional)
                        # Deserialize if serialized payload exists
                        payload = msg['data']
                        if getattr(payload, 'serialized_data', None):
                            try:
                                from serialization import type_registry
                                ts = type_registry.get_type_support("message/Message")
                                if ts is not None:
                                    deserialized = ts.deserialize(payload.serialized_data)
                                    payload = deserialized
                            except Exception:
                                pass
                        reader.callback(payload)
                    
                    trace_logger.log_event(
                        "dds_data_received",
                        {
                            "reader_guid": reader.guid,
                            "writer_guid": writer_guid,
                            "sequence_number": msg['sequence_number'],
                            "message_id": getattr(msg['data'], 'id', '')
                        },
                        self.context_key
                    )
    
    def _process_rmw_command(self, command: Dict):
        """Process RMW layer command"""
        cmd_type = command.get('type')
        
        if cmd_type == 'create_writer':
            writer = self.create_writer(
                command['topic'],
                command['type_name'],
                command['qos']
            )
            # Send response back to RMW
            response = {
                'type': 'writer_created',
                'writer_handle': writer.guid,
                'topic': command['topic']
            }
            return {self.rmw_response_out: response}
            
        elif cmd_type == 'create_reader':
            reader = self.create_reader(
                command['topic'],
                command['type_name'],
                command['qos'],
                command.get('callback')
            )
            response = {
                'type': 'reader_created',
                'reader_handle': reader.guid,
                'topic': command['topic']
            }
            return {self.rmw_response_out: response}
            
        elif cmd_type == 'write_data':
            success = self.write_data(
                command['writer_handle'],
                command['data']
            )
            response = {
                'type': 'write_complete',
                'success': success,
                'writer_handle': command['writer_handle']
            }
            return {self.rmw_response_out: response}
    
    def _get_all_writers(self) -> List[DataWriter]:
        """Get all local writers"""
        return [w for writers in self.state['local_writers'].values() for w in writers]
    
    def _get_all_readers(self) -> List[DataReader]:
        """Get all local readers"""
        return [r for readers in self.state['local_readers'].values() for r in readers]
    
    def cleanup_expired_participants(self):
        """Remove expired participants"""
        current_time = time.time()
        expired = []
        
        for guid, info in self.state['discovered_participants'].items():
            if current_time > info.lease_expiry:
                expired.append(guid)
                
        for guid in expired:
            del self.state['discovered_participants'][guid]
            # Also remove their endpoints
            to_remove = []
            for ep_guid, ep_info in self.state['discovered_endpoints'].items():
                if ep_info.participant_guid == guid:
                    to_remove.append(ep_guid)
                    
            for ep_guid in to_remove:
                del self.state['discovered_endpoints'][ep_guid]
                # Update matched endpoints
                for local_guid, matched in self.state['matched_endpoints'].items():
                    matched.discard(ep_guid)

    def _estimate_send_latency(self, dds_msg: Dict) -> float:
        """Estimate network send latency using config and payload size."""
        # Base on LAN latency; rough scaling by payload size if available
        base_us = config.network.lan_latency_us
        payload = dds_msg.get('data')
        size = 0
        if payload is not None and getattr(payload, 'serialized_data', None):
            try:
                size = len(payload.serialized_data)
            except Exception:
                size = 0
        # Add a simple size-based component: 1us per KB
        size_us = (size / 1024.0)
        return max(0.0, (base_us + size_us) / 1e6)