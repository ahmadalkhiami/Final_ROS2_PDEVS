"""
Transport layer implementation for DDS.
"""

from pypdevs.DEVS import AtomicDEVS, CoupledDEVS
from pypdevs.infinity import INFINITY
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
import time
import random

from dataTypes import Message, MessageType, QoSReliabilityPolicy
from configuration import config

class TransportType(Enum):
    """Transport types"""
    SHARED_MEMORY = "shm"
    UDP_MULTICAST = "udp_multicast"
    UDP_UNICAST = "udp_unicast"
    TCP = "tcp"


@dataclass
class TransportMessage:
    """Message in transport layer"""
    source_guid: str
    destination_guids: List[str]
    payload: bytes
    size_bytes: int
    transport_type: TransportType
    timestamp: float = field(default_factory=time.time)


class SharedMemoryTransport(AtomicDEVS):
    """Shared memory transport"""
    
    def __init__(self, name: str = "SharedMemoryTransport"):
        AtomicDEVS.__init__(self, name)
        self.data_in = self.addInPort("data_in")
        self.data_out = self.addOutPort("data_out")
        
    def __lt__(self, other):
        """Compare transports by name for DEVS simulator"""
        return self.name < other.name
        
    def timeAdvance(self):
        return INFINITY
        
    def outputFnc(self):
        return {}
        
    def intTransition(self):
        return self.state
        
    def extTransition(self, inputs):
        if self.data_in in inputs:
            # Instant delivery for shared memory
            return {self.data_out: inputs[self.data_in]}
        return self.state


class NetworkTransport(AtomicDEVS):
    """Network transport with configurable characteristics"""
    
    def __init__(self, name: str, transport_type: TransportType):
        AtomicDEVS.__init__(self, name)
        self.transport_type = transport_type
        self.send_in = self.addInPort("send_in")
        self.receive_out = self.addOutPort("receive_out")
        self.state = {'pending_messages': []}
        
    def __lt__(self, other):
        """Compare transports by name for DEVS simulator"""
        return self.name < other.name
        
    def timeAdvance(self):
        if self.state['pending_messages']:
            return self._get_latency()
        return INFINITY
        
    def outputFnc(self):
        if self.state['pending_messages']:
            return {self.receive_out: self.state['pending_messages'][0]}
        return {}
        
    def intTransition(self):
        if self.state['pending_messages']:
            self.state['pending_messages'].pop(0)
        return self.state
        
    def extTransition(self, inputs):
        if self.send_in in inputs:
            message = inputs[self.send_in]
            if not self._should_drop():
                self.state['pending_messages'].append(message)
        return self.state
        
    def _get_latency(self) -> float:
        """Get transport latency based on type with optional jitter"""
        base_us = (
            config.network.lan_latency_us
            if self.transport_type in (TransportType.UDP_MULTICAST, TransportType.UDP_UNICAST)
            else config.network.wan_latency_us
        )
        # Add jitter (Gaussian), clamp to >= 0
        jitter_us = random.gauss(config.network.jitter_us_mean, config.network.jitter_us_std)
        total_us = max(0.0, base_us + jitter_us)
        return total_us / 1e6
            
    def _should_drop(self) -> bool:
        """Check if message should be dropped"""
        if self.transport_type == TransportType.UDP_MULTICAST:
            return random.random() < config.network.lan_loss_rate
        elif self.transport_type == TransportType.UDP_UNICAST:
            return random.random() < config.network.lan_loss_rate
        else:  # TCP - no drops
            return False


class TransportRouter(AtomicDEVS):
    """Routes messages to appropriate transport"""
    
    def __init__(self, name: str = "TransportRouter"):
        AtomicDEVS.__init__(self, name)
        self.data_in = self.addInPort("data_in")
        self.shm_out = self.addOutPort("shm_out")
        self.multicast_out = self.addOutPort("multicast_out")
        self.unicast_out = self.addOutPort("unicast_out")
        self.tcp_out = self.addOutPort("tcp_out")
        
    def __lt__(self, other):
        """Compare routers by name for DEVS simulator"""
        return self.name < other.name
        
    def timeAdvance(self):
        return INFINITY
        
    def outputFnc(self):
        return {}
        
    def intTransition(self):
        return self.state
        
    def extTransition(self, inputs):
        if self.data_in in inputs:
            message = inputs[self.data_in]
            transport = self._select_transport(message)
            if transport == TransportType.SHARED_MEMORY:
                return {self.shm_out: message}
            elif transport == TransportType.UDP_MULTICAST:
                return {self.multicast_out: message}
            elif transport == TransportType.UDP_UNICAST:
                return {self.unicast_out: message}
            else:
                return {self.tcp_out: message}
        return self.state
        
    def _select_transport(self, message: Message) -> TransportType:
        """Select appropriate transport for message"""
        # Use shared memory for local communication
        if message.source_node in message.destination_nodes:
            return TransportType.SHARED_MEMORY
            
        # Use multicast for clock messages
        if message.type == MessageType.CLOCK:
            return TransportType.UDP_MULTICAST
            
        # Use TCP for reliable communication
        if message.qos_profile and message.qos_profile.reliability == QoSReliabilityPolicy.RELIABLE:
            return TransportType.TCP
            
        # Default to unicast UDP
        return TransportType.UDP_UNICAST


class TransportMultiplexer(CoupledDEVS):
    """
    Multiplexes between different transport types based on message characteristics.
    """
    
    def __init__(self, name: str = "TransportMultiplexer"):
        CoupledDEVS.__init__(self, name)
        
        # Create transport instances
        self.shm_transport = self.addSubModel(SharedMemoryTransport())
        self.multicast_transport = self.addSubModel(
            NetworkTransport("MulticastTransport", TransportType.UDP_MULTICAST)
        )
        self.unicast_transport = self.addSubModel(
            NetworkTransport("UnicastTransport", TransportType.UDP_UNICAST)
        )
        self.tcp_transport = self.addSubModel(
            NetworkTransport("TCPTransport", TransportType.TCP)
        )
        
        # Router to decide transport
        self.router = self.addSubModel(TransportRouter())
        
        # External ports
        self.data_in = self.addInPort("data_in")
        self.data_out = self.addOutPort("data_out")
        
        # Connect router to transports
        self.connectPorts(self.data_in, self.router.data_in)
        self.connectPorts(self.router.shm_out, self.shm_transport.data_in)
        self.connectPorts(self.router.multicast_out, self.multicast_transport.send_in)
        self.connectPorts(self.router.unicast_out, self.unicast_transport.send_in)
        self.connectPorts(self.router.tcp_out, self.tcp_transport.send_in)
        
        # Connect transport outputs
        self.connectPorts(self.shm_transport.data_out, self.data_out)
        self.connectPorts(self.multicast_transport.receive_out, self.data_out)
        self.connectPorts(self.unicast_transport.receive_out, self.data_out)
        self.connectPorts(self.tcp_transport.receive_out, self.data_out)
        
    def __lt__(self, other):
        """Compare multiplexers by name for DEVS simulator"""
        return self.name < other.name