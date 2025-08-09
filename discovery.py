"""
DDS Discovery Protocol implementation.
Handles participant and endpoint discovery.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set
import time

from policies import QoSProfile

@dataclass
class ParticipantInfo:
    """Information about a discovered participant"""
    guid: str
    domain_id: int
    lease_expiry: float
    vendor_id: str = "ROS2_DEVS"
    
    
@dataclass 
class EndpointInfo:
    """Information about a discovered endpoint (reader/writer)"""
    guid: str
    participant_guid: str = ""
    topic: str = ""
    type_name: str = ""
    kind: str = ""  # 'reader' or 'writer'
    qos: QoSProfile = field(default_factory=QoSProfile)
    partition: List[str] = field(default_factory=list)
    
    
@dataclass
class DiscoveryMessage:
    """Discovery protocol message"""
    participant_guid: str
    domain_id: int
    endpoints: List[EndpointInfo]
    lease_duration: float
    timestamp: float = field(default_factory=time.time)
    

class DiscoveryDatabase:
    """Maintains discovered entities database"""
    
    def __init__(self):
        self.participants: Dict[str, ParticipantInfo] = {}
        self.endpoints: Dict[str, EndpointInfo] = {}
        self.topic_cache: Dict[str, Set[str]] = {}  # topic -> set of endpoint guids
        
    def add_participant(self, info: ParticipantInfo):
        """Add or update participant info"""
        self.participants[info.guid] = info
        
    def add_endpoint(self, info: EndpointInfo):
        """Add or update endpoint info"""
        self.endpoints[info.guid] = info
        
        # Update topic cache
        if info.topic not in self.topic_cache:
            self.topic_cache[info.topic] = set()
        self.topic_cache[info.topic].add(info.guid)
        
    def remove_participant(self, guid: str):
        """Remove participant and its endpoints"""
        if guid in self.participants:
            del self.participants[guid]
            
        # Remove associated endpoints
        to_remove = []
        for ep_guid, ep_info in self.endpoints.items():
            if ep_info.participant_guid == guid:
                to_remove.append(ep_guid)
                
        for ep_guid in to_remove:
            self.remove_endpoint(ep_guid)
            
    def remove_endpoint(self, guid: str):
        """Remove endpoint"""
        if guid in self.endpoints:
            ep_info = self.endpoints[guid]
            del self.endpoints[guid]
            
            # Update topic cache
            if ep_info.topic in self.topic_cache:
                self.topic_cache[ep_info.topic].discard(guid)
                if not self.topic_cache[ep_info.topic]:
                    del self.topic_cache[ep_info.topic]
                    
    def get_endpoints_for_topic(self, topic: str) -> List[EndpointInfo]:
        """Get all endpoints for a topic"""
        if topic in self.topic_cache:
            return [self.endpoints[guid] for guid in self.topic_cache[topic] 
                   if guid in self.endpoints]
        return []
    
    def get_writers_for_topic(self, topic: str) -> List[EndpointInfo]:
        """Get all writers for a topic"""
        return [ep for ep in self.get_endpoints_for_topic(topic) 
               if ep.kind == 'writer']
               
    def get_readers_for_topic(self, topic: str) -> List[EndpointInfo]:
        """Get all readers for a topic"""
        return [ep for ep in self.get_endpoints_for_topic(topic) 
               if ep.kind == 'reader']
               
    def cleanup_expired(self, current_time: float):
        """Remove expired participants"""
        expired = []
        
        for guid, info in self.participants.items():
            if current_time > info.lease_expiry:
                expired.append(guid)
                
        for guid in expired:
            self.remove_participant(guid)
            
    def get_statistics(self) -> Dict:
        """Get discovery statistics"""
        topic_stats = {}
        for topic, endpoints in self.topic_cache.items():
            writers = sum(1 for guid in endpoints 
                         if guid in self.endpoints and 
                         self.endpoints[guid].kind == 'writer')
            readers = sum(1 for guid in endpoints 
                         if guid in self.endpoints and 
                         self.endpoints[guid].kind == 'reader')
            topic_stats[topic] = {'writers': writers, 'readers': readers}
            
        return {
            'total_participants': len(self.participants),
            'total_endpoints': len(self.endpoints),
            'topics': len(self.topic_cache),
            'topic_details': topic_stats
        }