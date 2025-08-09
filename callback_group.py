"""
RCLCPP Callback Group implementation.
Manages callback execution policies and thread safety.
"""

from abc import ABC, abstractmethod
from typing import List, Set, Optional, Dict
import threading
import uuid


class CallbackGroup(ABC):
    """
    Abstract base class for callback groups.
    Callback groups control how callbacks can be executed in parallel.
    """
    
    def __init__(self):
        self.id = str(uuid.uuid4())
        self._callbacks: Set[int] = set()  # Set of callback handles
        self._lock = threading.Lock()
        
    @abstractmethod
    def can_execute_in_parallel(self) -> bool:
        """Whether callbacks in this group can execute in parallel"""
        pass
        
    @abstractmethod
    def type_name(self) -> str:
        """Get the type name of this callback group"""
        pass
        
    def add_callback(self, handle: int):
        """Add a callback to this group"""
        with self._lock:
            self._callbacks.add(handle)
            
    def remove_callback(self, handle: int):
        """Remove a callback from this group"""
        with self._lock:
            self._callbacks.discard(handle)
            
    def has_callback(self, handle: int) -> bool:
        """Check if callback is in this group"""
        with self._lock:
            return handle in self._callbacks
            
    def get_callbacks(self) -> List[int]:
        """Get all callback handles in this group"""
        with self._lock:
            return list(self._callbacks)
            
    def size(self) -> int:
        """Get number of callbacks in this group"""
        with self._lock:
            return len(self._callbacks)


class MutuallyExclusiveCallbackGroup(CallbackGroup):
    """
    Mutually exclusive callback group.
    Only one callback from this group can execute at a time.
    """
    
    def __init__(self):
        super().__init__()
        self._executing: Optional[int] = None
        
    def can_execute_in_parallel(self) -> bool:
        return False
        
    def type_name(self) -> str:
        return "MutuallyExclusive"
        
    def try_acquire(self, handle: int) -> bool:
        """Try to acquire execution rights for a callback"""
        with self._lock:
            if self._executing is None:
                self._executing = handle
                return True
            return False
            
    def release(self, handle: int):
        """Release execution rights"""
        with self._lock:
            if self._executing == handle:
                self._executing = None
                
    def is_executing(self) -> bool:
        """Check if any callback is currently executing"""
        with self._lock:
            return self._executing is not None


class ReentrantCallbackGroup(CallbackGroup):
    """
    Reentrant callback group.
    All callbacks in this group can execute in parallel.
    """
    
    def __init__(self):
        super().__init__()
        self._executing: Set[int] = set()
        
    def can_execute_in_parallel(self) -> bool:
        return True
        
    def type_name(self) -> str:
        return "Reentrant"
        
    def try_acquire(self, handle: int) -> bool:
        """Try to acquire execution rights for a callback"""
        with self._lock:
            self._executing.add(handle)
            return True
            
    def release(self, handle: int):
        """Release execution rights"""
        with self._lock:
            self._executing.discard(handle)
            
    def get_executing_count(self) -> int:
        """Get number of currently executing callbacks"""
        with self._lock:
            return len(self._executing)


class CallbackGroupManager:
    """
    Manages callback groups for a node or executor.
    Enforces execution policies across groups.
    """
    
    def __init__(self):
        self._groups: Dict[str, CallbackGroup] = {}
        self._callback_to_group: Dict[int, str] = {}
        self._lock = threading.Lock()
        
    def create_group(self, group_type: str = "MutuallyExclusive") -> CallbackGroup:
        """Create a new callback group"""
        if group_type == "MutuallyExclusive":
            group = MutuallyExclusiveCallbackGroup()
        elif group_type == "Reentrant":
            group = ReentrantCallbackGroup()
        else:
            raise ValueError(f"Unknown callback group type: {group_type}")
            
        with self._lock:
            self._groups[group.id] = group
            
        return group
        
    def add_callback_to_group(self, handle: int, group: CallbackGroup):
        """Add a callback to a group"""
        with self._lock:
            if group.id not in self._groups:
                self._groups[group.id] = group
                
            group.add_callback(handle)
            self._callback_to_group[handle] = group.id
            
    def get_callback_group(self, handle: int) -> Optional[CallbackGroup]:
        """Get the group for a callback"""
        with self._lock:
            group_id = self._callback_to_group.get(handle)
            if group_id:
                return self._groups.get(group_id)
            return None
            
    def can_execute(self, handle: int) -> bool:
        """Check if a callback can execute based on group policies"""
        group = self.get_callback_group(handle)
        
        if not group:
            return True  # No group means no restrictions
            
        if isinstance(group, MutuallyExclusiveCallbackGroup):
            return group.try_acquire(handle)
        elif isinstance(group, ReentrantCallbackGroup):
            return group.try_acquire(handle)
            
        return True
        
    def notify_execution_complete(self, handle: int):
        """Notify that a callback has completed execution"""
        group = self.get_callback_group(handle)
        
        if group:
            if hasattr(group, 'release'):
                group.release(handle)
                
    def get_ready_callbacks(self, available_callbacks: List[int]) -> List[int]:
        """
        Filter available callbacks based on group policies.
        Returns list of callbacks that can execute.
        """
        ready = []
        
        # Get callback groups without holding the manager lock
        callback_groups = {}
        with self._lock:
            for handle in available_callbacks:
                group_id = self._callback_to_group.get(handle)
                if group_id:
                    callback_groups[handle] = self._groups.get(group_id)
        
        # Check execution permissions without holding manager lock
        for handle in available_callbacks:
            group = callback_groups.get(handle)
            
            if not group:
                # No group means no restrictions
                ready.append(handle)
            elif isinstance(group, MutuallyExclusiveCallbackGroup):
                # For mutually exclusive, only allow if not executing
                if not group.is_executing():
                    ready.append(handle)
            elif isinstance(group, ReentrantCallbackGroup):
                # For reentrant, always allow
                ready.append(handle)
                    
        return ready
        
    def get_statistics(self) -> Dict:
        """Get callback group statistics"""
        stats = {
            'total_groups': len(self._groups),
            'total_callbacks': len(self._callback_to_group),
            'groups_by_type': {}
        }
        
        for group in self._groups.values():
            group_type = group.type_name()
            if group_type not in stats['groups_by_type']:
                stats['groups_by_type'][group_type] = 0
            stats['groups_by_type'][group_type] += 1
            
        return stats
