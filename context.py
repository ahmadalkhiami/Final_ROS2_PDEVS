"""
Execution context management for ROS2 components.
Handles thread contexts, process mapping, and CPU affinity.
Compatible with ROS2 tracing format.
"""

from dataclasses import dataclass
from typing import Dict, Optional, Set, List, Any
import threading
import os
import sys
import random


@dataclass(frozen=True)  # Make the dataclass immutable and hashable
class ExecutionContext:
    """Execution context for a ROS2 component"""
    thread_id: int
    process_name: str
    process_id: int
    cpu_id: int
    component_name: str
    numa_node: int = 0
    priority: int = 20  # Linux nice value
    
    def __str__(self) -> str:
        return (f"Context(tid={self.thread_id}, pid={self.process_id}, "
                f"cpu={self.cpu_id}, component={self.component_name})")
    
    def to_ros2_context(self) -> Dict[str, Any]:
        """Convert to ROS2-compatible context format"""
        return {
            'cpu_id': self.cpu_id,
            'procname': self.process_name,
            'vtid': self.thread_id,
            'vpid': self.process_id
        }


class ContextManager:
    """Manages execution contexts with proper thread safety"""
    
    def __init__(self, num_cpus: int = 4):
        self._lock = threading.Lock()
        self._contexts: Dict[str, ExecutionContext] = {}
        self._thread_counter = 6900  # Start from 6900 like original model
        self._process_counter = 6900  # Start from 6900 like original model
        self._cpu_affinity_map: Dict[int, Set[str]] = {i: set() for i in range(num_cpus)}
        self._num_cpus = num_cpus
        
        # Process hierarchy tracking
        self._process_tree: Dict[int, Set[int]] = {}  # parent_pid -> child_pids
        self._component_processes: Dict[str, int] = {}  # component_type -> pid
        
    def create_process_context(self, process_name: str, 
                             parent_pid: Optional[int] = None) -> int:
        """Create a new process context"""
        with self._lock:
            pid = self._next_process_id()
            
            if parent_pid and parent_pid in self._process_tree:
                self._process_tree[parent_pid].add(pid)
            else:
                self._process_tree[pid] = set()
            
            # Always add the process to the tree, even if it's a root process
            if pid not in self._process_tree:
                self._process_tree[pid] = set()
            
            return pid
    
    def register_component(self, component_name: str, 
                          component_type: str,
                          process_name: Optional[str] = None,
                          cpu_affinity: Optional[List[int]] = None,
                          inherit_from: Optional[str] = None) -> ExecutionContext:
        """Register a component with smart context assignment.
        This now prefers real OS ids for tracing (vpid/vtid) while
        preserving simulated CPU placement and NUMA mapping.
        """
        with self._lock:
            # Determine process context
            if inherit_from and inherit_from in self._contexts:
                parent_ctx = self._contexts[inherit_from]
                pid = parent_ctx.process_id
                proc_name = parent_ctx.process_name
            elif process_name:
                proc_name = process_name
                if component_type in self._component_processes:
                    pid = self._component_processes[component_type]
                else:
                    # Prefer real process id when available
                    pid = os.getpid()
                    self._component_processes[component_type] = pid
            else:
                # Default to current process
                proc_name = process_name or os.path.basename(sys.argv[0] or "python")
                pid = os.getpid()
            
            # CPU assignment with load balancing
            if cpu_affinity:
                cpu_id = self._select_cpu_from_affinity(cpu_affinity)
            else:
                cpu_id = self._select_least_loaded_cpu()
            
            # Create context
            context = ExecutionContext(
                # Prefer real thread id to match OS tracing
                thread_id=threading.get_ident() or self._next_thread_id(),
                process_name=proc_name,
                process_id=pid,
                cpu_id=cpu_id,
                component_name=component_name,
                numa_node=cpu_id // (self._num_cpus // 2)  # Simple NUMA mapping
            )
            
            self._contexts[component_name] = context
            self._cpu_affinity_map[cpu_id].add(component_name)
            
            return context
    
    def get_context(self, component_name: str) -> Optional[ExecutionContext]:
        """Get context for a component"""
        with self._lock:
            return self._contexts.get(component_name)
    
    def get_ros2_context(self, component_name: str) -> Optional[Dict[str, Any]]:
        """Get ROS2-compatible context for a component"""
        context = self.get_context(component_name)
        if context:
            return context.to_ros2_context()
        return None
    
    def migrate_component(self, component_name: str, new_cpu_id: int):
        """Migrate component to different CPU"""
        with self._lock:
            if component_name in self._contexts:
                context = self._contexts[component_name]
                old_cpu = context.cpu_id
                
                # Update CPU affinity map
                self._cpu_affinity_map[old_cpu].discard(component_name)
                self._cpu_affinity_map[new_cpu_id].add(component_name)
                
                # Create new context with updated CPU
                new_context = ExecutionContext(
                    thread_id=context.thread_id,
                    process_name=context.process_name,
                    process_id=context.process_id,
                    cpu_id=new_cpu_id,
                    component_name=context.component_name,
                    numa_node=new_cpu_id // (self._num_cpus // 2),
                    priority=context.priority
                )
                
                # Replace the context
                self._contexts[component_name] = new_context
    
    def get_cpu_load(self, cpu_id: int) -> int:
        """Get number of components on a CPU"""
        with self._lock:
            return len(self._cpu_affinity_map.get(cpu_id, set()))
    
    def get_process_tree(self) -> Dict[int, Set[int]]:
        """Get process hierarchy"""
        with self._lock:
            return self._process_tree.copy()
    
    def _next_thread_id(self) -> int:
        """Generate next thread ID (starting from 6900 like original)"""
        tid = self._thread_counter
        self._thread_counter += 1
        return tid
    
    def _next_process_id(self) -> int:
        """Generate next process ID (starting from 6900 like original)"""
        pid = self._process_counter
        self._process_counter += 1
        return pid
    
    def _select_least_loaded_cpu(self) -> int:
        """Select CPU with least components"""
        min_load = float('inf')
        selected_cpu = 0
        
        for cpu_id in range(self._num_cpus):
            load = len(self._cpu_affinity_map[cpu_id])
            if load < min_load:
                min_load = load
                selected_cpu = cpu_id
        
        return selected_cpu
    
    def _select_cpu_from_affinity(self, affinity: List[int]) -> int:
        """Select CPU from affinity list based on load"""
        loads = [(cpu, len(self._cpu_affinity_map[cpu])) 
                 for cpu in affinity if cpu < self._num_cpus]
        
        if not loads:
            return self._select_least_loaded_cpu()
        
        # Select CPU with minimum load from affinity list
        return min(loads, key=lambda x: x[1])[0]
    
    def create_node_context(self, node_name: str) -> str:
        """Convenience method for node registration"""
        ctx = self.register_component(
            component_name=f"node_{node_name}",
            component_type="node",
            process_name=node_name
        )
        return f"node_{node_name}"
    
    def create_executor_context(self, executor_name: str, 
                              num_threads: int = 1) -> List[str]:
        """Create contexts for executor threads"""
        contexts = []
        
        # Create main executor context
        main_ctx = self.register_component(
            component_name=f"executor_{executor_name}",
            component_type="executor",
            process_name=f"{executor_name}_executor"
        )
        contexts.append(f"executor_{executor_name}")
        
        # Create worker thread contexts
        for i in range(num_threads - 1):
            worker_ctx = self.register_component(
                component_name=f"executor_{executor_name}_worker_{i}",
                component_type="executor_worker",
                inherit_from=f"executor_{executor_name}"
            )
            contexts.append(f"executor_{executor_name}_worker_{i}")
        
        return contexts
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get context manager statistics"""
        with self._lock:
            # Count unique processes and threads
            unique_processes = set()
            unique_threads = set()
            for context in self._contexts.values():
                unique_processes.add(context.process_id)
                unique_threads.add(context.thread_id)
            
            return {
                'total_contexts': len(self._contexts),
                'total_components': len(self._contexts),
                'total_processes': len(self._component_processes),
                'process_count': len(unique_processes),
                'thread_count': len(unique_threads),
                'cpu_distribution': {
                    cpu: len(components) 
                    for cpu, components in self._cpu_affinity_map.items()
                },
                'process_types': list(self._component_processes.keys())
            }


# Global context manager instance
context_manager = ContextManager(num_cpus=os.cpu_count() or 4)
