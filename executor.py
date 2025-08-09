"""
RCLCPP Executor implementations.
Provides different executor strategies for callback scheduling.
"""

from pypdevs.DEVS import AtomicDEVS
from pypdevs.infinity import INFINITY
import random
import time
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass
from enum import Enum
import threading
import queue

from tracing import trace_logger
from context import context_manager
from configuration import config
from callback_group import CallbackGroup, ReentrantCallbackGroup

@dataclass
class WorkItem:
    """Represents a unit of work for the executor"""
    work_type: str  # 'subscription', 'timer', 'service', 'guard_condition'
    handle: int
    callback: callable
    callback_group: CallbackGroup
    data: Optional[Any] = None
    priority: int = 0
    
    
class SingleThreadedExecutor(AtomicDEVS):
    """
    Single-threaded executor that processes callbacks sequentially.
    """
    
    def __init__(self, name: str = "SingleThreadedExecutor"):
        AtomicDEVS.__init__(self, name)
        
        self.state = {
            'phase': 'idle',
            'work_queue': [],
            'current_work': None,
            'statistics': {
                'callbacks_executed': 0,
                'total_execution_time': 0.0,
                'spin_count': 0
            }
        }
        
        # Ports
        self.work_in = self.addInPort("work_items")
        self.work_complete_out = self.addOutPort("work_complete")
        
        # Register context
        self.context_key = context_manager.register_component(
            f"executor_{name}",
            "executor",
            "rclcpp"
        )
        
    def timeAdvance(self):
        if self.state['phase'] == 'idle':
            if self.state['work_queue']:
                return 0.0  # Process immediately
            else:
                # Spin period
                return config.executor.spin_period_us / 1e6
                
        elif self.state['phase'] == 'executing':
            # Deterministic callback execution time based on configuration
            work = self.state['current_work']
            base_us = config.executor.callback_duration_us
            # Heuristic: timers are heavier than subscriptions in this model
            if work.work_type == 'timer':
                base_us *= 5
            duration = base_us / 1e6
            return duration
            
        return INFINITY
        
    def outputFnc(self):
        if self.state['phase'] == 'idle' and self.state['work_queue']:
            # Get next work item
            work = self.state['work_queue'][0]
            
            trace_logger.log_event(
                "rclcpp_executor_execute",
                {
                    "handle": f"0x{work.handle:X}",
                    "work_type": work.work_type
                },
                self.context_key
            )
            
            # Start execution
            self.state['current_work'] = work
            self.state['phase'] = 'executing'
            
            # Log callback start (ROS 2 aligned)
            if work.work_type == 'subscription' and work.data:
                trace_logger.log_event(
                    "callback_start",
                    {"message_id": work.data.id, "work_type": "subscription"},
                    self.context_key
                )
                
        elif self.state['phase'] == 'executing':
            # Complete execution
            work = self.state['current_work']
            
            # Send completion notification
            result = {
                'type': work.work_type + '_callback',
                'handle': work.handle,
                'execution_time': config.executor.callback_duration_us / 1e6
            }
            
            if work.work_type == 'subscription' and work.data:
                result['message_id'] = work.data.id
                # Log callback end (ROS 2 aligned)
                trace_logger.log_event(
                    "callback_end",
                    {"message_id": work.data.id, "work_type": "subscription"},
                    self.context_key
                )
                # Emit completion on a port-like event sink if present
                # This executor is standalone; integration scripts can observe this return dict
                
            self.state['statistics']['callbacks_executed'] += 1
            
            return {self.work_complete_out: result}
            
        return {}
        
    def intTransition(self):
        if self.state['phase'] == 'idle':
            # Check for work
            trace_logger.log_event(
                "rclcpp_executor_wait_for_work",
                {},
                self.context_key
            )
            
            if self.state['work_queue']:
                trace_logger.log_event(
                    "rclcpp_executor_get_next_ready",
                    {},
                    self.context_key
                )
                
        elif self.state['phase'] == 'executing':
            # Complete current work
            self.state['work_queue'].pop(0)
            self.state['current_work'] = None
            self.state['phase'] = 'idle'
            self.state['statistics']['spin_count'] += 1
            
        return self.state
        
    def extTransition(self, inputs):
        if self.work_in in inputs:
            work_item = inputs[self.work_in]
            if isinstance(work_item, dict):
                # Convert to WorkItem
                work = WorkItem(
                    work_type=work_item.get('type', 'unknown'),
                    handle=work_item.get('handle', 0),
                    callback=work_item.get('callback'),
                    callback_group=work_item.get('callback_group'),
                    data=work_item.get('message')
                )
                self.state['work_queue'].append(work)
                
        return self.state


class MultiThreadedExecutor(AtomicDEVS):
    """
    Multi-threaded executor that can process callbacks in parallel.
    """
    
    def __init__(self, name: str = "MultiThreadedExecutor", num_threads: int = 4):
        AtomicDEVS.__init__(self, name)
        
        self.num_threads = num_threads
        self.state = {
            'phase': 'idle',
            'work_queue': [],
            'active_threads': {},  # thread_id -> WorkItem
            'available_threads': list(range(num_threads)),
            'callback_group_locks': {},  # callback_group -> thread_id
            'thread_start_times': {},   # thread_id -> start_time
            'thread_durations': {},     # thread_id -> duration_sec
            'statistics': {
                'callbacks_executed': 0,
                'parallel_executions': 0,
                'thread_utilization': [0] * num_threads
            }
        }
        
        # Ports
        self.work_in = self.addInPort("work_items")
        self.work_complete_out = self.addOutPort("work_complete")
        
        # Register contexts for each thread
        self.thread_contexts = []
        contexts = context_manager.create_executor_context(name, num_threads)
        self.context_key = contexts[0]
        for i, ctx in enumerate(contexts[1:]):
            self.thread_contexts.append(ctx)
            
    def timeAdvance(self):
        if self.state['phase'] == 'idle':
            if self.state['work_queue'] and self.state['available_threads']:
                return 0.0  # Dispatch immediately
            elif self.state['active_threads']:
                # Wait for shortest running thread
                return self._get_shortest_execution_time()
            else:
                # Spin period
                return config.executor.spin_period_us / 1e6
                
        elif self.state['phase'] == 'dispatching':
            return 0.000001  # Very fast dispatch
            
        elif self.state['phase'] == 'executing':
            # Some thread is completing
            return 0.0
            
        return INFINITY
        
    def outputFnc(self):
        if self.state['phase'] == 'dispatching':
            # Dispatch work to available threads
            dispatched = []
            
            while self.state['work_queue'] and self.state['available_threads']:
                work = self.state['work_queue'][0]
                
                # Check if callback group allows execution
                if not self._can_execute(work):
                    # Try next work item
                    self.state['work_queue'].pop(0)
                    self.state['work_queue'].append(work)
                    continue
                    
                # Assign to thread
                thread_id = self.state['available_threads'].pop(0)
                self.state['active_threads'][thread_id] = work
                self.state['work_queue'].pop(0)
                # Track start time and deterministic duration
                self.state['thread_start_times'][thread_id] = time.time()
                base_us = config.executor.callback_duration_us
                if work.work_type == 'timer':
                    base_us *= 5
                self.state['thread_durations'][thread_id] = base_us / 1e6
                
                # Lock callback group if exclusive
                if not isinstance(work.callback_group, ReentrantCallbackGroup):
                    self.state['callback_group_locks'][work.callback_group] = thread_id
                    
                # Log execution on specific thread
                trace_logger.log_event(
                    "rclcpp_executor_execute",
                    {
                        "handle": f"0x{work.handle:X}",
                        "thread_id": thread_id
                    },
                    self.thread_contexts[thread_id] if thread_id < len(self.thread_contexts) else self.context_key
                )
                
                dispatched.append((thread_id, work))
                
            # Update statistics
            active_count = len(self.state['active_threads'])
            if active_count > 1:
                self.state['statistics']['parallel_executions'] += 1
                
        elif self.state['phase'] == 'executing':
            # Complete work on threads that are done
            completed = []
            current_time = time.time()
            
            for thread_id, work in list(self.state['active_threads'].items()):
                start = self.state['thread_start_times'].get(thread_id, current_time)
                duration = self.state['thread_durations'].get(thread_id, 0.0)
                if current_time - start >= duration:
                    completed.append((thread_id, work))
                    
            # Process completions
            for thread_id, work in completed:
                # Remove from active
                del self.state['active_threads'][thread_id]
                self.state['available_threads'].append(thread_id)
                self.state['thread_start_times'].pop(thread_id, None)
                self.state['thread_durations'].pop(thread_id, None)
                
                # Release callback group lock
                if work.callback_group in self.state['callback_group_locks']:
                    if self.state['callback_group_locks'][work.callback_group] == thread_id:
                        del self.state['callback_group_locks'][work.callback_group]
                        
                # Send completion
                result = {
                    'type': work.work_type + '_callback',
                    'handle': work.handle,
                    'thread_id': thread_id
                }
                
                if work.work_type == 'subscription' and work.data:
                    result['message_id'] = work.data.id
                    
                self.state['statistics']['callbacks_executed'] += 1
                self.state['statistics']['thread_utilization'][thread_id] += 1
                
                return {self.work_complete_out: result}
                
        return {}
        
    def intTransition(self):
        if self.state['phase'] == 'idle':
            if self.state['work_queue'] and self.state['available_threads']:
                self.state['phase'] = 'dispatching'
            elif self.state['active_threads']:
                self.state['phase'] = 'executing'
                
        elif self.state['phase'] == 'dispatching':
            if self.state['active_threads']:
                self.state['phase'] = 'executing'
            else:
                self.state['phase'] = 'idle'
                
        elif self.state['phase'] == 'executing':
            if self.state['work_queue'] and self.state['available_threads']:
                self.state['phase'] = 'dispatching'
            elif self.state['active_threads']:
                self.state['phase'] = 'executing'
            else:
                self.state['phase'] = 'idle'
                
        return self.state
        
    def extTransition(self, inputs):
        if self.work_in in inputs:
            work_item = inputs[self.work_in]
            if isinstance(work_item, dict):
                # Convert to WorkItem
                work = WorkItem(
                    work_type=work_item.get('type', 'unknown'),
                    handle=work_item.get('handle', 0),
                    callback=work_item.get('callback'),
                    callback_group=work_item.get('callback_group'),
                    data=work_item.get('message')
                )
                self.state['work_queue'].append(work)
                
        return self.state
        
    def _can_execute(self, work: WorkItem) -> bool:
        """Check if work item can be executed based on callback group"""
        if isinstance(work.callback_group, ReentrantCallbackGroup):
            # Reentrant groups can always execute
            return True
            
        # Mutually exclusive groups can only have one callback active
        return work.callback_group not in self.state['callback_group_locks']
        
    def _get_shortest_execution_time(self) -> float:
        """Estimate shortest remaining execution time"""
        remaining = []
        now = time.time()
        for thread_id in self.state['active_threads'].keys():
            start = self.state['thread_start_times'].get(thread_id, now)
            duration = self.state['thread_durations'].get(thread_id, 0.0)
            rem = max(0.0, duration - (now - start))
            remaining.append(rem)
        return min(remaining) if remaining else 0.0
        
    def _is_thread_complete(self, thread_id: int, current_time: float) -> bool:
        """Check if thread has completed its work (deterministic)"""
        start = self.state['thread_start_times'].get(thread_id, current_time)
        duration = self.state['thread_durations'].get(thread_id, 0.0)
        return (current_time - start) >= duration
        
    def get_statistics(self) -> Dict:
        """Get executor statistics"""
        stats = self.state['statistics'].copy()
        stats['thread_efficiency'] = [
            count / max(stats['callbacks_executed'], 1) 
            for count in stats['thread_utilization']
        ]
        stats['parallelism_ratio'] = (
            stats['parallel_executions'] / max(stats['callbacks_executed'], 1)
        )
        return stats


class StaticSingleThreadedExecutor(AtomicDEVS):
    """
    Static single-threaded executor with predetermined callback order.
    Provides deterministic execution order.
    """
    
    def __init__(self, name: str = "StaticSingleThreadedExecutor"):
        AtomicDEVS.__init__(self, name)
        
        self.state = {
            'phase': 'idle',
            'static_work_order': [],  # Predetermined order
            'work_index': 0,
            'work_items': {},  # handle -> WorkItem
            'statistics': {
                'callbacks_executed': 0,
                'order_violations': 0
            }
        }
        
        # Ports
        self.work_in = self.addInPort("work_items")
        self.work_complete_out = self.addOutPort("work_complete")
        
        # Register context
        self.context_key = context_manager.register_component(
            f"static_executor_{name}",
            "executor",
            "rclcpp"
        )
        
    def set_static_order(self, work_order: List[int]):
        """Set the static execution order by handle"""
        self.state['static_work_order'] = work_order
        self.state['work_index'] = 0
        
    def timeAdvance(self):
        if self.state['phase'] == 'idle':
            # Check if we have work in the predetermined order
            if self._has_next_work():
                return 0.0
            else:
                return config.executor.spin_period_us / 1e6
                
        elif self.state['phase'] == 'executing':
            # Deterministic execution time
            return 0.00001  # Fixed 10µs
            
        return INFINITY
        
    def outputFnc(self):
        if self.state['phase'] == 'idle' and self._has_next_work():
            # Get next work item in static order
            handle = self._get_next_handle()
            if handle in self.state['work_items']:
                work = self.state['work_items'][handle]
                
                trace_logger.log_event(
                    "rclcpp_executor_execute",
                    {
                        "handle": f"0x{handle:X}",
                        "order_index": self.state['work_index']
                    },
                    self.context_key
                )
                
                self.state['phase'] = 'executing'
                
        elif self.state['phase'] == 'executing':
            # Complete execution
            handle = self.state['static_work_order'][self.state['work_index']]
            if handle in self.state['work_items']:
                work = self.state['work_items'][handle]
                
                result = {
                    'type': work.work_type + '_callback',
                    'handle': handle,
                    'order_index': self.state['work_index']
                }
                
                self.state['statistics']['callbacks_executed'] += 1
                
                # Remove completed work
                del self.state['work_items'][handle]
                
                return {self.work_complete_out: result}
                
        return {}
        
    def intTransition(self):
        if self.state['phase'] == 'executing':
            # Move to next in order
            self.state['work_index'] = (
                (self.state['work_index'] + 1) % len(self.state['static_work_order'])
            )
            self.state['phase'] = 'idle'
            
        return self.state
        
    def extTransition(self, inputs):
        if self.work_in in inputs:
            work_item = inputs[self.work_in]
            if isinstance(work_item, dict):
                # Store work item by handle
                handle = work_item.get('handle', 0)
                
                work = WorkItem(
                    work_type=work_item.get('type', 'unknown'),
                    handle=handle,
                    callback=work_item.get('callback'),
                    callback_group=work_item.get('callback_group'),
                    data=work_item.get('message')
                )
                
                self.state['work_items'][handle] = work
                
                # Check if this violates static order
                if handle not in self.state['static_work_order']:
                    self.state['statistics']['order_violations'] += 1
                    # Add to order if not present
                    self.state['static_work_order'].append(handle)
                    
        return self.state
        
    def _has_next_work(self) -> bool:
        """Check if next work item in order is available"""
        if not self.state['static_work_order']:
            return False
            
        # Check next few items in order (circular)
        for i in range(len(self.state['static_work_order'])):
            idx = (self.state['work_index'] + i) % len(self.state['static_work_order'])
            handle = self.state['static_work_order'][idx]
            if handle in self.state['work_items']:
                return True
                
        return False
        
    def _get_next_handle(self) -> Optional[int]:
        """Get next available handle in order"""
        if not self.state['static_work_order']:
            return None
            
        # Find next available work
        for i in range(len(self.state['static_work_order'])):
            idx = (self.state['work_index'] + i) % len(self.state['static_work_order'])
            handle = self.state['static_work_order'][idx]
            if handle in self.state['work_items']:
                self.state['work_index'] = idx
                return handle
                
        return None