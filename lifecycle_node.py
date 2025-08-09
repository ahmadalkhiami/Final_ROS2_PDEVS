"""
ROS2 Lifecycle Node implementation.
"""

from pypdevs.DEVS import AtomicDEVS
from pypdevs.infinity import INFINITY
import time
from typing import Optional, Dict, Any, Callable
from enum import Enum, auto
from dataclasses import dataclass

from tracing import trace_logger
from context import context_manager
from configuration import config


class LifecycleState(Enum):
    """ROS2 Lifecycle states"""
    UNCONFIGURED = auto()
    INACTIVE = auto()
    ACTIVE = auto()
    FINALIZED = auto()
    

class LifecycleTransition(Enum):
    """ROS2 Lifecycle transitions"""
    CONFIGURE = auto()
    CLEANUP = auto()
    ACTIVATE = auto()
    DEACTIVATE = auto()
    SHUTDOWN = auto()
    

@dataclass
class TransitionEvent:
    """Lifecycle transition event"""
    transition: LifecycleTransition
    request_id: str
    timestamp: float = 0.0
    

@dataclass 
class TransitionResult:
    """Result of lifecycle transition"""
    success: bool
    error_msg: str = ""
    from_state: LifecycleState = None
    to_state: LifecycleState = None


class LifecycleNode(AtomicDEVS):
    """
    ROS2 Managed Lifecycle Node with proper state machine.
    """
    
    def __init__(self, name: str, node_name: str,
                 on_configure: Optional[Callable] = None,
                 on_cleanup: Optional[Callable] = None,
                 on_activate: Optional[Callable] = None,
                 on_deactivate: Optional[Callable] = None,
                 on_shutdown: Optional[Callable] = None,
                 on_error: Optional[Callable] = None):
        AtomicDEVS.__init__(self, name)
        
        # Configuration
        self.node_name = node_name
        
        # Callbacks
        self._on_configure = on_configure or self._default_on_configure
        self._on_cleanup = on_cleanup or self._default_on_cleanup
        self._on_activate = on_activate or self._default_on_activate
        self._on_deactivate = on_deactivate or self._default_on_deactivate
        self._on_shutdown = on_shutdown or self._default_on_shutdown
        self._on_error = on_error or self._default_on_error
        
        # State
        self.state = {
            'lifecycle_state': LifecycleState.UNCONFIGURED,
            'phase': 'idle',
            'pending_transition': None,
            'transition_start_time': 0.0,
            'publishers_active': False,
            'timers_active': False,
            'parameters': {},
            'transition_history': []
        }
        
        # Valid transitions map
        self.valid_transitions = {
            LifecycleState.UNCONFIGURED: {
                LifecycleTransition.CONFIGURE: LifecycleState.INACTIVE,
                LifecycleTransition.SHUTDOWN: LifecycleState.FINALIZED
            },
            LifecycleState.INACTIVE: {
                LifecycleTransition.CLEANUP: LifecycleState.UNCONFIGURED,
                LifecycleTransition.ACTIVATE: LifecycleState.ACTIVE,
                LifecycleTransition.SHUTDOWN: LifecycleState.FINALIZED
            },
            LifecycleState.ACTIVE: {
                LifecycleTransition.DEACTIVATE: LifecycleState.INACTIVE,
                LifecycleTransition.SHUTDOWN: LifecycleState.FINALIZED
            },
            LifecycleState.FINALIZED: {}
        }
        
        # Ports
        self.transition_in = self.addInPort("transitions")
        self.transition_out = self.addOutPort("transition_results")
        self.control_out = self.addOutPort("control_commands")
        
        # Register context
        self.context_key = context_manager.register_component(
            f"lifecycle_{node_name}",
            "lifecycle_node",
            node_name
        )
        
    def timeAdvance(self):
        if self.state['phase'] == 'transitioning':
            # Estimate transition time
            transition = self.state['pending_transition']
            if transition == LifecycleTransition.CONFIGURE:
                return 0.1  # 100ms for configuration
            elif transition == LifecycleTransition.ACTIVATE:
                return 0.05  # 50ms for activation
            elif transition == LifecycleTransition.DEACTIVATE:
                return 0.03  # 30ms for deactivation
            elif transition == LifecycleTransition.CLEANUP:
                return 0.08  # 80ms for cleanup
            elif transition == LifecycleTransition.SHUTDOWN:
                return 0.2  # 200ms for shutdown
                
        return INFINITY
        
    def outputFnc(self):
        if self.state['phase'] == 'transitioning':
            transition = self.state['pending_transition']
            from_state = self.state['lifecycle_state']
            
            # Execute transition
            success, error_msg = self._execute_transition(transition)
            
            if success:
                # Update to new state
                to_state = self.valid_transitions[from_state][transition]
                self.state['lifecycle_state'] = to_state
                
                # Control publishers/subscribers based on state
                control_cmd = self._get_control_command(to_state)
                
                result = TransitionResult(
                    success=True,
                    from_state=from_state,
                    to_state=to_state
                )
                
                trace_logger.log_event(
                    "lifecycle_transition_success",
                    {
                        "node": self.node_name,
                        "transition": transition.name,
                        "from_state": from_state.name,
                        "to_state": to_state.name
                    },
                    self.context_key
                )
                
                outputs = {self.transition_out: result}
                if control_cmd:
                    outputs[self.control_out] = control_cmd
                    
                return outputs
                
            else:
                # Transition failed
                result = TransitionResult(
                    success=False,
                    error_msg=error_msg,
                    from_state=from_state,
                    to_state=from_state
                )
                
                trace_logger.log_event(
                    "lifecycle_transition_failed",
                    {
                        "node": self.node_name,
                        "transition": transition.name,
                        "state": from_state.name,
                        "error": error_msg
                    },
                    self.context_key
                )
                
                return {self.transition_out: result}
                
        return {}
        
    def intTransition(self):
        if self.state['phase'] == 'transitioning':
            # Record transition in history
            self.state['transition_history'].append({
                'transition': self.state['pending_transition'],
                'timestamp': time.time(),
                'duration': time.time() - self.state['transition_start_time']
            })
            
            self.state['phase'] = 'idle'
            self.state['pending_transition'] = None
            
        return self.state
        
    def extTransition(self, inputs):
        if self.transition_in in inputs:
            event = inputs[self.transition_in]
            if isinstance(event, TransitionEvent):
                # Check if transition is valid
                current_state = self.state['lifecycle_state']
                
                if (current_state in self.valid_transitions and
                    event.transition in self.valid_transitions[current_state]):
                    
                    self.state['pending_transition'] = event.transition
                    self.state['phase'] = 'transitioning'
                    self.state['transition_start_time'] = time.time()
                    
                    trace_logger.log_event(
                        "lifecycle_transition_request",
                        {
                            "node": self.node_name,
                            "transition": event.transition.name,
                            "current_state": current_state.name
                        },
                        self.context_key
                    )
                else:
                    # Invalid transition
                    trace_logger.log_event(
                        "lifecycle_invalid_transition",
                        {
                            "node": self.node_name,
                            "transition": event.transition.name,
                            "current_state": current_state.name
                        },
                        self.context_key
                    )
                    
        return self.state
        
    def _execute_transition(self, transition: LifecycleTransition) -> tuple[bool, str]:
        """Execute lifecycle transition callback"""
        try:
            if transition == LifecycleTransition.CONFIGURE:
                return self._on_configure()
            elif transition == LifecycleTransition.CLEANUP:
                return self._on_cleanup()
            elif transition == LifecycleTransition.ACTIVATE:
                return self._on_activate()
            elif transition == LifecycleTransition.DEACTIVATE:
                return self._on_deactivate()
            elif transition == LifecycleTransition.SHUTDOWN:
                return self._on_shutdown()
        except Exception as e:
            self._on_error(e)
            return False, str(e)
            
        return False, "Unknown transition"
        
    def _get_control_command(self, state: LifecycleState) -> Optional[Dict]:
        """Get control command based on lifecycle state"""
        if state == LifecycleState.ACTIVE:
            return {
                'command': 'activate',
                'enable_publishers': True,
                'enable_timers': True,
                'enable_services': True
            }
        elif state == LifecycleState.INACTIVE:
            return {
                'command': 'deactivate',
                'enable_publishers': False,
                'enable_timers': False,
                'enable_services': True  # Services stay active
            }
        elif state == LifecycleState.FINALIZED:
            return {
                'command': 'shutdown',
                'enable_publishers': False,
                'enable_timers': False,
                'enable_services': False
            }
        return None
        
    # Default transition callbacks
    def _default_on_configure(self) -> tuple[bool, str]:
        """Default configure callback"""
        trace_logger.log_event(
            "lifecycle_on_configure",
            {"node": self.node_name},
            self.context_key
        )
        # Simulate configuration work
        time.sleep(0.01)
        return True, ""
        
    def _default_on_cleanup(self) -> tuple[bool, str]:
        """Default cleanup callback"""
        trace_logger.log_event(
            "lifecycle_on_cleanup",
            {"node": self.node_name},
            self.context_key
        )
        return True, ""
        
    def _default_on_activate(self) -> tuple[bool, str]:
        """Default activate callback"""
        trace_logger.log_event(
            "lifecycle_on_activate",
            {"node": self.node_name},
            self.context_key
        )
        self.state['publishers_active'] = True
        self.state['timers_active'] = True
        return True, ""
        
    def _default_on_deactivate(self) -> tuple[bool, str]:
        """Default deactivate callback"""
        trace_logger.log_event(
            "lifecycle_on_deactivate",
            {"node": self.node_name},
            self.context_key
        )
        self.state['publishers_active'] = False
        self.state['timers_active'] = False
        return True, ""
        
    def _default_on_shutdown(self) -> tuple[bool, str]:
        """Default shutdown callback"""
        trace_logger.log_event(
            "lifecycle_on_shutdown",
            {"node": self.node_name},
            self.context_key
        )
        return True, ""
        
    def _default_on_error(self, exception: Exception):
        """Default error callback"""
        trace_logger.log_event(
            "lifecycle_on_error",
            {
                "node": self.node_name,
                "error": str(exception)
            },
            self.context_key
        )
        
    def get_current_state(self) -> LifecycleState:
        """Get current lifecycle state"""
        return self.state['lifecycle_state']
        
    def get_available_transitions(self) -> list[LifecycleTransition]:
        """Get available transitions from current state"""
        current_state = self.state['lifecycle_state']
        if current_state in self.valid_transitions:
            return list(self.valid_transitions[current_state].keys())
        return []


class LifecycleManager(AtomicDEVS):
    """
    Manages lifecycle transitions for a group of nodes.
    """
    
    def __init__(self, name: str, managed_nodes: list[str]):
        AtomicDEVS.__init__(self, name)
        
        self.managed_nodes = managed_nodes
        
        # State
        self.state = {
            'phase': 'idle',
            'current_operation': None,  # 'startup', 'shutdown', etc.
            'operation_sequence': [],
            'current_step': 0,
            'waiting_for_responses': {},
            'node_states': {node: LifecycleState.UNCONFIGURED for node in managed_nodes}
        }
        
        # Ports
        self.command_in = self.addInPort("commands")
        self.transition_out = self.addOutPort("transitions")
        self.result_in = self.addInPort("results")
        self.status_out = self.addOutPort("status")
        
        # Register context
        self.context_key = context_manager.register_component(
            "lifecycle_manager",
            "lifecycle_manager",
            "system"
        )
        
    def timeAdvance(self):
        if self.state['phase'] == 'executing_sequence':
            return 0.1  # Check sequence progress
        elif self.state['phase'] == 'waiting_for_responses':
            return 1.0  # Timeout for responses
        return INFINITY
        
    def outputFnc(self):
        if self.state['phase'] == 'executing_sequence':
            if self.state['current_step'] < len(self.state['operation_sequence']):
                step = self.state['operation_sequence'][self.state['current_step']]
                
                event = TransitionEvent(
                    transition=step['transition'],
                    request_id=f"{self.state['current_operation']}_{self.state['current_step']}"
                )
                
                self.state['waiting_for_responses'][step['node']] = event.request_id
                
                trace_logger.log_event(
                    "lifecycle_manager_send_transition",
                    {
                        "operation": self.state['current_operation'],
                        "step": self.state['current_step'],
                        "node": step['node'],
                        "transition": step['transition'].name
                    },
                    self.context_key
                )
                
                return {self.transition_out: (step['node'], event)}
                
        elif self.state['phase'] == 'operation_complete':
            # Report operation status
            success = len(self.state['waiting_for_responses']) == 0
            
            status = {
                'operation': self.state['current_operation'],
                'success': success,
                'node_states': self.state['node_states'].copy()
            }
            
            trace_logger.log_event(
                "lifecycle_manager_operation_complete",
                {
                    "operation": self.state['current_operation'],
                    "success": success
                },
                self.context_key
            )
            
            return {self.status_out: status}
            
        return {}
        
    def intTransition(self):
        if self.state['phase'] == 'executing_sequence':
            self.state['phase'] = 'waiting_for_responses'
            self.state['current_step'] += 1
            
        elif self.state['phase'] == 'waiting_for_responses':
            if not self.state['waiting_for_responses']:
                # All responses received
                if self.state['current_step'] < len(self.state['operation_sequence']):
                    self.state['phase'] = 'executing_sequence'
                else:
                    self.state['phase'] = 'operation_complete'
            else:
                # Still waiting - check for timeout
                self.state['phase'] = 'waiting_for_responses'
                
        elif self.state['phase'] == 'operation_complete':
            self.state['phase'] = 'idle'
            self.state['current_operation'] = None
            self.state['operation_sequence'] = []
            self.state['current_step'] = 0
            
        return self.state
        
    def extTransition(self, inputs):
        # Handle commands
        if self.command_in in inputs:
            cmd = inputs[self.command_in]
            if isinstance(cmd, str) and self.state['phase'] == 'idle':
                if cmd == 'startup':
                    self._start_startup_sequence()
                elif cmd == 'shutdown':
                    self._start_shutdown_sequence()
                    
        # Handle transition results
        if self.result_in in inputs:
            node, result = inputs[self.result_in]
            if isinstance(result, TransitionResult) and node in self.state['waiting_for_responses']:
                del self.state['waiting_for_responses'][node]
                
                if result.success:
                    self.state['node_states'][node] = result.to_state
                    
        return self.state
        
    def _start_startup_sequence(self):
        """Start system startup sequence"""
        self.state['current_operation'] = 'startup'
        self.state['operation_sequence'] = []
        
        # Configure all nodes
        for node in self.managed_nodes:
            self.state['operation_sequence'].append({
                'node': node,
                'transition': LifecycleTransition.CONFIGURE
            })
            
        # Then activate all nodes
        for node in self.managed_nodes:
            self.state['operation_sequence'].append({
                'node': node,
                'transition': LifecycleTransition.ACTIVATE
            })
            
        self.state['current_step'] = 0
        self.state['phase'] = 'executing_sequence'
        
    def _start_shutdown_sequence(self):
        """Start system shutdown sequence"""
        self.state['current_operation'] = 'shutdown'
        self.state['operation_sequence'] = []
        
        # Deactivate all nodes (in reverse order)
        for node in reversed(self.managed_nodes):
            self.state['operation_sequence'].append({
                'node': node,
                'transition': LifecycleTransition.DEACTIVATE
            })
            
        # Then cleanup all nodes
        for node in reversed(self.managed_nodes):
            self.state['operation_sequence'].append({
                'node': node,
                'transition': LifecycleTransition.CLEANUP
            })
            
        # Finally shutdown all nodes
        for node in reversed(self.managed_nodes):
            self.state['operation_sequence'].append({
                'node': node,
                'transition': LifecycleTransition.SHUTDOWN
            })
            
        self.state['current_step'] = 0
        self.state['phase'] = 'executing_sequence'