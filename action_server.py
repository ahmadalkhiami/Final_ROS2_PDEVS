"""
ROS2 Action Server implementation.
"""

from pypdevs.DEVS import AtomicDEVS
from pypdevs.infinity import INFINITY
import time
import uuid
from typing import Optional, Dict, Any, Callable
from enum import Enum, auto
from dataclasses import dataclass
from tracing import trace_logger
from context import context_manager
from configuration import config


class GoalStatus(Enum):
    """Action goal status"""
    PENDING = auto()
    EXECUTING = auto()
    SUCCEEDED = auto()
    ABORTED = auto()
    CANCELED = auto()
    

@dataclass
class ActionGoal:
    """Action goal request"""
    goal_id: str
    goal_data: Any
    client_id: str
    timestamp: float = 0.0
    

@dataclass
class ActionFeedback:
    """Action feedback message"""
    goal_id: str
    progress: float  # 0.0 to 1.0
    feedback_data: Any
    timestamp: float = 0.0
    

@dataclass
class ActionResult:
    """Action result message"""
    goal_id: str
    status: GoalStatus
    result_data: Any
    timestamp: float = 0.0


class ActionServer(AtomicDEVS):
    """
    ROS2 Action Server for long-running, preemptible tasks.
    """
    
    def __init__(self, name: str, node_name: str, action_name: str,
                 execute_callback: Optional[Callable] = None,
                 goal_callback: Optional[Callable] = None,
                 cancel_callback: Optional[Callable] = None,
                 max_concurrent_goals: int = 1):
        AtomicDEVS.__init__(self, name)
        
        # Configuration
        self.node_name = node_name
        self.action_name = action_name
        self.max_concurrent_goals = max_concurrent_goals
        
        # Callbacks
        self._execute_callback = execute_callback or self._default_execute
        self._goal_callback = goal_callback or self._default_goal_callback
        self._cancel_callback = cancel_callback or self._default_cancel_callback
        
        # State
        self.state = {
            'phase': 'idle',
            'active_goals': {},  # goal_id -> goal_info
            'pending_goals': [],
            'pending_cancellations': [],
            'current_goal': None,
            'feedback_rate': 0.1,  # Feedback every 100ms
            'last_feedback_time': 0.0
        }
        
        # Ports
        self.goal_in = self.addInPort("goals")
        self.cancel_in = self.addInPort("cancellations")
        self.goal_response_out = self.addOutPort("goal_responses")
        self.feedback_out = self.addOutPort("feedback")
        self.result_out = self.addOutPort("results")
        
        # Register context
        self.context_key = context_manager.register_component(
            f"action_server_{action_name}",
            "action_server",
            node_name
        )
        
    def timeAdvance(self):
        if self.state['pending_goals']:
            return 0.001  # Process goal quickly
            
        elif self.state['pending_cancellations']:
            return 0.001  # Process cancellation quickly
            
        elif self.state['current_goal']:
            # Check if need to send feedback
            time_since_feedback = time.time() - self.state['last_feedback_time']
            if time_since_feedback >= self.state['feedback_rate']:
                return 0.0  # Send feedback now
            else:
                # Wait until next feedback or completion
                remaining = self.state['feedback_rate'] - time_since_feedback
                goal_info = self.state['active_goals'][self.state['current_goal']]
                
                # Check if goal is complete
                if goal_info['progress'] >= 1.0:
                    return 0.001  # Complete goal
                    
                return remaining
                
        return INFINITY
        
    def outputFnc(self):
        if self.state['pending_goals']:
            goal = self.state['pending_goals'][0]
            
            # Check if can accept goal
            if len(self.state['active_goals']) < self.max_concurrent_goals:
                # Accept goal
                accepted = self._goal_callback(goal)
                
                if accepted:
                    goal_info = {
                        'goal': goal,
                        'status': GoalStatus.PENDING,
                        'progress': 0.0,
                        'start_time': time.time(),
                        'executor_data': {}
                    }
                    
                    self.state['active_goals'][goal.goal_id] = goal_info
                    
                    trace_logger.log_event(
                        "action_goal_accepted",
                        {
                            "action": self.action_name,
                            "goal_id": goal.goal_id,
                            "client": goal.client_id
                        },
                        self.context_key
                    )
                    
                    return {self.goal_response_out: {
                        'goal_id': goal.goal_id,
                        'accepted': True
                    }}
                else:
                    # Reject goal
                    trace_logger.log_event(
                        "action_goal_rejected",
                        {
                            "action": self.action_name,
                            "goal_id": goal.goal_id,
                            "reason": "goal_callback_rejected"
                        },
                        self.context_key
                    )
                    
                    return {self.goal_response_out: {
                        'goal_id': goal.goal_id,
                        'accepted': False
                    }}
            else:
                # Server busy
                trace_logger.log_event(
                    "action_goal_rejected",
                    {
                        "action": self.action_name,
                        "goal_id": goal.goal_id,
                        "reason": "server_busy"
                    },
                    self.context_key
                )
                
                return {self.goal_response_out: {
                    'goal_id': goal.goal_id,
                    'accepted': False,
                    'reason': 'server_busy'
                }}
                
        elif self.state['pending_cancellations']:
            cancel_req = self.state['pending_cancellations'][0]
            goal_id = cancel_req['goal_id']
            
            if goal_id in self.state['active_goals']:
                # Cancel goal
                goal_info = self.state['active_goals'][goal_id]
                if self._cancel_callback(goal_info['goal']):
                    goal_info['status'] = GoalStatus.CANCELED
                    
                    result = ActionResult(
                        goal_id=goal_id,
                        status=GoalStatus.CANCELED,
                        result_data={'canceled_at_progress': goal_info['progress']},
                        timestamp=time.time()
                    )
                    
                    trace_logger.log_event(
                        "action_goal_canceled",
                        {
                            "action": self.action_name,
                            "goal_id": goal_id,
                            "progress": goal_info['progress']
                        },
                        self.context_key
                    )
                    
                    return {self.result_out: result}
                    
        elif self.state['current_goal']:
            goal_id = self.state['current_goal']
            goal_info = self.state['active_goals'][goal_id]
            
            # Send feedback
            if time.time() - self.state['last_feedback_time'] >= self.state['feedback_rate']:
                feedback = ActionFeedback(
                    goal_id=goal_id,
                    progress=goal_info['progress'],
                    feedback_data=goal_info.get('feedback_data', {}),
                    timestamp=time.time()
                )
                
                self.state['last_feedback_time'] = time.time()
                
                trace_logger.log_event(
                    "action_feedback",
                    {
                        "action": self.action_name,
                        "goal_id": goal_id,
                        "progress": f"{goal_info['progress']*100:.1f}%"
                    },
                    self.context_key
                )
                
                return {self.feedback_out: feedback}
                
            # Check if goal complete
            if goal_info['progress'] >= 1.0:
                result = ActionResult(
                    goal_id=goal_id,
                    status=goal_info['status'],
                    result_data=goal_info.get('result_data', {}),
                    timestamp=time.time()
                )
                
                trace_logger.log_event(
                    "action_goal_complete",
                    {
                        "action": self.action_name,
                        "goal_id": goal_id,
                        "status": goal_info['status'].name,
                        "duration": time.time() - goal_info['start_time']
                    },
                    self.context_key
                )
                
                return {self.result_out: result}
                
        return {}
        
    def intTransition(self):
        if self.state['pending_goals']:
            self.state['pending_goals'].pop(0)
            
        elif self.state['pending_cancellations']:
            cancel_req = self.state['pending_cancellations'].pop(0)
            goal_id = cancel_req['goal_id']
            
            # Remove canceled goal
            if goal_id in self.state['active_goals']:
                del self.state['active_goals'][goal_id]
                if self.state['current_goal'] == goal_id:
                    self.state['current_goal'] = None
                    
        elif self.state['current_goal']:
            goal_id = self.state['current_goal']
            goal_info = self.state['active_goals'][goal_id]
            
            if goal_info['progress'] >= 1.0:
                # Goal complete - remove it
                del self.state['active_goals'][goal_id]
                self.state['current_goal'] = None
                
        # Start executing next goal if available
        if not self.state['current_goal'] and self.state['active_goals']:
            # Find pending goal
            for goal_id, goal_info in self.state['active_goals'].items():
                if goal_info['status'] == GoalStatus.PENDING:
                    self.state['current_goal'] = goal_id
                    goal_info['status'] = GoalStatus.EXECUTING
                    
                    # Start execution
                    self._start_execution(goal_info)
                    break
                    
        return self.state
        
    def extTransition(self, inputs):
        # Handle new goals
        if self.goal_in in inputs:
            goal = inputs[self.goal_in]
            if isinstance(goal, ActionGoal):
                self.state['pending_goals'].append(goal)
                
        # Handle cancellation requests
        if self.cancel_in in inputs:
            cancel_req = inputs[self.cancel_in]
            if isinstance(cancel_req, dict) and 'goal_id' in cancel_req:
                self.state['pending_cancellations'].append(cancel_req)
                
        return self.state
        
    def _start_execution(self, goal_info: Dict):
        """Start executing a goal"""
        # Call execute callback in separate thread (simulated)
        try:
            self._execute_callback(
                goal_info['goal'],
                lambda p, d: self._update_progress(goal_info['goal'].goal_id, p, d),
                lambda r: self._set_result(goal_info['goal'].goal_id, r)
            )
        except Exception as e:
            # Execution failed
            goal_info['status'] = GoalStatus.ABORTED
            goal_info['result_data'] = {'error': str(e)}
            goal_info['progress'] = 1.0
            
    def _update_progress(self, goal_id: str, progress: float, feedback_data: Any):
        """Update goal progress"""
        if goal_id in self.state['active_goals']:
            goal_info = self.state['active_goals'][goal_id]
            goal_info['progress'] = min(1.0, max(0.0, progress))
            goal_info['feedback_data'] = feedback_data
            
    def _set_result(self, goal_id: str, result_data: Any):
        """Set goal result"""
        if goal_id in self.state['active_goals']:
            goal_info = self.state['active_goals'][goal_id]
            goal_info['result_data'] = result_data
            goal_info['status'] = GoalStatus.SUCCEEDED
            goal_info['progress'] = 1.0
            
    # Default callbacks
    def _default_goal_callback(self, goal: ActionGoal) -> bool:
        """Default goal callback - accept all goals"""
        return True
        
    def _default_cancel_callback(self, goal: ActionGoal) -> bool:
        """Default cancel callback - allow all cancellations"""
        return True
        
    def _default_execute(self, goal: ActionGoal, 
                        feedback_callback: Callable,
                        result_callback: Callable):
        """Default execute callback - simple progress simulation"""
        # Simulate execution with progress updates
        steps = 10
        for i in range(steps):
            time.sleep(0.1)  # Simulate work
            progress = (i + 1) / steps
            feedback_callback(progress, {'step': i + 1, 'total': steps})
            
        # Set result
        result_callback({'success': True, 'message': 'Goal completed'})