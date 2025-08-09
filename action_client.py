"""
ROS2 Action Client implementation.
"""

from pypdevs.DEVS import AtomicDEVS
from pypdevs.infinity import INFINITY
import time
import uuid
from typing import Optional, Dict, Any, Callable
from dataclasses import dataclass

from action import ActionGoal, ActionFeedback, ActionResult, GoalStatus
from tracing import trace_logger
from context import context_manager
from configuration import config


class ActionClient(AtomicDEVS):
    """
    ROS2 Action Client for sending goals and monitoring execution.
    """
    
    def __init__(self, name: str, node_name: str, action_name: str,
                 feedback_callback: Optional[Callable] = None,
                 result_callback: Optional[Callable] = None):
        AtomicDEVS.__init__(self, name)
        
        # Configuration
        self.node_name = node_name
        self.action_name = action_name
        
        # Callbacks
        self._feedback_callback = feedback_callback or self._default_feedback_callback
        self._result_callback = result_callback or self._default_result_callback
        
        # State
        self.state = {
            'phase': 'idle',
            'pending_goals': [],
            'active_goals': {},  # goal_id -> goal_info
            'completed_goals': {},
            'cancel_requests': []
        }
        
        # Ports
        self.goal_out = self.addOutPort("goals")
        self.cancel_out = self.addOutPort("cancellations")
        self.goal_response_in = self.addInPort("goal_responses")
        self.feedback_in = self.addInPort("feedback")
        self.result_in = self.addInPort("results")
        
        # Register context
        self.context_key = context_manager.register_component(
            f"action_client_{action_name}",
            "action_client",
            node_name
        )
        
    def timeAdvance(self):
        if self.state['pending_goals']:
            return 0.001  # Send goal quickly
            
        elif self.state['cancel_requests']:
            return 0.001  # Send cancellation quickly
            
        return INFINITY
        
    def outputFnc(self):
        if self.state['pending_goals']:
            goal_data = self.state['pending_goals'][0]
            
            # Create goal message
            goal = ActionGoal(
                goal_id=str(uuid.uuid4()),
                goal_data=goal_data,
                client_node=self.node_name,
                timestamp=time.time()
            )
            
            # Track goal
            self.state['active_goals'][goal.goal_id] = {
                'goal': goal,
                'status': 'pending',
                'feedback_count': 0,
                'send_time': time.time()
            }
            
            trace_logger.log_event(
                "action_send_goal",
                {
                    "action": self.action_name,
                    "goal_id": goal.goal_id,
                    "client": self.node_name
                },
                self.context_key
            )
            
            return {self.goal_out: goal}
            
        elif self.state['cancel_requests']:
            goal_id = self.state['cancel_requests'][0]
            
            if goal_id in self.state['active_goals']:
                trace_logger.log_event(
                    "action_send_cancel",
                    {
                        "action": self.action_name,
                        "goal_id": goal_id
                    },
                    self.context_key
                )
                
                return {self.cancel_out: {'goal_id': goal_id}}
                
        return {}
        
    def intTransition(self):
        if self.state['pending_goals']:
            self.state['pending_goals'].pop(0)
            
        elif self.state['cancel_requests']:
            self.state['cancel_requests'].pop(0)
            
        return self.state
        
    def extTransition(self, inputs):
        # Handle goal responses
        if self.goal_response_in in inputs:
            response = inputs[self.goal_response_in]
            if isinstance(response, dict):
                goal_id = response['goal_id']
                if goal_id in self.state['active_goals']:
                    goal_info = self.state['active_goals'][goal_id]
                    
                    if response['accepted']:
                        goal_info['status'] = 'accepted'
                        
                        trace_logger.log_event(
                            "action_goal_accepted",
                            {
                                "action": self.action_name,
                                "goal_id": goal_id
                            },
                            self.context_key
                        )
                    else:
                        goal_info['status'] = 'rejected'
                        # Move to completed
                        self.state['completed_goals'][goal_id] = goal_info
                        del self.state['active_goals'][goal_id]
                        
                        trace_logger.log_event(
                            "action_goal_rejected",
                            {
                                "action": self.action_name,
                                "goal_id": goal_id,
                                "reason": response.get('reason', 'unknown')
                            },
                            self.context_key
                        )
                        
        # Handle feedback
        if self.feedback_in in inputs:
            feedback = inputs[self.feedback_in]
            if isinstance(feedback, ActionFeedback):
                if feedback.goal_id in self.state['active_goals']:
                    goal_info = self.state['active_goals'][feedback.goal_id]
                    goal_info['feedback_count'] += 1
                    goal_info['last_feedback'] = feedback
                    
                    # Call feedback callback
                    self._feedback_callback(feedback)
                    
                    trace_logger.log_event(
                        "action_feedback_received",
                        {
                            "action": self.action_name,
                            "goal_id": feedback.goal_id,
                            "progress": f"{feedback.progress_percent:.1f}%",
                            "feedback_count": goal_info['feedback_count']
                        },
                        self.context_key
                    )
                    
        # Handle results
        if self.result_in in inputs:
            result = inputs[self.result_in]
            if isinstance(result, ActionResult):
                if result.goal_id in self.state['active_goals']:
                    goal_info = self.state['active_goals'][result.goal_id]
                    goal_info['result'] = result
                    goal_info['completion_time'] = time.time()
                    goal_info['duration'] = time.time() - goal_info['send_time']
                    
                    # Move to completed
                    self.state['completed_goals'][result.goal_id] = goal_info
                    del self.state['active_goals'][result.goal_id]
                    
                    # Call result callback
                    self._result_callback(result)
                    
                    trace_logger.log_event(
                        "action_result_received",
                        {
                            "action": self.action_name,
                            "goal_id": result.goal_id,
                            "status": result.status.value,
                            "duration": goal_info['duration'],
                            "feedback_count": goal_info['feedback_count']
                        },
                        self.context_key
                    )
                    
        return self.state
        
    def send_goal(self, goal_data: Any):
        """Send a new goal to the action server"""
        self.state['pending_goals'].append(goal_data)
        
    def cancel_goal(self, goal_id: str):
        """Request cancellation of a goal"""
        if goal_id in self.state['active_goals']:
            self.state['cancel_requests'].append(goal_id)
            
    def cancel_all_goals(self):
        """Cancel all active goals"""
        for goal_id in list(self.state['active_goals'].keys()):
            self.state['cancel_requests'].append(goal_id)
            
    def get_result(self, goal_id: str) -> Optional[ActionResult]:
        """Get result for a completed goal"""
        if goal_id in self.state['completed_goals']:
            return self.state['completed_goals'][goal_id].get('result')
        return None
        
    def is_goal_active(self, goal_id: str) -> bool:
        """Check if a goal is still active"""
        return goal_id in self.state['active_goals']
        
    def wait_for_result(self, goal_id: str, timeout: float = None) -> Optional[ActionResult]:
        """Wait for a goal to complete (simulated)"""
        # In real implementation, this would block
        # For simulation, we just check if complete
        return self.get_result(goal_id)
        
    # Default callbacks
    def _default_feedback_callback(self, feedback: ActionFeedback):
        """Default feedback callback"""
        pass
        
    def _default_result_callback(self, result: ActionResult):
        """Default result callback"""
        pass
        
    def get_statistics(self) -> Dict[str, Any]:
        """Get client statistics"""
        total_goals = len(self.state['completed_goals']) + len(self.state['active_goals'])
        
        # Calculate success rate
        succeeded = sum(1 for g in self.state['completed_goals'].values()
                       if g.get('result') and g['result'].status == GoalStatus.STATUS_SUCCEEDED)
        
        # Calculate average duration
        durations = [g['duration'] for g in self.state['completed_goals'].values()
                    if 'duration' in g]
        avg_duration = sum(durations) / len(durations) if durations else 0.0
        
        return {
            'total_goals_sent': total_goals,
            'active_goals': len(self.state['active_goals']),
            'completed_goals': len(self.state['completed_goals']),
            'success_rate': succeeded / len(self.state['completed_goals']) if self.state['completed_goals'] else 0.0,
            'average_duration': avg_duration
        }