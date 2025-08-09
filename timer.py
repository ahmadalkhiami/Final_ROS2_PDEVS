"""
Timer-related messages for ROS2 DEVS simulation.
"""

from dataclasses import dataclass
import time
from typing import Dict, List, Optional

from message import Message, MessageType


class Timer:
    """Timer class for ROS2 simulation"""
    
    def __init__(self, period: float = 1.0):
        """Initialize timer with period in seconds"""
        # Clamp period to minimum positive value
        self.period = max(0.001, period)  # Minimum 1ms period
        self.last_trigger_time = time.time()
    
    def is_ready(self) -> bool:
        """Check if timer is ready to trigger"""
        current_time = time.time()
        return current_time >= self.last_trigger_time + self.period
    
    def trigger(self):
        """Trigger the timer and update last trigger time"""
        self.last_trigger_time = time.time()
    
    def reset(self):
        """Reset timer to current time"""
        self.last_trigger_time = time.time()
    
    def get_time_until_next(self) -> float:
        """Get time until next trigger"""
        current_time = time.time()
        next_trigger = self.last_trigger_time + self.period
        return max(0.0, next_trigger - current_time)


class TimerManager:
    """Manager for multiple timers"""
    
    def __init__(self):
        self.timers: Dict[int, Timer] = {}
    
    def add_timer(self, timer_id: int, period: float):
        """Add a timer with given ID and period"""
        self.timers[timer_id] = Timer(period)
    
    def remove_timer(self, timer_id: int):
        """Remove a timer by ID"""
        if timer_id in self.timers:
            del self.timers[timer_id]
    
    def get_expired_timers(self) -> List[int]:
        """Get list of expired timer IDs"""
        expired = []
        for timer_id, timer in self.timers.items():
            if timer.is_ready():
                expired.append(timer_id)
        return expired
    
    def get_next_expiration(self) -> Optional[float]:
        """Get time until next timer expiration"""
        if not self.timers:
            return None
        
        next_time = None
        for timer in self.timers.values():
            time_until = timer.get_time_until_next()
            if next_time is None or time_until < next_time:
                next_time = time_until
        
        if next_time is not None:
            return time.time() + next_time
        return None
    
    def update(self):
        """Update all timers"""
        # This method can be used to trigger expired timers
        expired = self.get_expired_timers()
        for timer_id in expired:
            if timer_id in self.timers:
                self.timers[timer_id].trigger()


@dataclass
class TimerEvent(Message):
    """Timer event message"""
    timer_id: str = ""
    period_ms: float = 0.0
    expected_trigger_time: float = 0.0
    actual_trigger_time: float = 0.0
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.DATA
        self.actual_trigger_time = time.time()
        
    def calculate_jitter_ms(self) -> float:
        """Calculate timer jitter in milliseconds"""
        if self.expected_trigger_time > 0:
            return abs(self.actual_trigger_time - self.expected_trigger_time) * 1000
        return 0.0
        

@dataclass
class ClockMessage(Message):
    """ROS2 Clock message for /clock topic"""
    clock_sec: int = 0
    clock_nanosec: int = 0
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.DATA
        self.topic = "/clock"
        
    @classmethod
    def from_timestamp(cls, timestamp: float) -> 'ClockMessage':
        """Create clock message from timestamp"""
        sec = int(timestamp)
        nanosec = int((timestamp - sec) * 1e9)
        return cls(clock_sec=sec, clock_nanosec=nanosec)
        
    def to_timestamp(self) -> float:
        """Convert to timestamp"""
        return self.clock_sec + self.clock_nanosec / 1e9
