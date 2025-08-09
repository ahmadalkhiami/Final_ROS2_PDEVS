"""
Configuration for ROS2 DEVS simulation.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import yaml

@dataclass
class DDSConfig:
    """DDS layer configuration"""
    domain_id: int = 0
    discovery_period_ms: int = 100
    participant_lease_duration_ms: int = 10000
    max_participants: int = 1000
    max_readers: int = 5000
    max_writers: int = 5000
    enable_shared_memory: bool = True
    enable_security: bool = False
    
@dataclass
class LoggingConfig:
    """Logging configuration"""
    trace_to_file: bool = True
    trace_file_path: str = "ros2_trace.log"
    trace_to_console: bool = True
    default_log_level: str = "INFO"

@dataclass
class ExecutorConfig:
    """Executor configuration"""
    spin_period_us: int = 100
    callback_duration_us: int = 10

@dataclass
class NetworkConfig:
    """Network configuration"""
    lan_latency_us: int = 1000  # 1ms LAN latency
    wan_latency_us: int = 50000  # 50ms WAN latency
    lan_loss_rate: float = 0.001  # 0.1% packet loss
    # Jitter configuration (added)
    jitter_us_mean: float = 0.0
    jitter_us_std: float = 0.0

@dataclass
class SimulationConfig:
    """Complete simulation configuration"""
    # System components
    enable_map_server: bool = True
    enable_robot_state_publisher: bool = True
    enable_joint_state_publisher: bool = True
    enable_laser_scanner: bool = True
    
    # Communication settings
    enable_parameter_services: bool = True
    enable_diagnostics: bool = True
    
    # Performance settings
    simulation_time_seconds: float = 10.0
    time_scale: float = 1.0
    
    # Layer configurations
    dds: DDSConfig = field(default_factory=DDSConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    executor: ExecutorConfig = field(default_factory=ExecutorConfig)
    network: NetworkConfig = field(default_factory=NetworkConfig)
    
    @classmethod
    def from_yaml(cls, file_path: str) -> 'SimulationConfig':
        """Load configuration from YAML file"""
        with open(file_path, 'r') as f:
            data = yaml.safe_load(f)
        return cls(**data)

class ConfigPresets:
    """Common configuration presets"""
    
    @staticmethod
    def development() -> SimulationConfig:
        """Development configuration with all debug features enabled"""
        config = SimulationConfig()
        config.logging.default_log_level = "DEBUG"
        return config
        
    @staticmethod
    def production() -> SimulationConfig:
        """Production configuration optimized for performance"""
        config = SimulationConfig()
        config.logging.trace_to_console = False
        config.executor.spin_period_us = 50
        return config
        
    @staticmethod
    def testing() -> SimulationConfig:
        """Testing configuration with predictable behavior"""
        config = SimulationConfig()
        config.time_scale = 0.1
        return config
        
    @staticmethod
    def benchmark() -> SimulationConfig:
        """Benchmark configuration for performance testing"""
        config = SimulationConfig()
        config.logging.trace_to_console = False
        config.logging.trace_to_file = True
        config.time_scale = 10.0
        return config

# Global configuration instance
config = SimulationConfig() 