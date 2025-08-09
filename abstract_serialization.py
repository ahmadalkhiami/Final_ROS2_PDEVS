"""
Abstract Serialization/Deserialization for ROS2 DEVS Performance Modeling.
Models serialization latency and throughput impacts without actual data transformation.
"""

import time
import math
from typing import Any, Dict, Optional
from dataclasses import dataclass
from enum import Enum
from abc import ABC, abstractmethod

from tracing import trace_logger


class SerializationFormat(Enum):
    """Serialization format types"""
    CDR = "cdr"
    JSON = "json"
    PROTOBUF = "protobuf"
    MSGPACK = "msgpack"


@dataclass
class SerializationConfig:
    """Configuration for serialization performance modeling"""
    # Base latencies (microseconds)
    base_serialize_latency_us: float = 10.0
    base_deserialize_latency_us: float = 8.0
    
    # Throughput (bytes per microsecond)
    serialize_throughput_bps_us: float = 100.0  # ~100 MB/s
    deserialize_throughput_bps_us: float = 150.0  # ~150 MB/s
    
    # CPU overhead factors
    cpu_overhead_factor: float = 1.0
    memory_overhead_factor: float = 1.0
    
    # Format-specific multipliers
    format_multipliers: Dict[SerializationFormat, float] = None
    
    def __post_init__(self):
        if self.format_multipliers is None:
            self.format_multipliers = {
                SerializationFormat.CDR: 1.0,        # Baseline (fastest)
                SerializationFormat.PROTOBUF: 1.2,   # 20% slower
                SerializationFormat.JSON: 2.5,       # 150% slower
                SerializationFormat.MSGPACK: 1.1,    # 10% slower
            }


@dataclass
class MessageSize:
    """Estimated message size for different data types"""
    
    @staticmethod
    def estimate_size(data: Any) -> int:
        """Estimate serialized size in bytes"""
        if data is None:
            return 4  # Null indicator
        elif isinstance(data, bool):
            return 1
        elif isinstance(data, int):
            if -128 <= data <= 127:
                return 1
            elif -32768 <= data <= 32767:
                return 2
            elif -2147483648 <= data <= 2147483647:
                return 4
            else:
                return 8
        elif isinstance(data, float):
            return 8
        elif isinstance(data, str):
            return len(data.encode('utf-8')) + 4  # String + length prefix
        elif isinstance(data, bytes):
            return len(data) + 4  # Bytes + length prefix
        elif isinstance(data, (list, tuple)):
            return 4 + sum(MessageSize.estimate_size(item) for item in data)
        elif isinstance(data, dict):
            return 4 + sum(
                MessageSize.estimate_size(k) + MessageSize.estimate_size(v) 
                for k, v in data.items()
            )
        elif hasattr(data, '__dict__'):
            # Dataclass or object
            return sum(
                MessageSize.estimate_size(getattr(data, attr, None))
                for attr in dir(data) 
                if not attr.startswith('_')
            ) + 20  # Object overhead
        else:
            return 50  # Default estimate for unknown types


class AbstractSerializer(ABC):
    """Abstract base class for performance-modeling serializers"""
    
    def __init__(self, config: SerializationConfig, format_type: SerializationFormat):
        self.config = config
        self.format = format_type
        self.format_multiplier = config.format_multipliers[format_type]
        
    @abstractmethod
    def serialize(self, data: Any) -> 'SerializationResult':
        """Serialize data and return performance metrics"""
        pass
        
    @abstractmethod  
    def deserialize(self, data: Any, size_bytes: int) -> 'DeserializationResult':
        """Deserialize data and return performance metrics"""
        pass


@dataclass
class SerializationResult:
    """Result of serialization operation"""
    serialized_data: Any  # Could be actual data or just a placeholder
    size_bytes: int
    latency_us: float
    cpu_cycles: int
    memory_bytes: int


@dataclass
class DeserializationResult:
    """Result of deserialization operation"""
    deserialized_data: Any
    latency_us: float
    cpu_cycles: int
    memory_bytes: int


class PerformanceSerializer(AbstractSerializer):
    """Performance-focused serializer for ROS2 DEVS simulation"""
    
    def __init__(self, config: SerializationConfig = None, 
                 format_type: SerializationFormat = SerializationFormat.CDR):
        if config is None:
            config = SerializationConfig()
        super().__init__(config, format_type)
        
    def serialize(self, data: Any) -> SerializationResult:
        """Model serialization performance"""
        start_time = time.time()
        
        # Estimate message size
        size_bytes = MessageSize.estimate_size(data)
        
        # Calculate latency components
        base_latency = self.config.base_serialize_latency_us * self.format_multiplier
        throughput_latency = size_bytes / self.config.serialize_throughput_bps_us
        cpu_overhead = base_latency * self.config.cpu_overhead_factor
        
        total_latency_us = base_latency + throughput_latency + cpu_overhead
        
        # Estimate CPU cycles (assuming 3GHz CPU)
        cpu_cycles = int(total_latency_us * 3.0)  # 3 cycles per microsecond
        
        # Estimate memory usage (input + output + overhead)
        memory_bytes = int(size_bytes * (1.5 + self.config.memory_overhead_factor))
        
        # Simulate serialization delay in real-time simulation
        if hasattr(self.config, 'real_time_simulation') and self.config.real_time_simulation:
            time.sleep(total_latency_us / 1e6)
            
        # Create placeholder serialized data (for performance modeling)
        serialized_data = f"<serialized_{self.format.value}_{size_bytes}_bytes>"
        
        # Log performance event
        trace_logger.log_event(
            "serialize_performance",
            {
                "format": self.format.value,
                "size_bytes": size_bytes,
                "latency_us": total_latency_us,
                "cpu_cycles": cpu_cycles,
                "memory_bytes": memory_bytes,
                "throughput_mbps": (size_bytes * 8) / total_latency_us if total_latency_us > 0 else 0
            }
        )
        
        return SerializationResult(
            serialized_data=serialized_data,
            size_bytes=size_bytes,
            latency_us=total_latency_us,
            cpu_cycles=cpu_cycles,
            memory_bytes=memory_bytes
        )
        
    def deserialize(self, data: Any, size_bytes: int) -> DeserializationResult:
        """Model deserialization performance"""
        start_time = time.time()
        
        # Calculate latency components  
        base_latency = self.config.base_deserialize_latency_us * self.format_multiplier
        throughput_latency = size_bytes / self.config.deserialize_throughput_bps_us
        cpu_overhead = base_latency * self.config.cpu_overhead_factor
        
        total_latency_us = base_latency + throughput_latency + cpu_overhead
        
        # Estimate CPU cycles
        cpu_cycles = int(total_latency_us * 3.0)
        
        # Estimate memory usage
        memory_bytes = int(size_bytes * (1.2 + self.config.memory_overhead_factor))
        
        # Simulate deserialization delay
        if hasattr(self.config, 'real_time_simulation') and self.config.real_time_simulation:
            time.sleep(total_latency_us / 1e6)
            
        # Create placeholder deserialized data
        deserialized_data = f"<deserialized_data_{size_bytes}_bytes>"
        
        # Log performance event
        trace_logger.log_event(
            "deserialize_performance", 
            {
                "format": self.format.value,
                "size_bytes": size_bytes,
                "latency_us": total_latency_us,
                "cpu_cycles": cpu_cycles,
                "memory_bytes": memory_bytes,
                "throughput_mbps": (size_bytes * 8) / total_latency_us if total_latency_us > 0 else 0
            }
        )
        
        return DeserializationResult(
            deserialized_data=deserialized_data,
            latency_us=total_latency_us,
            cpu_cycles=cpu_cycles,
            memory_bytes=memory_bytes
        )


class AdaptiveSerializer(PerformanceSerializer):
    """Adaptive serializer that adjusts performance based on system load"""
    
    def __init__(self, config: SerializationConfig = None,
                 format_type: SerializationFormat = SerializationFormat.CDR):
        super().__init__(config, format_type)
        self.system_load = 0.0  # 0.0 to 1.0
        self.message_rate = 0.0  # messages per second
        
    def update_system_conditions(self, cpu_load: float, memory_usage: float, 
                                network_load: float, message_rate: float):
        """Update system conditions that affect serialization performance"""
        # Weighted system load
        self.system_load = (cpu_load * 0.5 + memory_usage * 0.3 + network_load * 0.2)
        self.message_rate = message_rate
        
    def serialize(self, data: Any) -> SerializationResult:
        """Serialize with adaptive performance based on system conditions"""
        # Get base result
        result = super().serialize(data)
        
        # Apply system load penalties
        load_penalty = 1.0 + (self.system_load * 2.0)  # Up to 3x slower under full load
        rate_penalty = 1.0 + min(self.message_rate / 1000.0, 1.0)  # Penalty for high message rates
        
        total_penalty = load_penalty * rate_penalty
        
        # Adjust performance metrics
        result.latency_us *= total_penalty
        result.cpu_cycles = int(result.cpu_cycles * total_penalty)
        result.memory_bytes = int(result.memory_bytes * (1.0 + self.system_load * 0.5))
        
        # Log adaptive performance
        trace_logger.log_event(
            "adaptive_serialize_performance",
            {
                "original_latency_us": result.latency_us / total_penalty,
                "adjusted_latency_us": result.latency_us,
                "system_load": self.system_load,
                "message_rate": self.message_rate,
                "performance_penalty": total_penalty
            }
        )
        
        return result
        
    def deserialize(self, data: Any, size_bytes: int) -> DeserializationResult:
        """Deserialize with adaptive performance"""
        result = super().deserialize(data, size_bytes)
        
        # Apply similar penalties as serialization
        load_penalty = 1.0 + (self.system_load * 1.5)  # Deserialization less affected
        rate_penalty = 1.0 + min(self.message_rate / 1500.0, 0.8)
        
        total_penalty = load_penalty * rate_penalty
        
        result.latency_us *= total_penalty
        result.cpu_cycles = int(result.cpu_cycles * total_penalty)
        result.memory_bytes = int(result.memory_bytes * (1.0 + self.system_load * 0.3))
        
        return result


class SerializationProfiler:
    """Profiler for analyzing serialization performance patterns"""
    
    def __init__(self):
        self.serialization_stats = []
        self.deserialization_stats = []
        
    def record_serialization(self, data_type: str, result: SerializationResult):
        """Record serialization performance"""
        self.serialization_stats.append({
            'timestamp': time.time(),
            'data_type': data_type,
            'size_bytes': result.size_bytes,
            'latency_us': result.latency_us,
            'throughput_mbps': (result.size_bytes * 8) / result.latency_us if result.latency_us > 0 else 0
        })
        
    def record_deserialization(self, data_type: str, result: DeserializationResult):
        """Record deserialization performance"""
        self.deserialization_stats.append({
            'timestamp': time.time(),
            'data_type': data_type,
            'latency_us': result.latency_us,
        })
        
    def get_performance_summary(self) -> Dict:
        """Get performance summary statistics"""
        if not self.serialization_stats:
            return {"error": "No data recorded"}
            
        serialize_latencies = [s['latency_us'] for s in self.serialization_stats]
        deserialize_latencies = [d['latency_us'] for d in self.deserialization_stats]
        
        return {
            'serialization': {
                'count': len(serialize_latencies),
                'avg_latency_us': sum(serialize_latencies) / len(serialize_latencies),
                'max_latency_us': max(serialize_latencies),
                'min_latency_us': min(serialize_latencies),
                'total_bytes': sum(s['size_bytes'] for s in self.serialization_stats)
            },
            'deserialization': {
                'count': len(deserialize_latencies),
                'avg_latency_us': sum(deserialize_latencies) / len(deserialize_latencies) if deserialize_latencies else 0,
                'max_latency_us': max(deserialize_latencies) if deserialize_latencies else 0,
                'min_latency_us': min(deserialize_latencies) if deserialize_latencies else 0,
            }
        }


# Default serializer instance for the simulation
default_serializer = PerformanceSerializer()
adaptive_serializer = AdaptiveSerializer()
profiler = SerializationProfiler()
