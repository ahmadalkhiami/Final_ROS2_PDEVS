"""
RCL Parameter system implementation.
"""

from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from enum import Enum, auto

from tracing import trace_logger

class ParameterType(Enum):
    """ROS2 parameter types"""
    NOT_SET = auto()
    BOOL = auto()
    INTEGER = auto()
    DOUBLE = auto()
    STRING = auto()
    BYTE_ARRAY = auto()
    BOOL_ARRAY = auto()
    INTEGER_ARRAY = auto()
    DOUBLE_ARRAY = auto()
    STRING_ARRAY = auto()
    

@dataclass
class Parameter:
    """ROS2 parameter"""
    name: str
    value: Any
    type: ParameterType
    
    @classmethod
    def from_value(cls, name: str, value: Any) -> 'Parameter':
        """Create parameter from value with automatic type detection"""
        param_type = cls._detect_type(value)
        return cls(name, value, param_type)
        
    @staticmethod
    def _detect_type(value: Any) -> ParameterType:
        """Detect parameter type from value"""
        if value is None:
            return ParameterType.NOT_SET
        elif isinstance(value, bool):
            return ParameterType.BOOL
        elif isinstance(value, int):
            return ParameterType.INTEGER
        elif isinstance(value, float):
            return ParameterType.DOUBLE
        elif isinstance(value, str):
            return ParameterType.STRING
        elif isinstance(value, bytes):
            return ParameterType.BYTE_ARRAY
        elif isinstance(value, list):
            if not value:
                return ParameterType.NOT_SET
            elif all(isinstance(v, bool) for v in value):
                return ParameterType.BOOL_ARRAY
            elif all(isinstance(v, int) for v in value):
                return ParameterType.INTEGER_ARRAY
            elif all(isinstance(v, float) for v in value):
                return ParameterType.DOUBLE_ARRAY
            elif all(isinstance(v, str) for v in value):
                return ParameterType.STRING_ARRAY
        return ParameterType.NOT_SET


@dataclass
class ParameterDescriptor:
    """Parameter descriptor with metadata"""
    name: str
    type: ParameterType
    description: str = ""
    read_only: bool = False
    dynamic_typing: bool = False
    additional_constraints: str = ""
    integer_range: Optional[tuple] = None  # (min, max)
    floating_point_range: Optional[tuple] = None  # (min, max)
    

class ParameterServer:
    """RCL parameter server"""
    
    def __init__(self):
        self.parameters: Dict[str, Dict[str, Parameter]] = {}  # node -> params
        self.descriptors: Dict[str, Dict[str, ParameterDescriptor]] = {}
        self.callbacks: Dict[str, List[callable]] = {}  # node -> callbacks
        
    def declare_parameter(self, node_name: str, parameter: Parameter, 
                         descriptor: Optional[ParameterDescriptor] = None) -> bool:
        """Declare a parameter for a node"""
        if node_name not in self.parameters:
            self.parameters[node_name] = {}
            self.descriptors[node_name] = {}
            
        if parameter.name in self.parameters[node_name]:
            return False  # Already declared
            
        self.parameters[node_name][parameter.name] = parameter
        
        if descriptor:
            self.descriptors[node_name][parameter.name] = descriptor
            
        trace_logger.log_event(
            "rcl_parameter_declared",
            {
                "node": node_name,
                "parameter": parameter.name,
                "type": parameter.type.name,
                "value": str(parameter.value)
            }
        )
        
        return True
        
    def set_parameter(self, node_name: str, parameter: Parameter) -> tuple[bool, str]:
        """Set parameter value"""
        if node_name not in self.parameters:
            return False, f"Node {node_name} not found"
            
        if parameter.name not in self.parameters[node_name]:
            return False, f"Parameter {parameter.name} not declared"
            
        old_param = self.parameters[node_name][parameter.name]
        
        # Check type compatibility
        if old_param.type != parameter.type:
            return False, f"Type mismatch: expected {old_param.type.name}, got {parameter.type.name}"
            
        # Check constraints
        if node_name in self.descriptors and parameter.name in self.descriptors[node_name]:
            descriptor = self.descriptors[node_name][parameter.name]
            
            if descriptor.read_only:
                return False, "Parameter is read-only"
                
            # Range checks
            if parameter.type == ParameterType.INTEGER and descriptor.integer_range:
                if not (descriptor.integer_range[0] <= parameter.value <= descriptor.integer_range[1]):
                    return False, f"Value {parameter.value} outside range {descriptor.integer_range}"
                    
            elif parameter.type == ParameterType.DOUBLE and descriptor.floating_point_range:
                if not (descriptor.floating_point_range[0] <= parameter.value <= descriptor.floating_point_range[1]):
                    return False, f"Value {parameter.value} outside range {descriptor.floating_point_range}"
                    
        # Update parameter
        old_value = old_param.value
        self.parameters[node_name][parameter.name] = parameter
        
        # Trigger callbacks
        self._trigger_callbacks(node_name, parameter.name, old_value, parameter.value)
        
        trace_logger.log_event(
            "rcl_parameter_set",
            {
                "node": node_name,
                "parameter": parameter.name,
                "old_value": str(old_value),
                "new_value": str(parameter.value)
            }
        )
        
        return True, "Success"
        
    def get_parameter(self, node_name: str, param_name: str) -> Optional[Parameter]:
        """Get parameter value"""
        if node_name in self.parameters and param_name in self.parameters[node_name]:
            return self.parameters[node_name][param_name]
        return None
        
    def list_parameters(self, node_name: str, prefixes: List[str] = None) -> List[str]:
        """List parameters for a node"""
        if node_name not in self.parameters:
            return []
            
        params = list(self.parameters[node_name].keys())
        
        if prefixes:
            filtered = []
            for param in params:
                if any(param.startswith(prefix) for prefix in prefixes):
                    filtered.append(param)
            return filtered
            
        return params
        
    def describe_parameters(self, node_name: str, 
                          param_names: List[str]) -> Dict[str, ParameterDescriptor]:
        """Get parameter descriptors"""
        result = {}
        
        if node_name in self.descriptors:
            for name in param_names:
                if name in self.descriptors[node_name]:
                    result[name] = self.descriptors[node_name][name]
                    
        return result
        
    def register_parameter_callback(self, node_name: str, callback: callable):
        """Register parameter change callback"""
        if node_name not in self.callbacks:
            self.callbacks[node_name] = []
        self.callbacks[node_name].append(callback)
        
    def _trigger_callbacks(self, node_name: str, param_name: str, 
                          old_value: Any, new_value: Any):
        """Trigger parameter change callbacks"""
        if node_name in self.callbacks:
            for callback in self.callbacks[node_name]:
                try:
                    callback(param_name, old_value, new_value)
                except Exception as e:
                    trace_logger.log_event(
                        "rcl_parameter_callback_error",
                        {
                            "node": node_name,
                            "parameter": param_name,
                            "error": str(e)
                        }
                    )