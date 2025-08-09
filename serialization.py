"""
CDR (Common Data Representation) serialization for DDS.
Handles message serialization/deserialization.
"""

import struct
import time
from typing import Any, Dict, List, Optional, Tuple, Union, Type
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum, IntEnum
import io
from message import Message
from tracing import trace_logger



class CDREncapsulation(Enum):
    """CDR Encapsulation schemes"""
    CDR_BE = 0x0000  # Big Endian
    CDR_LE = 0x0001  # Little Endian
    PL_CDR_BE = 0x0002  # Parameter List CDR Big Endian
    PL_CDR_LE = 0x0003  # Parameter List CDR Little Endian
    CDR2_BE = 0x0006  # CDR2 Big Endian
    CDR2_LE = 0x0007  # CDR2 Little Endian
    DELIMITED_CDR2_BE = 0x0008  # Delimited CDR2 Big Endian
    DELIMITED_CDR2_LE = 0x0009  # Delimited CDR2 Little Endian


class CDRSerializer:
    """
    CDR serializer for DDS messages.
    Implements OMG CDR specification for ROS2/DDS compatibility.
    """
    
    def __init__(self, encapsulation: CDREncapsulation = CDREncapsulation.CDR_LE):
        self.encapsulation = encapsulation
        self.endianness = '<' if 'LE' in encapsulation.name else '>'
        self._buffer = io.BytesIO()
        self._offset = 0
        
    def serialize_message(self, msg: Any) -> bytes:
        """Serialize a message to CDR format"""
        start_time = time.time()
        
        # Reset buffer
        self._buffer = io.BytesIO()
        self._offset = 0
        
        # Write encapsulation header
        self._write_encapsulation_header()
        
        # Serialize message fields
        if isinstance(msg, Message) or is_dataclass(msg):
            self._serialize_dataclass(msg)
        else:
            self._serialize_object(msg)
        
        # Add padding to 4-byte boundary
        self._align(4)
        
        # Get serialized data
        data = self._buffer.getvalue()
        
        serialization_time = time.time() - start_time
        
        trace_logger.log_event(
            "dds_cdr_serialize",
            {
                "message_type": type(msg).__name__,
                "size_bytes": len(data),
                "time_us": int(serialization_time * 1e6)
            }
        )
        
        return data
        
    def deserialize_message(self, data: bytes, message_type: Type) -> Any:
        """Deserialize CDR data to message"""
        start_time = time.time()
        
        # Reset buffer
        self._buffer = io.BytesIO(data)
        self._offset = 0
        
        # Read encapsulation header
        self._read_encapsulation_header()
        
        # Deserialize message
        if is_dataclass(message_type):
            msg = self._deserialize_dataclass(message_type)
        else:
            msg = self._deserialize_object(message_type)
        
        deserialization_time = time.time() - start_time
        
        trace_logger.log_event(
            "dds_cdr_deserialize",
            {
                "message_type": message_type.__name__,
                "size_bytes": len(data),
                "time_us": int(deserialization_time * 1e6)
            }
        )
        
        return msg
        
    def _write_encapsulation_header(self):
        """Write CDR encapsulation header"""
        # Encapsulation identifier (2 bytes)
        self._write_uint16(self.encapsulation.value)
        # Options (2 bytes) - reserved, must be 0
        self._write_uint16(0)
        
    def _read_encapsulation_header(self):
        """Read CDR encapsulation header"""
        encap_id = self._read_uint16()
        options = self._read_uint16()
        
        # Update endianness based on encapsulation
        if encap_id & 0x0001:  # Little endian
            self.endianness = '<'
        else:  # Big endian
            self.endianness = '>'
            
    def _serialize_object(self, obj: Any):
        """Serialize an object recursively"""
        if obj is None:
            self._write_bool(False)  # Null indicator
        elif isinstance(obj, bool):
            self._write_bool(obj)
        elif isinstance(obj, int):
            # Determine integer size based on value
            if -128 <= obj <= 127:
                self._write_int8(obj)
            elif -32768 <= obj <= 32767:
                self._write_int16(obj)
            elif -2147483648 <= obj <= 2147483647:
                self._write_int32(obj)
            else:
                self._write_int64(obj)
        elif isinstance(obj, float):
            self._write_float64(obj)
        elif isinstance(obj, str):
            self._write_string(obj)
        elif isinstance(obj, bytes):
            self._write_sequence(obj, self._write_uint8)
        elif isinstance(obj, (list, tuple)):
            self._write_sequence(obj, self._serialize_object)
        elif isinstance(obj, dict):
            self._serialize_dict(obj)
        elif isinstance(obj, Enum):
            self._write_int32(obj.value)
        elif isinstance(obj, IntEnum):
            self._write_int32(int(obj))
        elif is_dataclass(obj):
            self._serialize_dataclass(obj)
        else:
            # Try to serialize as string representation
            self._write_string(str(obj))
            
    def _deserialize_object(self, obj_type: Type) -> Any:
        """Deserialize an object based on type"""
        if obj_type == bool:
            return self._read_bool()
        elif obj_type == int:
            return self._read_int32()
        elif obj_type == float:
            return self._read_float64()
        elif obj_type == str:
            return self._read_string()
        elif obj_type == bytes:
            return bytes(self._read_sequence(self._read_uint8))
        elif hasattr(obj_type, '__origin__'):  # Generic types
            if obj_type.__origin__ == list:
                elem_type = obj_type.__args__[0] if obj_type.__args__ else Any
                return self._read_sequence(lambda: self._deserialize_object(elem_type))
            elif obj_type.__origin__ == dict:
                key_type = obj_type.__args__[0] if len(obj_type.__args__) > 0 else Any
                val_type = obj_type.__args__[1] if len(obj_type.__args__) > 1 else Any
                return self._deserialize_dict(key_type, val_type)
        elif issubclass(obj_type, Enum):
            value = self._read_int32()
            return obj_type(value)
        elif is_dataclass(obj_type):
            return self._deserialize_dataclass(obj_type)
        else:
            raise ValueError(f"Cannot deserialize type {obj_type}")
            
    def _serialize_dataclass(self, obj):
        """Serialize a dataclass"""
        for field in fields(obj):
            value = getattr(obj, field.name)
            
            # Handle optional fields
            if hasattr(field.type, '__origin__') and field.type.__origin__ == Union:
                # Check if it's Optional (Union with None)
                if type(None) in field.type.__args__:
                    if value is None:
                        self._write_bool(False)  # Not present
                        continue
                    else:
                        self._write_bool(True)  # Present
                        
            self._serialize_typed_value(value, field.type)
            
    def _serialize_typed_value(self, value: Any, field_type: Type):
        """Serialize a value with known type information"""
        if field_type == bool:
            self._write_bool(value)
        elif field_type == int:
            self._write_int32(value)
        elif field_type == float:
            self._write_float64(value)
        elif field_type == str:
            self._write_string(value)
        elif hasattr(field_type, '__origin__'):
            if field_type.__origin__ == list:
                elem_type = field_type.__args__[0] if field_type.__args__ else Any
                self._write_sequence(value, lambda x: self._serialize_typed_value(x, elem_type))
            elif field_type.__origin__ == dict:
                self._serialize_dict(value)
        elif issubclass(field_type, Enum):
            self._write_int32(value.value)
        elif is_dataclass(field_type):
            self._serialize_dataclass(value)
        else:
            self._serialize_object(value)
            
    def _deserialize_dataclass(self, cls: Type) -> Any:
        """Deserialize a dataclass"""
        values = {}
        
        for field in fields(cls):
            # Handle optional fields
            if hasattr(field.type, '__origin__') and field.type.__origin__ == Union:
                if type(None) in field.type.__args__:
                    present = self._read_bool()
                    if not present:
                        values[field.name] = None
                        continue
                        
            values[field.name] = self._deserialize_typed_value(field.type)
            
        return cls(**values)
        
    def _deserialize_typed_value(self, field_type: Type) -> Any:
        """Deserialize a value with known type information"""
        if field_type == bool:
            return self._read_bool()
        elif field_type == int:
            return self._read_int32()
        elif field_type == float:
            return self._read_float64()
        elif field_type == str:
            return self._read_string()
        elif hasattr(field_type, '__origin__'):
            if field_type.__origin__ == list:
                elem_type = field_type.__args__[0] if field_type.__args__ else Any
                return self._read_sequence(lambda: self._deserialize_typed_value(elem_type))
            elif field_type.__origin__ == dict:
                key_type = field_type.__args__[0] if len(field_type.__args__) > 0 else Any
                val_type = field_type.__args__[1] if len(field_type.__args__) > 1 else Any
                return self._deserialize_dict(key_type, val_type)
        elif issubclass(field_type, Enum):
            value = self._read_int32()
            return field_type(value)
        elif is_dataclass(field_type):
            return self._deserialize_dataclass(field_type)
        else:
            return self._deserialize_object(field_type)
        
    def _serialize_dict(self, d: Dict):
        """Serialize a dictionary as a map"""
        self._write_uint32(len(d))
        for key, value in d.items():
            self._serialize_object(key)
            self._serialize_object(value)
            
    def _deserialize_dict(self, key_type: Type, val_type: Type) -> Dict:
        """Deserialize a dictionary"""
        length = self._read_uint32()
        result = {}
        for _ in range(length):
            key = self._deserialize_object(key_type)
            value = self._deserialize_object(val_type)
            result[key] = value
        return result
            
    # Primitive type writers
    def _write_bool(self, value: bool):
        self._align(1)
        self._buffer.write(struct.pack('?', value))
        self._offset += 1
        
    def _write_int8(self, value: int):
        self._align(1)
        self._buffer.write(struct.pack('b', value))
        self._offset += 1
        
    def _write_uint8(self, value: int):
        self._align(1)
        self._buffer.write(struct.pack('B', value))
        self._offset += 1
        
    def _write_int16(self, value: int):
        self._align(2)
        self._buffer.write(struct.pack(f'{self.endianness}h', value))
        self._offset += 2
        
    def _write_uint16(self, value: int):
        self._align(2)
        self._buffer.write(struct.pack(f'{self.endianness}H', value))
        self._offset += 2
        
    def _write_int32(self, value: int):
        self._align(4)
        self._buffer.write(struct.pack(f'{self.endianness}i', value))
        self._offset += 4
        
    def _write_uint32(self, value: int):
        self._align(4)
        self._buffer.write(struct.pack(f'{self.endianness}I', value))
        self._offset += 4
        
    def _write_int64(self, value: int):
        self._align(8)
        self._buffer.write(struct.pack(f'{self.endianness}q', value))
        self._offset += 8
        
    def _write_uint64(self, value: int):
        self._align(8)
        self._buffer.write(struct.pack(f'{self.endianness}Q', value))
        self._offset += 8
        
    def _write_float32(self, value: float):
        self._align(4)
        self._buffer.write(struct.pack(f'{self.endianness}f', value))
        self._offset += 4
        
    def _write_float64(self, value: float):
        self._align(8)
        self._buffer.write(struct.pack(f'{self.endianness}d', value))
        self._offset += 8
        
    def _write_string(self, value: str):
        """Write string with length prefix"""
        encoded = value.encode('utf-8')
        self._write_uint32(len(encoded) + 1)  # Include null terminator
        self._buffer.write(encoded)
        self._buffer.write(b'\x00')  # Null terminator
        self._offset += len(encoded) + 1
        
    def _write_sequence(self, seq: List, writer_func):
        """Write a sequence with length prefix"""
        self._write_uint32(len(seq))
        for item in seq:
            writer_func(item)
            
    # Primitive type readers
    def _read_bool(self) -> bool:
        self._align(1)
        value = struct.unpack_from('?', self._buffer.read(1))[0]
        self._offset += 1
        return value
        
    def _read_int8(self) -> int:
        self._align(1)
        value = struct.unpack_from('b', self._buffer.read(1))[0]
        self._offset += 1
        return value
        
    def _read_uint8(self) -> int:
        self._align(1)
        value = struct.unpack_from('B', self._buffer.read(1))[0]
        self._offset += 1
        return value
        
    def _read_int16(self) -> int:
        self._align(2)
        value = struct.unpack_from(f'{self.endianness}h', self._buffer.read(2))[0]
        self._offset += 2
        return value
        
    def _read_uint16(self) -> int:
        self._align(2)
        value = struct.unpack_from(f'{self.endianness}H', self._buffer.read(2))[0]
        self._offset += 2
        return value
        
    def _read_int32(self) -> int:
        self._align(4)
        value = struct.unpack_from(f'{self.endianness}i', self._buffer.read(4))[0]
        self._offset += 4
        return value
        
    def _read_uint32(self) -> int:
        self._align(4)
        value = struct.unpack_from(f'{self.endianness}I', self._buffer.read(4))[0]
        self._offset += 4
        return value
        
    def _read_int64(self) -> int:
        self._align(8)
        value = struct.unpack_from(f'{self.endianness}q', self._buffer.read(8))[0]
        self._offset += 8
        return value
        
    def _read_uint64(self) -> int:
        self._align(8)
        value = struct.unpack_from(f'{self.endianness}Q', self._buffer.read(8))[0]
        self._offset += 8
        return value
        
    def _read_float32(self) -> float:
        self._align(4)
        value = struct.unpack_from(f'{self.endianness}f', self._buffer.read(4))[0]
        self._offset += 4
        return value
        
    def _read_float64(self) -> float:
        self._align(8)
        value = struct.unpack_from(f'{self.endianness}d', self._buffer.read(8))[0]
        self._offset += 8
        return value
        
    def _read_string(self) -> str:
        """Read string with length prefix"""
        length = self._read_uint32()
        if length > 0:
            # Read string without null terminator
            data = self._buffer.read(length - 1)
            self._buffer.read(1)  # Skip null terminator
            self._offset += length
            return data.decode('utf-8')
        return ""
        
    def _read_sequence(self, reader_func) -> List:
        """Read a sequence with length prefix"""
        length = self._read_uint32()
        return [reader_func() for _ in range(length)]
        
    def _align(self, alignment: int):
        """Align buffer to boundary"""
        current_pos = self._buffer.tell()
        remainder = current_pos % alignment
        if remainder != 0:
            padding = alignment - remainder
            self._buffer.write(b'\x00' * padding)
            self._offset += padding


class TypeSupport:
    """Type support information for CDR serialization"""
    
    def __init__(self, type_name: str, message_type: Type):
        self.type_name = type_name
        self.message_type = message_type
        self.serializer = CDRSerializer()
        
    def serialize(self, msg: Any) -> bytes:
        """Serialize message"""
        return self.serializer.serialize_message(msg)
        
    def deserialize(self, data: bytes) -> Any:
        """Deserialize message"""
        return self.serializer.deserialize_message(data, self.message_type)
        
    def get_type_hash(self) -> bytes:
        """Get type hash for type compatibility checking"""
        # Simplified - in real DDS this would be XCDR2 type hash
        import hashlib
        type_str = f"{self.type_name}:{self.message_type}"
        return hashlib.sha256(type_str.encode()).digest()


class TypeRegistry:
    """Registry for type information and serialization"""
    
    def __init__(self):
        self._types: Dict[str, TypeSupport] = {}
        self._register_builtin_types()
        
    def _register_builtin_types(self):
        """Register built-in ROS2 types"""
        # Register basic message type
        self.register_type("message/Message", Message)
        
    def register_type(self, type_name: str, type_class: Type):
        """Register a type for serialization"""
        self._types[type_name] = TypeSupport(type_name, type_class)
        
    def get_type_support(self, type_name: str) -> Optional[TypeSupport]:
        """Get type support for a type"""
        return self._types.get(type_name)
        
    def serialize(self, type_name: str, obj: Any) -> bytes:
        """Serialize object of given type"""
        type_support = self.get_type_support(type_name)
        if not type_support:
            raise ValueError(f"Unknown type: {type_name}")
        return type_support.serialize(obj)
        
    def deserialize(self, type_name: str, data: bytes) -> Any:
        """Deserialize data to object of given type"""
        type_support = self.get_type_support(type_name)
        if not type_support:
            raise ValueError(f"Unknown type: {type_name}")
        return type_support.deserialize(data)
        
    def get_registered_types(self) -> List[str]:
        """Get list of registered type names"""
        return list(self._types.keys())


# Global type registry instance
type_registry = TypeRegistry()
