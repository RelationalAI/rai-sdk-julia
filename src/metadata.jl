import ProtoBuf

# Conversion to Julia primitive types
# ========================================================================================
const proto_to_julia_type_map = Dict(
    protocol.PrimitiveType.INT_128 => Int128,
    protocol.PrimitiveType.INT_64 => Int64,
    protocol.PrimitiveType.INT_32 => Int32,
    protocol.PrimitiveType.INT_16 => Int16,
    protocol.PrimitiveType.INT_8 => Int8,
    protocol.PrimitiveType.UINT_128 => UInt128,
    protocol.PrimitiveType.UINT_64 => UInt64,
    protocol.PrimitiveType.UINT_32 => UInt32,
    protocol.PrimitiveType.UINT_16 => UInt16,
    protocol.PrimitiveType.UINT_8 => UInt8,
    protocol.PrimitiveType.FLOAT_64 => Float64,
    protocol.PrimitiveType.FLOAT_32 => Float32,
    protocol.PrimitiveType.FLOAT_16 => Float16,
    protocol.PrimitiveType.STRING => String,
    protocol.PrimitiveType.SYMBOL => Symbol,
    protocol.PrimitiveType.CHAR => Char,
    protocol.PrimitiveType.BOOL => Bool,
)

function primitive_type_from_proto(primitive_type::protocol.PrimitiveType.T)
    # Each of these maps to a single Julia type.
    proto_to_julia_type_map[primitive_type]
end

function extract_values_from_proto(rel_tuple::protocol.RelTuple)
    return [value_from_proto(val) for val in rel_tuple.arguments]
end

# Julia values from protos.
function value_from_proto(v::protocol.PrimitiveValue)
    # Use the `tag` to infer the Julia type.
    T = primitive_type_from_proto(v.tag)

    # Construct the Julia type.
    return _from_proto(T, v.value.value)
end

_from_proto(T::Type, v) = T(v)
# TODO (dba) Consider creating a `Symbol` here instead of a `String` as in Julia a
# specialized, or constant, string is a `Symbol`
# Byte array must be copied because Symbol takes ownership!
_from_proto(::Type{String}, v) = String(copy(v))
_from_proto(::Type{Int16}, v) = v % Int16
_from_proto(::Type{Int8}, v) = v % Int8
_from_proto(::Type{UInt16}, v) = v % Int16
_from_proto(::Type{UInt8}, v) = v % Int8
_from_proto(T::Type{UInt128}, v) = _from_uint128_proto(v)
_from_proto(T::Type{Int128}, v) = _from_int128_proto(v)

function _from_uint128_proto(i::protocol.RelUInt128)
    return (UInt128(i.highbits) << 64) | i.lowbits
end

function _from_int128_proto(i::protocol.RelInt128)
    return (Int128(i.highbits) << 64) | i.lowbits
end

# Display
# ========================================================================================
function show_relation_id(io::IO, rel_id::protocol.RelationId)
    for rel_type in rel_id.arguments
        print(io, "/")
        show_rel_type(io, rel_type)
    end
    print(io, "\n")
    return nothing
end

# PrimitiveType
function show_rel_type(io::IO, rel_type::protocol.RelType{Nothing,Nothing})
    return show(io, primitive_type_from_proto(rel_type.primitive_type))
end
# ConstantType
function show_rel_type(
    io::IO,
    rel_type::protocol.RelType{protocol.ConstantType,Nothing},
)
    constant_type = rel_type.constant_type
    print(io, "ConstantType(")
    show_rel_type(io, constant_type.rel_type)
    print(io, ", ")
    values = extract_values_from_proto(constant_type.value)
    if length(values) == 1
        show(io, values[1])
    else
        Base.print_array(io, values)
    end
    print(io, ")")
    return nothing
end
# ValueType
function show_rel_type(io::IO, rel_type::protocol.RelType{Nothing,protocol.ValueType})
    print(io, "ValueType(")
    args = rel_type.value_type.argument_types
    for (idx, type) in enumerate(args)
        show_rel_type(io, type)
        length(args) !== idx && print(io, ", ")
    end
    print(io, ")")
    return nothing
end

