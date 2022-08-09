import ProtocolBuffers

# Conversion to Julia primitive types
# ========================================================================================
const proto_to_julia_type_map = Dict(
    Protocol_PB.PrimitiveType.INT_128 => Int128,
    Protocol_PB.PrimitiveType.INT_64 => Int64,
    Protocol_PB.PrimitiveType.INT_32 => Int32,
    Protocol_PB.PrimitiveType.INT_16 => Int16,
    Protocol_PB.PrimitiveType.INT_8 => Int8,
    Protocol_PB.PrimitiveType.UINT_128 => UInt128,
    Protocol_PB.PrimitiveType.UINT_64 => UInt64,
    Protocol_PB.PrimitiveType.UINT_32 => UInt32,
    Protocol_PB.PrimitiveType.UINT_16 => UInt16,
    Protocol_PB.PrimitiveType.UINT_8 => UInt8,
    Protocol_PB.PrimitiveType.FLOAT_64 => Float64,
    Protocol_PB.PrimitiveType.FLOAT_32 => Float32,
    Protocol_PB.PrimitiveType.FLOAT_16 => Float16,
    Protocol_PB.PrimitiveType.STRING => String,
    Protocol_PB.PrimitiveType.SYMBOL => Symbol,
    Protocol_PB.PrimitiveType.CHAR => Char,
    Protocol_PB.PrimitiveType.BOOL => Bool,
)

function primitive_type_from_proto(primitive_type::Protocol_PB.PrimitiveType.T)
    # Each of these maps to a single Julia type.
    proto_to_julia_type_map[primitive_type]
end

function extract_values_from_proto(rel_tuple::Protocol_PB.RelTuple)
    return [value_from_proto(val) for val in rel_tuple.arguments]
end

# Julia values from protos.
function value_from_proto(v::Protocol_PB.PrimitiveValue)
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
_from_proto(T::Type{Int128}, v) = _from_uint128_proto(v)

function _from_uint128_proto(i::Protocol_PB.RelUInt128)
    return (UInt128(i.highbits) << 64) | i.lowbits
end

function _from_int128_proto(i::Protocol_PB.RelInt128)
    return (Int128(i.highbits) << 64) | i.lowbits
end

# Display
# ========================================================================================
function show_relation_id(io::IO, rel_id::Protocol_PB.RelationId)
    for rel_type in rel_id.arguments
        print(io, "/")
        show_rel_type(io, rel_type)
    end
    print(io, "\n")
    return nothing
end

# PrimitiveType
function show_rel_type(io::IO, rel_type::Protocol_PB.RelType{Nothing,Nothing})
    return show(io, primitive_type_from_proto(rel_type.primitive_type))
end
# ConstantType
function show_rel_type(
    io::IO,
    rel_type::Protocol_PB.RelType{Protocol_PB.ConstantType,Nothing},
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
function show_rel_type(io::IO, rel_type::Protocol_PB.RelType{Nothing,Protocol_PB.ValueType})
    print(io, "ValueType(")
    args = rel_type.value_type.argument_types
    for (idx, type) in enumerate(args)
        show_rel_type(io, type)
        length(args) !== idx && print(io, ", ")
    end
    print(io, ")")
    return nothing
end

# TODO (dba) Workaround as `RelType` has no definite size when encoding!
# https://github.com/Drvi/ProtocolBuffers.jl/issues/13
function ProtocolBuffers.encode(e::ProtocolBuffers.AbstractProtoEncoder, i::Int, x::Vector{Protocol_PB.RelType})
    # This is the max size of `RelType` when either the `ConstantType` or `ValueType` are
    # set!
    size = 24
    ProtocolBuffers.Codecs.maybe_ensure_room(e.io, length(x) * size)
    for el in x
        ProtocolBuffers.Codecs.encode_tag(e, i, ProtocolBuffers.Codecs.LENGTH_DELIMITED)
        ProtocolBuffers.Codecs._with_size(ProtocolBuffers.encode, e.io, e, el)
    end
    return nothing
end
