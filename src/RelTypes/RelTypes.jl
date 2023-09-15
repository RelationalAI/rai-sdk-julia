""" 
The RelTypes module provides an interface to control the (de)serialization of types (from)to
Rel. We provide a hook `RelTypes.from_rel` that together with `RelTypes.JuliaType` can be
used to convert a Rel ValueType into a Julia type.

TODO (dba) Implement `to_rel` and `RelType` for user- and system-defined serialization form
Julia to Rel.
"""
module RelTypes

import Arrow
import Dates
import ..RAI
import ..RAI.protocol

using FixedPointDecimals: FixedDecimal

export JuliaType, from_rel

"""
    JuliaType(::Val{Tuple{<:Symbol}}, ::Type{S}, params::Vector) = T
    JuliaType(::Val{Tuple{<:Symbol}}, ::Type{S}) = T

Register the custom Julia logical type `T` into which a ValueType should be converted. A
ValueType has a unique signature, a tuple of Symbols and a physical type `S`. We are using
a `Val` to allow overloading this method. Make sure to also implement [`from_rel`](@ref) for
`S` and `T` which will do the actual conversion.
"""
function JuliaType end
JuliaType(S) = nothing
JuliaType(_, S) = JuliaType(S)
JuliaType(V, S, _) = JuliaType(V, S)

# Rel base conversions:
JuliaType(::Val{(:rel, :base, :DateTime)}, ::Type{Int64}) = Dates.DateTime
JuliaType(::Val{(:rel, :base, :Date)}, ::Type{Int64}) = Dates.Date
JuliaType(::Val{(:rel, :base, :Nanosecond)}, ::Type{Int64}) = Dates.Nanosecond
JuliaType(::Val{(:rel, :base, :Millisecond)}, ::Type{Int64}) = Dates.Millisecond
JuliaType(::Val{(:rel, :base, :Microsecond)}, ::Type{Int64}) = Dates.Microsecond
JuliaType(::Val{(:rel, :base, :Second)}, ::Type{Int64}) = Dates.Second
JuliaType(::Val{(:rel, :base, :Minute)}, ::Type{Int64}) = Dates.Minute
JuliaType(::Val{(:rel, :base, :Hour)}, ::Type{Int64}) = Dates.Hour
JuliaType(::Val{(:rel, :base, :Day)}, ::Type{Int64}) = Dates.Day
JuliaType(::Val{(:rel, :base, :Week)}, ::Type{Int64}) = Dates.Week
JuliaType(::Val{(:rel, :base, :Month)}, ::Type{Int64}) = Dates.Month
JuliaType(::Val{(:rel, :base, :Year)}, ::Type{Int64}) = Dates.Year
JuliaType(::Val{(:rel, :base, :Hash)}, ::Type{Tuple{UInt64,UInt64}}) = UInt128
JuliaType(::Val{(:rel, :base, :Rational)}, ::Type{Tuple{P,P}}) where {P} = Rational{P}
# Params[1] is the bit-lenght, but its already given by the type P.
JuliaType(::Val{(:rel, :base, :FixedDecimal)}, ::Type{P}, params) where {P} =
    FixedDecimal{P,params[2]}

# `JuliaType` returns either a registered Julia type that a Rel type should be converted to,
# or nothing.
function JuliaType(reltype::protocol.RelType{Nothing,Nothing})
    # All of the primitive Rel types except `Int128`, `UInt128`, and `Char` are identical to
    # their Julia counterpart.
    if reltype.primitive_type == protocol.PrimitiveType.INT_128
        return Int128
    elseif reltype.primitive_type == protocol.PrimitiveType.UINT_128
        return UInt128
    elseif reltype.primitive_type == protocol.PrimitiveType.CHAR
        return Char
    else
        return nothing
    end
end
function JuliaType(reltype::protocol.RelType{Nothing,protocol.ValueType})
    # This is the signature, such as ((:rel, :base, :Date), Int64, []). Parameters are
    # non-symbol constants in the signature of a value type.
    (marker, type, params) = signature(reltype)
    return JuliaType(Val(marker), type, params)
end

# Each value type has a unique signature. One or multiple markers, such as `(:rel, :base,
# :Date)`, a field type, such as `Int64`, and optionally parameters.
function signature(reltype::protocol.RelType{Nothing,protocol.ValueType})
    # Each type in a value type again is a `RelType`.
    markers = Tuple(
        RAI.value_from_proto(type) for
        type in reltype.value_type.argument_types if is_marker(type)
    )
    type = field_type(reltype)
    params = [
        RAI.value_from_proto(type) for
        type in reltype.value_type.argument_types if is_param(type)
    ]
    return (markers, type, params)
end

# Extract the first value as a Julia native type from a constant type.
function RAI.value_from_proto(reltype::protocol.RelType{protocol.ConstantType,Nothing})
    return RAI.value_from_proto(reltype.constant_type.value.arguments[1])
end

is_marker(::protocol.RelType{Nothing,protocol.ValueType}) = false
is_marker(::protocol.RelType{Nothing,Nothing}) = false
function is_marker(reltype::protocol.RelType{protocol.ConstantType,Nothing})
    # Only constant strings are considered markers!
    return reltype.constant_type.rel_type.primitive_type === protocol.PrimitiveType.STRING
end

is_param(::protocol.RelType{Nothing,protocol.ValueType}) = false
is_param(::protocol.RelType{Nothing,Nothing}) = false
function is_param(reltype::protocol.RelType{protocol.ConstantType,Nothing})
    # Primitive constants besided STRINGS are considered parameters of a type.
    return reltype.constant_type.rel_type.primitive_type !== protocol.PrimitiveType.STRING
end


# ValueTypes might be nested and contain these besides primitive field types.
is_field(::protocol.RelType{protocol.ConstantType}) = false
is_field(::protocol.RelType{Nothing,protocol.ValueType}) = true
is_field(::protocol.RelType{Nothing,Nothing}) = true

field(rel_type::protocol.RelType{Nothing,Nothing}) =
    RAI.primitive_type_from_proto(rel_type.primitive_type)
field(rel_type::protocol.RelType{Nothing,protocol.ValueType}) = field_type(rel_type)

# Returns the field type of a this value type. Its either a single type or a Tuple type.
function field_type(rel_type::protocol.RelType{Nothing,protocol.ValueType})
    arg_types = rel_type.value_type.argument_types
    candidates = [field(type) for type in arg_types if is_field(type)]
    if length(candidates) == 1
        return candidates[1]
    else
        # Types are flattened in our metadata but not in our data!
        return Tuple{candidates...}
    end
end

"""
    from_rel(::Type{T}, x::S)

Conversion method to convert a Rel ValueType into a Julia type `T`. You also need to
register the corresponding [`JuliaType`](@ref) method. `S` is the physical type of the data
in the resulting Arrow file.

## Examples
```rel
value type Point = Int, Int
def output = ^Point[1,2]
```

```julia
struct Point
    x::Int64
    y::Int64
end

from_rel(::Type{Point}, t::Tuple{Int64,Int64}) = Point(t...)
```
"""

function from_rel end
from_rel(::Type{T}, x::T) where {T} = x
from_rel(::Type{UInt128}, t::Tuple{UInt64,UInt64}) = UInt128(t[2]) << 64 | t[1]
from_rel(::Type{Int128}, t::Tuple{UInt64,UInt64}) = Int128(t[2]) << 64 | t[1]
from_rel(::Type{Char}, i::UInt32) = Char(i)
from_rel(::Type{Dates.DateTime}, i) = Dates.DateTime(Dates.UTM(i))
from_rel(::Type{Dates.Date}, i) = Dates.Date(Dates.UTD(i))
from_rel(::Type{Dates.Nanosecond}, i) = Dates.Nanosecond(i)
from_rel(::Type{Dates.Microsecond}, i) = Dates.Microsecond(i)
from_rel(::Type{Dates.Millisecond}, i) = Dates.Millisecond(i)
from_rel(::Type{Dates.Second}, i) = Dates.Second(i)
from_rel(::Type{Dates.Minute}, i) = Dates.Minute(i)
from_rel(::Type{Dates.Hour}, i) = Dates.Hour(i)
from_rel(::Type{Dates.Day}, i) = Dates.Day(i)
from_rel(::Type{Dates.Week}, i) = Dates.Week(i)
from_rel(::Type{Dates.Month}, i) = Dates.Month(i)
from_rel(::Type{Dates.Year}, i) = Dates.Year(i)
from_rel(::Type{Rational{P}}, t::Tuple{P,P}) where {P} = Rational(t...)
# We need an extra method for Rationals with 128 bit-length as they are differently
# transferred over the wire.
from_rel(::Type{Rational{Int128}}, t::Tuple{Tuple{UInt64,UInt64},Tuple{UInt64,UInt64}}) =
    Rational(from_rel(Int128, t[1]), from_rel(Int128, t[2]))
from_rel(T::Type{<:FixedDecimal}, i) = reinterpret(T,i)

end # End of module.
