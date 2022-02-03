# Copyright 2022 RelationalAI, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Functions for accessing transaction results, including an implementation
# of the Tables.jl interfaces.

import JSON3
import Tables

struct TransactionResult
    _data::JSON3.Object
end

_data(result::TransactionResult) = getfield(result, :_data)

Base.getindex(result::TransactionResult, key) = _data(result)[key]

function Base.getproperty(result::TransactionResult, name::Symbol)
    data = _data(result)
    name == :relations && return Relations(data.output)
    return data[name]
end

struct Relations
    _data::JSON3.Array
end

_data(relations::Relations) = getfield(relations, :_data)

Base.getindex(relations::Relations, key::Int) = Relation(_data(relations)[key])

Base.length(relations::Relations) = length(_data(relations))

function Base.iterate(relations::Relations, i = 1)
    data = _data(relations)
    i > length(data) && return nothing
    item = data[i]
    return Relation(item), i + 1
end

struct Relation
    _data::JSON3.Object
    _types::Vector{Type}
    _names::Vector{Symbol}
    _getrow::Function

    function Relation(data)
        item = data.rel_key
        relkey = cat(item.keys, item.values; dims = 1)
        types = [datatype(rtype) for rtype in relkey]
        names = [Symbol("Column$i") for i in 1:length(types)]
        getrow = _make_getrow(relkey, data.columns)
        return new(data, types, names, getrow)
    end
end

_data(relation::Relation) = getfield(relation, :_data)
_names(relation::Relation) = getfield(relation, :_names)
_types(relation::Relation) = getfield(relation, :_types)
_getrow(relation::Relation) = getfield(relation, :_getrow)

# Returns a getrow functions that returns the requested row number as a
# `Tuple` of values. This function "lowers" symbols from type space to
# symbolic values in the corresponding position in value space.
function _make_getrow(relkey, columns)
    col = 1
    getters = []
    for item in relkey
        if startswith(item, ":")  # symbol
            let sym = Symbol(item[2:end])
                push!(getters, _ -> sym)
            end
        else
            let col = col
                push!(getters, row -> columns[col][row])
            end
            col += 1
        end
    end
    @assert col == length(columns) + 1
    @assert length(getters) == length(relkey)
    return row -> Tuple([getter(row) for getter in getters])
end

Base.eltype(_::Relation) = RelRow
Base.getindex(relation::Relation, key::Int) = getrow(relation, key)

function Base.getproperty(relation::Relation, name::Symbol)
    return _data(relation)[name]
end

function Base.iterate(relation::Relation, i = 1)
    i > length(relation) && return nothing
    return RelRow(relation, i), i + 1
end

function Base.length(relation::Relation)
    cols = _data(relation).columns
    length(cols) == 0 && return 0  # empty relation
    return length(cols[1])
end

function getrow(relation::Relation, row::Int)::Tuple
    return _getrow(relation)(row)
end

schema(relation::Relation) = _types(relation)

# todo: review list of rel primitive types
const _typemap = Dict{String,DataType}(
    "Any" => Any,          # todo
    "AutoNumber" => Int,   # todo
    "Date" => String,      # todo
    "DateTime" => String,  # todo
    "Char" => Char,
    "RelationalAITypes.FilePos" => Int,
    "Float16" => Float16,
    "Float32" => Float32,
    "Float64" => Float64,
    "Hash" => String,      # todo
    "Int8" => Int8,
    "Int16" => Int16,
    "Int32" => Int32,
    "Int64" => Int64,
    "Int128" => Int128,
    "RelName" => String,
    "String" => String,
    "Symbol" => Symbol,
    "UInt8" => UInt8,
    "UInt16" => UInt16,
    "UInt32" => UInt32,
    "UInt64" => UInt64,
    "UInt128" => UInt128)

# Returns the Julia data type corresponding to the given Rel type string.
function datatype(reltype::String)
    dt = get(_typemap, reltype, nothing)
    !isnothing(dt) && return dt
    startswith(reltype, ":") && return Symbol
    @assert false "unknown type: $reltype"
    return nothing
end

# Implementation of the Tables.jl `AbstractRow` interface.

Tables.columnnames(relation::Relation) = _names(relation)
Tables.istable(::Type{<:Relation}) = true
Tables.rowaccess(::Type{<:Relation}) = true
Tables.rows(relation::Relation) = relation

function Tables.schema(relation::Relation)
    return Tables.Schema(_names(relation), _types(relation))
end

struct RelRow <: Tables.AbstractRow
    _relation::Relation
    values::Tuple

    function RelRow(relation::Relation, row::Int)
        new(relation, getrow(relation, row))
    end
end

_names(row::RelRow) = _names(_relation(row))
_relation(row::RelRow) = getfield(row, :_relation)
_values(row::RelRow) = getfield(row, :values)
_indexof(row, name) = findfirst(==(name), _names(row))

Base.getindex(row::RelRow, key::Int) = _values(row)[key]

function Base.getproperty(row::RelRow, name::Symbol)
    name in _names(row) && return getcolumn(row, name)
    return getfield(row, name)
end

getcolumn(row::RelRow, col::Int) = _values(row)[col]

function getcolumn(row::RelRow, name::Symbol)
    col = _indexof(row, name)
    return _values(row)[col]
end

Tables.columnnames(row::RelRow) = _names(row)
Tables.getcolumn(row::RelRow, col::Int) = getcolumn(row, col)
Tables.getcolumn(row::RelRow, name::Symbol) = getcolumn(row, name)
