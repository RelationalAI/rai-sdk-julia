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

function Base.propertynames(result::TransactionResult, private::Bool=false)
    data = _data(result)
    names = (keys(data)..., :relations)
    if private
        names = (names..., fieldnames(TransactionResult)...)
    end
    return names
end
function Base.getproperty(result::TransactionResult, name::Symbol)
    name in fieldnames(TransactionResult) && return getfield(result, name)
    data = _data(result)
    name == :relations && return Relations(data.output)
    return data[name]
end

function Base.show(io::IO, result::TransactionResult)
    if result.aborted
        println(io, "aborted")
        return nothing
    end
    count = 0
    for relation in result.relations
        if relation.name == "abort" && length(schema(relation)) == 0
            continue  # ignore ic results
        end
        count > 0 && println(io)
        show(io, relation)
        count += 1
    end
    show_problems(result)
end

show_result(io::IO, rsp::JSON3.Object) = show(io, TransactionResult(rsp))
show_result(rsp::JSON3.Object) = show(stdout, TransactionResult(rsp))

show_result(rsp::TransactionResponse) = show_result(stdout, rsp)
function show_result(io::IO, rsp::TransactionResponse)
    rsp.metadata === nothing && return
    rsp.results === nothing && return

    for (idx, relation_metadata) in enumerate(rsp.metadata.relations)
        show_relation_id(io, relation_metadata.relation_id)
        data = rsp.results[idx][2]
        tuples = isempty(data) ? [()] : zip(data...)
        # Reuse julia's array printing function to print this array of tuples.
        Base.print_array(io, collect(tuples))

        # Print trailing newline
        if idx !== lastindex(rsp.metadata.relations)
            println(io, "\n")
        else
            println(io, "")
        end
    end
end

"""
    show_problems([io::IO], rsp)

Print the problems associated with the given transaction response to the output
stream `io`.
"""
function show_problems(io::IO, rsp::JSON3.Object)
    isnothing(rsp) && return nothing
    @assert rsp.type == "TransactionResult"
    problems = rsp.problems
    isnothing(problems) && return nothing
    for (i, problem) in enumerate(problems)
        if get(problem, "is_error", false)
            kind = "error: "
        elseif get(problem, "is_exception", false)
            kind = "exception: "
        else
            kind = ""
        end
        i > 1 && println(io)
        println(io, "$kind$(problem.message)")
        report = get(problem, "report", nothing)
        isnothing(report) && continue
        println(io, strip(report))
    end
    return nothing
end

show_problems(rsp::JSON3.Object) = show_problems(stdout, rsp)
show_problems(result::TransactionResult) = show_problems(_data(result))
show_problems(io::IO, result::TransactionResult) = show_problems(io, _data(result))

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

# Returns a getrow lambda function that returns the requested row number as a
# `Tuple` of values. This function "lowers" symbols from type space to values
# in the corresponding position in the tuple.
function _make_getrow(relkey, columns)
    if columns == [[]]
        # Special case for when the relation is `true` in rel (only contains `()`):
        # This means that the relation is _only specialized values_, so we can just return
        # the specialized values from the relkey directly.
        return row -> _relname_to_symbol.(Tuple(relkey))
    end
    col = 1
    getters = []
    for item in relkey
        if startswith(item, ":")  # symbol
            let sym = _relname_to_symbol(item)
                push!(getters, _ -> sym)
            end
        else
            let col = col
                push!(getters, row -> columns[col][row])
            end
            col += 1
        end
    end
    @assert col == length(columns) + 1  "for $relkey: $col != $(length(columns) + 1)"
    @assert length(getters) == length(relkey)
    return row -> Tuple(getter(row) for getter in getters)
end
_relname_to_symbol(relname::String) = Symbol(relname[2:end])

Base.eltype(::Relation) = RelRow
Base.getindex(relation::Relation, key::Int) = getrow(relation, key)

function Base.propertynames(relation::Relation, private::Bool=false)
    data = _data(relation)
    names = (keys(data)..., :name)
    if private
        names = (names..., fieldnames(Relation)...)
    end
    return names
end
function Base.getproperty(relation::Relation, name::Symbol)
    name in fieldnames(Relation) && return getfield(result, name)
    data = _data(relation)
    name == :name && return data.rel_key.name
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

function Base.show(io::IO, relation::Relation)
    types = ["$item" for item in schema(relation)]
    sig = join(types, "/")
    println(io, "// $(relation.name) $sig")
    for (i, row) in enumerate(relation)
        i > 1 && println(io, ";")
        values = [_gen_literal(item) for item in row]
        print(io, join(values, ", "))
    end
    println(io)
    return nothing
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
    "Missing" => Nothing,
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
Tables.istable(::Type{Relation}) = true
Tables.rowaccess(::Type{Relation}) = true
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

function Base.propertynames(row::RelRow)
    return (_names(row)..., fieldnames(RelRow))
end
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
