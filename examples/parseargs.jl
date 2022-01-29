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

using ArgParse: ArgName, ArgParseSettings, add_arg_table!, parse_args
using Base: getproperty

struct Args
    __args__::Dict{String,Any}
end

function Base.getindex(args::Args, key::String)
    return getfield(args, :__args__)[key]
end

function Base.getproperty(args::Args, name::Symbol)
    return getfield(args, :__args__)[String(name)]
end

function parseargs(table::Union{ArgName,Vector,Dict}...)
    s = ArgParseSettings()
    add_arg_table!(s, table...)
    args = parse_args(ARGS, s)
    return Args(args)
end
