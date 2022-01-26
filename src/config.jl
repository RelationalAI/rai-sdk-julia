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

import ConfParser
using Base: Filesystem

mutable struct Config
    region::Union{String,Nothing}
    scheme::Union{String,Nothing}
    host::Union{String,Nothing}
    port::Union{String,Nothing}
    credentials::Union{Credentials,Nothing}
end

function _load_stanza(fname::AbstractString, profile::AbstractString)
    fname = Filesystem.expanduser(fname)
    conf = ConfParser.ConfParse(fname)
    ConfParser.parse_conf!(conf)
    return ConfParser.retrieve(conf, profile)
end

# Note, ConfParser returns Dict{String => Vector{T}} (declared as Dict{Any,Any})
function _get_value(d::Dict, k::String)
    v = get(d, k, nothing)
    isnothing(v) && return nothing
    # todo: if length(v) != 1 .. throw
    return v[1]
end

function load_config(; fname = nothing, profile = nothing)::Config
    isnothing(fname) && (fname = "~/.rai/config")
    isnothing(profile) && (profile = "default")
    stanza = _load_stanza(fname, profile)
    region = _get_value(stanza, "region")
    scheme = _get_value(stanza, "scheme")
    host = _get_value(stanza, "host")
    port = _get_value(stanza, "port")
    client_id = _get_value(stanza, "client_id")
    client_secret = _get_value(stanza, "client_secret")
    credentials = nothing
    if !isnothing(client_id) && !isnothing(client_secret)
        client_credentials_url = _get_value(stanza, "client_credentials_url")
        credentials = ClientCredentials(client_id, client_secret, client_credentials_url)
    end
    return Config(region, scheme, host, port, credentials)
end
