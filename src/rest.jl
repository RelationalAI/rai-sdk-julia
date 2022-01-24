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

import HTTP

"""
    Context

Contains the settings required to make requests against the RelationalAI
REST APIs in the given region, using the given credentials. It's also possible
to set the host, port and URL scheme, although typically clients will use the
default values for those settings.
"""
struct Context
    region::String
    scheme::String
    host::String
    port::String
    credentials::Union{Credentials,Nothing}

    Context(region, scheme, host, port, credentials) =
        new(region, scheme, host, port, credentials)

    function Context(cfg::Config)
        region = !isnothing(cfg.region) ? cfg.region : "eastus"
        scheme = !isnothing(cfg.scheme) ? cfg.scheme : "https"
        host = !isnothing(cfg.host) ? cfg.host : "azure.relationalai.com"
        port = !isnothing(cfg.port) ? cfg.port : "443"
        new(region, scheme, host, port, cfg.credentials)
    end
end

# Answers if the given `headers` contains a key that is a case insensitive
# match for the given key.
function _haskeyfold(headers::HTTP.Headers, key::AbstractString)::Bool
    key = lowercase(key)
    for (k, _) in headers
        key == lowercase(k) && return true
    end
    return false
end

# Returns the default User-Agent string for this library.
function _default_user_agent()::String
    return "rai-sdk-julia/$VERSION"
end

# Ensures that the given headers contain the required values.
function _ensure_headers!(
    ctx::Context, h::HTTP.Headers=HTTP.Headers()
)::HTTP.Headers
    !_haskeyfold(h, "accept") &&
        push!(h, "Accept" => "application/json")
    !_haskeyfold(h, "content-type") &&
        push!(h, "Content-Type" => "application/json")
    !_haskeyfold(h, "host") &&
        push!(h, "Host" => ctx.host)
    !_haskeyfold(h, "user-agent") &&
        push!(h, "User-Agent" => _default_user_agent())
    return h
end

const _default_client_credentials_url =
    "https://login.relationalai.com/oauth/token"

function get_access_token(ctx::Context, creds::ClientCredentials)
    url = !isnothing(creds.client_credentials_url) ?
        creds.client_credentials_url : _default_client_credentials_url
    h = _ensure_headers!(ctx)
    body = """{
        "client_id": "$(creds.client_id)",
        "client_secret": "$(creds.client_secret)",
        "audience": "https://$(ctx.host)",
        "grant_type": "client_credentials"
    }"""
    println(creds.client_id)
    println(creds.client_secret)
    println(url)
    println(body)
    rsp = HTTP.post(url, h, body)
    println(rsp)
end

function _authenticate!(ctx::Context, headers::HTTP.Headers)::Nothing
    _authenticate!(ctx, ctx.credentials, headers)
    return nothing
end

function _authenticate!(
    ctx::Context,
    creds::ClientCredentials,
    headers::HTTP.Headers
)::Nothing
    if isnothing(creds.access_token)
        creds.access_token = get_access_token(ctx, creds)
    end
    push!(headers, "Authorization" => "Bearer $(creds.access_token)")
    return nothing
end

function request(
    ctx::Context, method, url, h=HTTP.Header[], b=UInt8[];
    headers=h, query=nothing, body=b, kw...
)::HTTP.Response
    _ensure_headers!(ctx, headers)
    _authenticate!(ctx, headers)
    println(headers)
    #return request(method, url, headers, body; query=query, kw...)
end
