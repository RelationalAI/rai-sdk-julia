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

# Low level HTTP interface to the RAI REST API. Handles authentication of
# requests and other protocol level details.

using Dates: now, datetime2unix
import HTTP
import JSON3

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
    audience::String
end
# todo: consider use of kwargs like we do in the python SDK
#   consider use of @Base.kwdef
function Context(cfg::Config)
    region = !isnothing(cfg.region) ? cfg.region : "us-east"
    scheme = !isnothing(cfg.scheme) ? cfg.scheme : "https"
    host = !isnothing(cfg.host) ? cfg.host : "azure.relationalai.com"
    port = !isnothing(cfg.port) ? cfg.port : "443"
    audience = !isnothing(cfg.audience) ? cfg.audience : "https://$(host)"
    return Context(region, scheme, host, port, cfg.credentials, audience)
end

# Answers if the given `headers` contains a key that is a case insensitive
# match for the given key.
function _haskeyfold(headers, key::AbstractString)::Bool
    key = lowercase(key)
    for (k, _) in headers
        key == lowercase(k) && return true
    end
    return false
end

# Returns the default User-Agent string for this library.
function _user_agent()
    return "rai-sdk-julia/$PROJECT_VERSION"
end

# Ensures that the given headers contain the required values.
function _ensure_headers!(h = HTTP.Headers())
    _haskeyfold(h, "accept") || push!(h, "Accept" => "application/json")
    _haskeyfold(h, "content-type") || push!(h, "Content-Type" => "application/json")
    _haskeyfold(h, "user-agent") || push!(h, "User-Agent" => _user_agent())
    return h
end

function get_access_token(ctx::Context, creds::ClientCredentials)::AccessToken
    url = _get_client_credentials_url(creds)
    h = _ensure_headers!()
    body = """{
        "client_id": $(repr(creds.client_id)),
        "client_secret": $(repr(creds.client_secret)),
        "audience": $(repr(ctx.audience)),
        "grant_type": "client_credentials"
    }"""
    opts = (redirect = false, retry_non_idempotent = true, connect_timeout = 30, readtimeout = 30, keepalive = true)
    rsp = HTTP.request("POST", url, h, body; opts...)
    data = JSON3.read(rsp.body)
    return AccessToken(data.access_token, data.scope, data.expires_in, datetime2unix(now()))
end

# cache name
function _cache_file()
    return joinpath(homedir(), ".rai", "tokens.json")
end

# read oauth cache
function _read_cache()
    try
        if isfile(_cache_file())
            return copy(JSON3.read(read(_cache_file())))
        else
            return nothing
        end
    catch e
        @warn e
        return nothing
    end
end

# Read access token from cache
function _read_token_cache(creds::ClientCredentials)
    try
        cache = _read_cache()
        cache === nothing && return nothing

        if haskey(cache, Symbol(creds.client_id))
            access_token = cache[Symbol(creds.client_id)]
            return AccessToken(
                access_token[:access_token],
                access_token[:scope],
                access_token[:expires_in],
                access_token[:created_on],
            )
        else
            return nothing
        end
    catch e
        @warn e
        return nothing
    end
end

# Write access token to cache
function _write_token_cache(creds::ClientCredentials)
    try
        cache = _read_cache()
        if cache === nothing
            cache = Dict(creds.client_id => creds.access_token)
        else
            cache[Symbol(creds.client_id)] = creds.access_token
        end
        write(_cache_file(), JSON3.write(cache))
    catch e
        @warn e
    end
end

function _get_client_credentials_url(creds::ClientCredentials)
    return !isnothing(creds.client_credentials_url) ?
           creds.client_credentials_url : "https://login.relationalai.com/oauth/token"
end

function _authenticate!(ctx::Context, headers)
    if !isnothing(ctx.credentials)
        _authenticate!(ctx, ctx.credentials, headers)
    end
    return nothing
end

function _authenticate!(
    ctx::Context,
    creds::ClientCredentials,
    headers,
)::Nothing
    if isnothing(creds.access_token)
        creds.access_token = _read_token_cache(creds)
        if isnothing(creds.access_token)
            creds.access_token = get_access_token(ctx, creds)
            _write_token_cache(creds)
        end
    end

    if isexpired(creds.access_token)
        creds.access_token = get_access_token(ctx, creds)
        _write_token_cache(creds)
    end

    push!(headers, "Authorization" => "Bearer $(creds.access_token.access_token)")
    return nothing
end

# Note, this function is deliberately patterend on the HTTP.jl request function.
function request(
    ctx::Context, method, url, h = HTTP.Header[], b = UInt8[];
    headers = h, query = nothing, body = b, kw...
)::HTTP.Response
    isnothing(body) && (body = UInt8[])
    headers = _ensure_headers!(headers)
    _authenticate!(ctx, headers)
    opts = (;redirect = false, connection_limit = 4096)
    rsp = HTTP.request(method, url, headers; query = query, body = body, opts..., kw...)
    if rsp.status >= 400
        @warn rsp
        request_id = HTTP.header(rsp, "X-Request-Id")
        throw(HTTPError(rsp.status, "x-request-id: $request_id\n$(String(rsp.body))"))
    end
    return rsp
end
