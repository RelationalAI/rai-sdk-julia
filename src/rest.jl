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

using Dates: now
import HTTP
import JSON3

struct HTTPError <: Exception
    status::Int
    message::String
    HTTPError(status, message) = new(status, message)
    HTTPError(status) = new(status, HTTP.statustext(status))
end


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
function _user_agent()
    return "rai-sdk-julia/$VERSION"
end

# Ensures that the given headers contain the required values.
function _ensure_headers!(h::HTTP.Headers = HTTP.Headers())::HTTP.Headers
    !_haskeyfold(h, "accept") && push!(h, "Accept" => "application/json")
    !_haskeyfold(h, "content-type") && push!(h, "Content-Type" => "application/json")
    !_haskeyfold(h, "user-agent") && push!(h, "User-Agent" => _user_agent())
    return h
end

function get_access_token(ctx::Context, creds::ClientCredentials)::AccessToken
    url = _get_client_credentials_url(creds)
    h = _ensure_headers!()
    body = """{
        "client_id": "$(creds.client_id)",
        "client_secret": "$(creds.client_secret)",
        "audience": "https://$(ctx.host)",
        "grant_type": "client_credentials"
    }"""
    opts = (readtimeout = 5, redirect = false, retry = false)
    rsp = HTTP.request("POST", url, h, body; opts...)
    data = JSON3.read(rsp.body)
    return AccessToken(data.access_token, data.scope, data.expires_in, now())
end

function _get_client_credentials_url(creds::ClientCredentials)::String
    return !isnothing(creds.client_credentials_url) ?
           creds.client_credentials_url : "https://login.relationalai.com/oauth/token"
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
    push!(headers, "Authorization" => "Bearer $(creds.access_token.token)")
    return nothing
end

function request(
    ctx::Context, method, url, h = HTTP.Header[], b = UInt8[];
    headers = h, query = nothing, body = b, kw...
)::HTTP.Response
    _ensure_headers!(headers)
    _authenticate!(ctx, headers)
    opts = (redirect = false, retry = false, status_exception = false)
    return HTTP.request(method, url, headers, body; query = query, opts...)
end
