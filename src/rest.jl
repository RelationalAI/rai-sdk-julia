import HTTP

Credentials = Union{ClientCredentials,Nothing}

Headers = Dict{String,String}

QueryArgs = Dict{String,Union{String,Array{String,1}}}

"""
    Context

Contains the settings required to make requests against the RelationalAI
REST APIs in the given region, using the given credentials. It's also possible
to set the host, port and scheme, although typically clients will use the
default values for those settings.
"""
struct Context
    region::String
    scheme::String
    host::String
    port::String
    credentials::{Credentials, Nothing}

    Context(region, scheme, host, port, credentials) =
        new(region, scheme, host, port, credentials)

    Context(; region = "eastus", scheme = "https", host = "azure.relationalai.com", port = "443", credentials = nothing) =
        new(region, scheme, host, port, credentials)

    function Context(cfg::Config)
        region = !isnothing(cfg.region) ? cfg.region : "eastus"
        scheme = !isnothing(cfg.scheme) ? cfg.scheme : "https"
        host = !isnothing(cfg.host) ? cfg.host : "azure.relationalai.com"
        port = !isnothing(cfg.port) ? cfg.port : "443"
        new(region, scheme, host, port, cfg.credentials)
    end
end

# Answers if the given `Dict` contains a key that is a case insensitive match
# for the given term.
function _haskey_insensitive(d::Dict{String,String}, term::String)::Bool
    term = lowercase(term)
    for k in keys(d)
        term == lowercase(k) && return true
    end
    return false
end

# Returns the default User-Agent string for this library.
function _default_user_agent()::String
    return "rai-sdk-julia/$VERSION"
end

function _default_headers!(ctx::Context, h::Headers)::Nothing
    if isnothing(h)
        h = Headers()
    end
    if !_haskey_insensitive(h, "accept")
        h["Accept"] = "application/json"
    end
    if !_haskey_insensitive(h, "content-type")
        h["Content-Type"] = "application/json"
    end
    if !_haskey_insensitive(h, "host")
        h["Host"] = ctx.host
    end
    if !_haskey_insensitive(h, "user-agent")
        h["User-Agent"] = _default_user_agent()
    end
    return nothing
end

mutable struct Request
    ctx::Context
    method::String
    url::String
    args::Union{QueryArgs,Nothing}
    headers::Union{Headers,Nothing}
    data

    Request(
        ctx::Context,
        method::AbstractString,
        url::AbstractString;
        args::QueryArgs = nothing,
        headers::Headers = nothing,
        data = nothing
    ) = new(ctx, method, url, args, headers, data)
end

function request(
    ctx::Context,
    method::AbstractString,
    url::AbstractString;
    args::QueryArgs = nothing,
    headers::Headers = nothing,
    data = nothing,
    kw...
)
    if isnothing(args)
        args = QueryArgs()
    end
    _default_headers!(ctx, headers)
    req = Request(ctx, method, url; args = args, headers = headers, data = data)
end
