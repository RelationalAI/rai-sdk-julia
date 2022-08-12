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

using Dates: DateTime, Second

struct AccessToken
    token::String
    scope::String
    expires_in::Int  # seconds
    created_on::DateTime
end

function Base.show(io::IO, t::AccessToken)
    print(
        io,
        "(",
        isempty(t.token) ? "" : "$(t.token[1:3])...",
        ", ", t.scope,
        ", ", t.expires_in,
        ", ", t.created_on,
        ")"
    )
end

function isexpired(access_token::AccessToken)::Bool
    expires_on = access_token.created_on + Second(access_token.expires_in)
    return expires_on - Second(5) < now() # anticipate token expiration by 5 seconds
end

abstract type Credentials end

mutable struct ClientCredentials <: Credentials
    client_id::String
    client_secret::String
    client_credentials_url::Union{String,Nothing}
    access_token::Union{AccessToken,Nothing}
    ClientCredentials(client_id, client_secret, client_credentials_url = nothing) =
        new(client_id, client_secret, client_credentials_url, nothing)
end

function Base.show(io::IO, c::ClientCredentials)
    print(
        io,
        "(",
        c.client_id,
        c.client_secret == nothing ? "" : ", $(c.client_secret[1:3])...",
        c.access_token == nothing ? "" : ", $(c.access_token)",
        ", ",
        c.client_credentials_url,
        ")"
    )
end
