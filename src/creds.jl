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

using Dates: datetime2unix

"""
    AccessToken

Represents the oauth access token.
Please not that token is hidden by default when displaying AccessToken
but we can still print the secret if needed:

Example:
```
t.access_token
````
"""
struct AccessToken
    access_token::String
    scope::String
    expires_in::Int  # seconds
    created_on::Float64
end

function Base.show(io::IO, t::AccessToken)
    print(
        io,
        "(",
        isempty(t.access_token) ? "" : "$(t.access_token[1:3])...",
        ", ", t.scope,
        ", ", t.expires_in,
        ", ", t.created_on,
        ")"
    )
end

function isexpired(access_token::AccessToken)::Bool
    expires_on = access_token.created_on + access_token.expires_in
    return expires_on - 60 < datetime2unix(now()) # anticipate token expiration by 60 seconds
end

abstract type Credentials end

"""
    ClientCredentials

Represents the client credentials object.
Please not that client_secret is hidden by default when displaying ClientCredentials
but we can still print the secret if needed:

Example:
```
credentials.client_secret
````
"""
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
        c.client_secret === nothing ? "" : ", $(c.client_secret[1:3])...",
        c.access_token === nothing ? "" : ", $(c.access_token)",
        ", ",
        c.client_credentials_url,
        ")"
    )
end
