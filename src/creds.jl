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

using Dates: DateTime

struct AccessToken
    token::String
    scope::String
    expires_in::Int  # seconds
    created_on::DateTime
end

function isexpired(access_token::AccessToken)::Bool
    expires_in = access_token.expires_in * 1000  # millis
    expires_on = access_token.created_on + expires_in
    return expires_on > now()
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
