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

struct AccessToken
    token::String    # json:access_token
    Scope::String    # json:scope
    expires_in::Int  # json:expires_in  token duration in seconds
    created_on       # json:created_on
end

abstract type Credentials end

struct ClientCredentials <: Credentials
    client_id::String
    client_secret::String
    client_credentials_url::Union{String,Nothing}
end
