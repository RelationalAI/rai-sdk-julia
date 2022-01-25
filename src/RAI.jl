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

"""
    RAI

The `RAI` module provides functionality for accessing the RelationalAI REST
APIs.
"""
module RAI

const VERSION = v"0.0.1"

export
    AccessToken,
    ClientCredentials,
    Config,
    Context

include("creds.jl")
include("config.jl")
include("rest.jl")
include("api.jl")

conf = load_config()
ctx = Context(conf)
rsp = list_databases(ctx)
println(rsp)

end # module
