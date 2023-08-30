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
# limitations under the License

# List engines, optionally filtered by state.

using RAI: Context, HTTPError, load_config, get_transaction_profile
using JSON3
using HTTP

include("parseargs.jl")

function run(; id, profile)
    conf = load_config(; profile = profile)
    ctx = Context(conf)
    
    return get_transaction_profile(ctx, id)
end

function main()
    args = parseargs(
        "--id", Dict(:help => "transaction id"),
        "--profile", Dict(:help => "config profile (default: default)"))
    try
        res = run(; id = args.id, profile = args.profile)
        JSON3.write(res)
    catch e
        e isa HTTPError ? show(e) : rethrow()
    end
end

main()
