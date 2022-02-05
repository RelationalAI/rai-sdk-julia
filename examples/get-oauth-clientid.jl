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

# Get the OAuth clientid corresponding to the given client name.

using RAI: Context, HTTPError, load_config, list_oauth_clients

include("parseargs.jl")

function get_clientid(ctx, name)
    rsp = list_oauth_clients(ctx)
    for item in rsp
        item["name"] == name && return item["id"]
    end
    return nothing
end

function run(name; profile)
    cfg = load_config(; profile = profile)
    ctx = Context(cfg)
    rsp = get_clientid(ctx, name)
    println(rsp)
end

function main()
    args = parseargs(
        "name", Dict(:help => "client name", :required => true),
        "--profile", Dict(:help => "config profile (default: default)"))
    try
        run(args.name; profile = args.profile)
    catch e
        e isa HTTPError ? show(e) : rethrow()
    end
end

main()
