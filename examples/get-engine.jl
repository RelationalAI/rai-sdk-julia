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

# Fetch details of the given engine.

using RAI: Context, HTTPError, load_config, get_engine

include("parseargs.jl")

function run(engine; profile)
    cfg = load_config(; profile = profile)
    ctx = Context(cfg)
    rsp = get_engine(ctx, engine)
    println(rsp)
end

function main()
    args = parseargs(
        "engine", Dict(:help => "engine name", :required => true),
        "--profile", Dict(:help => "config profile (default: default)"))
    try
        run(args.engine; profile = args.profile)
    catch e
        e isa HTTPError ? show(e) : rethrow(e)
    end
end

main()
