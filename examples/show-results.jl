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

using RAI: Context, HTTPError, load_config, exec

include("parseargs.jl")

function run(database, engine; profile)
    conf = load_config(; profile = profile)
    ctx = Context(conf)
    source = "x, x^2, x^3, x^4 from x in {1; 2; 3; 4; 5}"
    rsp = exec(ctx, database, engine, source)
    println(rsp)
end

function main()
    args = parseargs(
        "database", Dict(:help => "database name", :required => true),
        "engine", Dict(:help => "engine name", :required => true),
        "--profile", Dict(:help => "config profile (default: default)"))
    try
        run(args.database, args.engine; profile = args.profile)
    catch e
        e isa HTTPError ? show(e) : rethrow(e)
    end
end

main()
