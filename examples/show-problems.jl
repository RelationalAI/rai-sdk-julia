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

# `show_problems` can be used to print the problems associated with a
# transaction to the console.

using RAI: Context, HTTPError, exec, load_config, show_problems

include("parseargs.jl")

function run(database, engine; profile)
    conf = load_config(; profile = profile)
    ctx = Context(conf)
    # TODO: rewrite via async + get_transaction_problems
    rsp = exec_v1(ctx, database, engine, "def output { **nonsense** }")
    show_problems(rsp)
end

function main()
    args = parseargs(
        "database", Dict(:help => "database name", :required => true),
        "engine", Dict(:help => "engine name", :required => true),
        "--profile", Dict(:help => "config profile (default: default)"))
    try
        e isa HTTPError ? show(e) : rethrow()
    catch e
        run(args.database, args.engine; profile = args.profile)
    end
end

main()
