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

# Exeecute the given query string.

using RAI: Context, HTTPError, exec, load_config, show_result

include("parseargs.jl")

function run(database, engine, source; profile)
    conf = load_config(; profile = profile)
    ctx = Context(conf)
    rsp = exec_v1(ctx, database, engine, source)
    show_result(rsp)
end

function main()
    args = parseargs(
        "database", Dict(:help => "database name", :required => true),
        "engine", Dict(:help => "engine name", :required => true),
        "command", Dict(:help => "rel source string"),
        ["--file", "-f"], Dict(:help => "rel source file"),
        "--readonly", Dict(:help => "readonly query (default: false)", :action => "store_true"),
        "--profile", Dict(:help => "config profile (default: default)"))
    try
        source = nothing
        if !isnothing(args.command)
            source = args.command
        elseif !isnothing(args.file)
            source = open(args.file, "r")
        end
        isnothing(source) && return  # nothing to execute
        run(args.database, args.engine, source; profile = args.profile)
    catch e
        e isa HTTPError ? show(e) : rethrow()
    end
end

main()
