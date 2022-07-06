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

# Execute the given query string.
# Example:
# $ julia --proj=. examples/exec_async.jl "nhd-test-1" "nhd-s" "2+2"
# Transaction is done...
# JSON3.Object{Vector{UInt8}, Vector{UInt64}} with 2 entries:
#   :id    => "261f5d59-f1b4-f778-4c13-a7993871c972"
#   :state => "CREATED"

import RAI
using RAI: Context, HTTPError, exec_async, load_config, show_result, get_transaction

include("parseargs.jl")

function run(database, engine, source; profile)
    conf = load_config(; profile = profile)
    ctx = Context(conf)
    txn = exec_async(ctx, database, engine, source)
    println("Transaction is created...")
    display(txn)
    println()
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
