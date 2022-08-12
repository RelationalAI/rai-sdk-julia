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

# Create a database and print the response.

using RAI: Context, HTTPError, load_config, create_database

include("parseargs.jl")

# Answers if the given value represents a terminal state.
is_term_state(state) = state == "CREATED" || occursin("FAILED", state)

function run(database; profile)
    conf = load_config(; profile = profile)
    ctx = Context(conf)
    # NOTE: create_database is a synchronous operation, so there is no need to poll.
    rsp = create_database(ctx, database)
    println(rsp)
end

function main()
    args = parseargs(
        "database", Dict(:help => "database name", :required => true),
        "--profile", Dict(:help => "config profile (default: default)"))
    try
        run(args.database; profile = args.profile)
    catch e
        e isa HTTPError ? show(e) : rethrow()
    end
end

main()
