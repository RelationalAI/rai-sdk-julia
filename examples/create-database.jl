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

# Create a database, optionally overwriting an existing database.

using ArgParse
using RAI: Context, load_config, create_database, get_database

# Answers if the given value represents a terminal state.
is_term_state(state) = state == "CREATED" || occursin("FAILED", state)

function run(database, engine, overwrite)
    conf = load_config(; profile = args["profile"])
    ctx = Context(conf)
    rsp = create_database(ctx, database, engine; overwrite = overwrite)
    while !is_term_state(get(rsp, "state", ""))  # wait for terminal state
        sleep(3)
        rsp = get_database(ctx, database)
    end
    println(rsp)
end

s = add_arg_table!(ArgParseSettings(),
    "database", Dict(:help => "database name", :required => true),
    "engine", Dict(:help => "engine name", :required => true),
    "--overwrite", Dict(:help => "overwrite existing database", :action => "store_true"),
    "--profile", Dict(:help => "config profile (default: default)"))
args = parse_args(ARGS, s)
run(args["database"], args["engine"], args["overwrite"])
