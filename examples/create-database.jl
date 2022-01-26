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

# Create a new database, optionally overwriting an existing database.

using ArgParse
using RAI: Context, load_config, create_database, get_database

# Answers if the given state is a terminal state.
#def is_term_state(state: str) -> bool:
#    return state == "CREATED" or ("FAILED" in state)

s = add_arg_table!(ArgParseSettings(),
    "database", Dict(:help=>"database name"),
    "engine", Dict(:help=>"engine name"),
    "--overwrite", Dict(:help=>"overwrite existing database", :action=>"store_true"),
    "--profile", Dict(:help=>"config profile (default: default)"))
args = parse_args(ARGS, s)

conf = load_config(; profile = args["profile"])
ctx = Context(conf)
rsp = create_database(ctx, args["database"], args["engine"]; overwrite = args["overwrite"])
##while True:  # wait for request to reach terminal state
##    time.sleep(3)
##    rsp = api.get_database(ctx, database)
##    if is_term_state(rsp["state"]):
##        break
println(rsp)
