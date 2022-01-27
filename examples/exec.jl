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

using ArgParse
using RAI: Context, load_config, exec

s = add_arg_table!(ArgParseSettings(),
    "database", Dict(:help => "database name"),
    "engine", Dict(:help => "engine name"),
    "command", Dict(:help => "rel source string"),
    "--readonly", Dict(:help => "readonly query (default: false)", :action => "store_true"),
    "--profile", Dict(:help => "config profile (default: default)"))
args = parse_args(ARGS, s)

conf = load_config(; profile = args["profile"])
ctx = Context(conf)
rsp = exec(ctx, args["database"], args["engine"], args["command"])
println(rsp)
