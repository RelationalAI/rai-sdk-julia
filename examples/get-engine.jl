# Copyright 2021 RelationalAI, Inc.
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

using ArgParse
using RAI: Context, load_config, get_engine

s = add_arg_table!(ArgParseSettings(),
    "engine", Dict(:help => "engine name", :required => true),
    "--profile", Dict(:help => "config profile (default: default)"))
args = parse_args(ARGS, s)

cfg = load_config(; profile = args["profile"])
ctx = Context(cfg)
rsp = get_engine(ctx, args["engine"])
println(rsp)
