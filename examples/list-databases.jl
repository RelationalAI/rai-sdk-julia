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
# limitations under the License.

using ArgParse
using RAILib: Context, load_config, list_databases

s = ArgParseSettings()
@add_arg_table s begin
    "--state" help = "state filter (default: nothing)"
    "--profile" help = "config profile (default: default)"
end
args = parse_args(ARGS, s)

conf = load_config(; profile=args["profile"])
ctx = Context(conf)
rsp = list_databases(ctx)
println(rsp)
