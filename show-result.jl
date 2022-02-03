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

# There are several ways to display transaction results in a friendly way:
#
#     show_result(rsp)
#
# .. which is equivalent to:
#
#     show(TransactionResult(rsp))
#
# .. or, if you want to navigate the relations explicitly:
#
#     for relation in result.relations
#         show(relation)
#     end

using RAI: Context, HTTPError, exec, load_config, show_result

include("parseargs.jl")

const source = """
def output = 
    :drink, "martini", 2, 12.50, "2020-01-01";
    :drink, "sazerac", 4, 14.25, "2020-02-02";
    :drink, "cosmopolitan", 4, 11.00, "2020-03-03";
    :drink, "bellini", 3, 12.25, "2020-04-04"
"""

function run(database, engine; profile)
    conf = load_config(; profile = profile)
    ctx = Context(conf)
    rsp = exec(ctx, database, engine, source)
    show_result(rsp)
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
