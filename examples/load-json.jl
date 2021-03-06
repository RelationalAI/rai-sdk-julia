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

# Load a JSON file into the given database with the given relation name.

using RAI: Context, HTTPError, load_config, load_json

include("parseargs.jl")

# Returns the file name without path and extension.
_sansext(fullname) = first(splitext(last(splitdir(fullname))))

function run(database, engine, relation, fullname; profile)
    isnothing(relation) && (relation = _sansext(fullname))
    data = read(fullname, String)
    cfg = load_config(; profile = profile)
    ctx = Context(cfg)
    rsp = load_json(ctx, database, engine, relation, data)
    println(rsp)
end

function main()
    args = parseargs(
        "database", Dict(:help => "database name", :required => true),
        "engine", Dict(:help => "engine name", :required => true),
        "file", Dict(:help => "source file", :required => true),
        ["--relation", "-r"], Dict(:help => "relation name (default: file name)"),
        "--profile", Dict(:help => "config profile (default: default)"))
    try
        run(args.database, args.engine, args.relation, args.file; profile = args.profile)
    catch e
        e isa HTTPError ? show(e) : rethrow()
    end
end

main()
