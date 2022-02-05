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

# Load a CSV file into the given database with the given relation name.

using RAI: Context, HTTPError, load_config, load_csv

include("parseargs.jl")

# Returns the file name without path and extension.
_sansext(fullname) = first(splitext(last(splitdir(fullname))))

function run(database, engine, relation, fullname; profile, kw...)
    isnothing(relation) && (relation = _sansext(fullname))
    data = read(fullname, String)
    cfg = load_config(; profile = profile)
    ctx = Context(cfg)
    rsp = load_csv(ctx, database, engine, relation, data; kw...)
    println(rsp)
end

function main()
    args = parseargs(
        "database", Dict(:help => "database name", :required => true),
        "engine", Dict(:help => "engine name", :required => true),
        "file", Dict(:help => "source file", :required => true),
        "--header-row", Dict(
            :help => "header row number, 0 for no header (default: 1)",
            :arg_type => Int),
        "--delim", Dict(:help => "field delimiter"),
        "--escapechar", Dict(:help => "character used to escape quotes"),
        "--quotechar", Dict(:help => "quoted field character"),
        ["--relation", "-r"], Dict(:help => "relation name (default: file name)"),
        "--profile", Dict(:help => "config profile (default: default)"))
    try
        run(args.database, args.engine, args.relation, args.file;
            delim = args.delim, header_row = args["header-row"],
            escapechar = args.escapechar, quotechar = args.quotechar,
            profile = args.profile)
    catch e
        e isa HTTPError ? show(e) : rethrow()
    end
end

main()
