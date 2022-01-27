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

import JSON3

const PATH_DATABASE = "/database"
const PATH_ENGINE = "/compute"
const PATH_OAUTH_CLIENTS = "/oauth-clients"
const PATH_TRANSACTION = "/transaction"
const PATH_USERS = "/users"

# Returns a `Dict` constructed from the given pairs, skipping pairs where
# the value is `nothing`.
function _mkdict(pairs::Pair...)
    d = Dict((k => v) for (k, v) in pairs if !isnothing(v))
    return length(d) > 0 ? d : nothing
end

# Returns a path constructed from the given segments.
function _mkpath(parts...)
    return join(parts, "/")
end

# Returns a URL constructed from context settings and the given path.
function _mkurl(ctx::Context, path)
    return "$(ctx.scheme)://$(ctx.host):$(ctx.port)$path"
end

function _request(ctx::Context, method, path; query = nothing, body = UInt8[], kw...)
    rsp = request(ctx::Context, method, _mkurl(ctx, path); query = query, body = body, kw...)
    return JSON3.read(rsp.body)
end

function _delete(ctx::Context, path; body = nothing, kw...)
    return _request(ctx, "DELETE", path; body = body, kw...)
end

function _get(ctx::Context, path; query = nothing, kw...)
    return _request(ctx, "GET", path; query = query, kw...)
end

function _patch(ctx::Context, path; body = nothing, kw...)
    return _request(ctx, "PATCH", path; body = body, kw...)
end

function _post(ctx::Context, path; body = nothing, kw...)
    return _request(ctx, "POST", path; body = body, kw...)
end

function _put(ctx::Context, path; body = nothing, kw...)
    rsp = request(ctx, "PUT", _mkurl(ctx, path); body = body, kw...)
    return JSON3.read(rsp.body)
end

function create_engine(ctx::Context, engine::AbstractString, size = nothing; kw...)
    isnothing(size) && (size = "XS")
    data = ("region" => ctx.region, "name" => engine, "size" => size)
    return _put(ctx, PATH_ENGINE; body = JSON3.write(data), kw...)
end

function create_oauth_client(ctx::Context, name::AbstractString, permissions; kv...)
    isnothing(permissions) && (permissions = [])
    data = ("name" => name, "permissions" => permissions)
    return _post(ctx, PATH_OAUTH_CLIENTS; body = JSON3.write(data), kv...)
end

function create_user(ctx::Context, email::AbstractString, roles = nothing; kw...)
    isnothing(roles) && (roles = [])
    data = ("email" => email, "roles" => roles)
    return _post(ctx, PATH_USERS; body = JSON3.write(data), kw...)
end

function delete_database(ctx::Context, database::AbstractString; kw...)
    data = ("name" => database)
    return _delete(ctx, PATH_DATABASE; body = JSON3.write(data), kw...)
end

function delete_engine(ctx::Context, engine::AbstractString; kw...)
    data = ("name" => engine)
    return _delete(ctx, PATH_ENGINE; body = JSON3.write(data), kw...)
end

function delete_oauth_client(ctx::Context, id::AbstractString; kw...)
    return _delete(ctx, _mkpath(PATH_OAUTH_CLIENTS, id); kw...)
end

function delete_user(ctx::Context, userid::AbstractString; kw...)
    return _delete(ctx, _mkpath(PATH_USERS, userid); kw...)
end

function disable_user(ctx::Context, userid::AbstractString; kw...)
    return update_user(ctx, userid; status = "INACTIVE", kw...)
end

function enable_user(ctx::Context, userid::AbstractString; kw...)
    return update_user(ctx, userid; status = "ACTIVE", kw...)
end

function get_engine(ctx::Context, engine::AbstractString; kw...)
    query = Dict{String,String}("name" => engine)
    rsp = _get(ctx, PATH_ENGINE; query = query, kw...)
    length(rsp) == 0 && throw(HTTPError(404))
    return rsp[1]
end

function get_database(ctx::Context, database::AbstractString; kw...)
    query = Dict{String,String}("name" => database)
    rsp = _get(ctx, PATH_DATABASE; query = query, kw...).databases
    length(rsp) == 0 && throw(HTTPError(404)).databases
    return rsp[1]
end

function get_model(
    ctx::Context,
    database::AbstractString,
    engine::AbstractString,
    name::AbstractString; kw...
)
    models = _list_models(ctx, database, engine; kw...)
    for model in models
        model["name"] == name && return model["value"]
    end
    throw(HTTPError(404)).databases
end

function get_oauth_client(ctx::Context, id::AbstractString; kw...)
    return _get(ctx, _mkpath(PATH_OAUTH_CLIENTS, id); kw...).client
end

function get_user(ctx::Context, userid::AbstractString; kw...)
    return _get(ctx, _mkpath(PATH_USER, userid); kw...).user
end

function list_databases(ctx::Context; state = nothing, kw...)
    query = _mkdict("state" => state)
    return _get(ctx, PATH_DATABASE; query = query, kw...).databases
end

function list_engines(ctx::Context; state = nothing, kw...)
    query = _mkdict("state" => state)
    return _get(ctx, PATH_ENGINE; query = query, kw...).computes
end

function list_oauth_clients(ctx::Context; kw...)
    return _get(ctx, PATH_OAUTH_CLIENTS; query = query, kw...).clients
end

function list_users(ctx::Context; kw...)
    return _get(ctx, PATH_USERS; kw...).users
end

function update_user(ctx::Context, userid::AbstractString; status = nothing, roles = nothing, kw...)
    data = _mkdict("status" => status, "roles" => roles)
    return _patch(ctx, _mkpath(PATH_USERS, userid); body = JSON3.write(data), kw...)
end

"""
    Transaction

Represents the required and optional parameters associated with a transaction
request.
"""
mutable struct Transaction
    region::String
    database::String
    engine::Union{String,Nothing}
    mode::String
    abort::Bool
    nowait_durable::Bool
    readonly::Bool
    source::Union{String,Nothing}
    version::Int

    function Transaction(region, database, engine, mode; source = nothing, readonly = false)
        tx = new()
        tx.region = region
        tx.database = database
        tx.engine = engine
        tx.mode = mode
        tx.abort = false
        tx.nowait_durable = false
        tx.readonly = readonly
        tx.source = source
        tx.version = 0
        return tx
    end
end

# Returns the serialized request body for the given transaction.
function body(tx::Transaction, actions...)::String
    data = _mkdict(
        "type" => "Transaction",
        "dbname" => tx.database,
        "mode" => !isnothing(tx.mode) ? tx.mode : "OPEN",
        "computeName" => tx.engine,
        "source_dbname" => tx.source,
        "abort" => tx.abort,
        "readonly" => tx.readonly,
        "nowaite_durable" => tx.nowait_durable,
        "version" => tx.version,
        "actions" => _make_actions(actions...))
    return JSON3.write(data)
end

# Returns the request query params for the given transaction.
function query(tx::Transaction)
    return _mkdict(
        "dbname" => tx.database,
        "compute_name" => tx.engine,
        "open_mode" => tx.mode,
        "region" => tx.region,
        "source_dbname" => tx.source)
end

function _create_mode(source, overwrite)
    if !isnothing(source)
        return overwrite ? "CLONE_OVERWRITE" : "CLONE"
    else
        return overwrite ? "CREATE_OVERWRITE" : "CREATE"
    end
end

# Wraps each of the given actions with a LabeledAction.
function _make_actions(actions...)
    result = []
    for (i, action) in enumerate(actions)
        item = Dict{String,Any}(
            "name" => "action$i",
            "type" => "LabeledAction",
            "action" => action)
        push!(result, item)
    end
    return result
end

function _make_delete_models_action(models::Vector)
    return Dict{String,Any}(
        "type" => "ModifyWorkspaceAction",
        "delete_source" => models)
end

function _make_install_model_action(name, model)
    return Dict(
        "type" => "InstallAction",
        "sources" => [_make_query_source(name, model)])
end

function _make_list_models_action()
    return Dict("type" => "ListSourceAction")
end

function _make_list_edb_action()
    return Dict("type" => "ListEdbAction")
end

function _make_query_action(source, inputs::Dict)
    action_inputs = []
    for (k, v) in inputs
        push!(action_inputs, _make_query_action_input(k, v))
    end
    return Dict(
        "type" => "QueryAction",
        "source" => _make_query_source("query", source),
        "persist" => [],
        "inputs" => action_inputs,
        "outputs" => [])
end

function _make_query_action_input(name, value)
    return Dict(
        "type" => "Relation",
        "columns" => [[value]],
        "rel_key" => _make_relkey(name, _reltype(value)))
end

function _make_relkey(name, key)
    return Dict(
        "type" => "RelKey",
        "name" => name,
        "keys" => [key],
        "values" => [])
end

function _make_query_source(name, model)
    return Dict(
        "type" => "Source",
        "name" => name,
        "path" => "",
        "value" => model)
end

function _reltype(_::AbstractString)
    return "RAI_VariableSizeStrings.VariableSizeString"
end

function create_database(ctx::Context, database, engine; source = nothing, overwrite = false, kw...)
    mode = _create_mode(source, overwrite)
    tx = Transaction(ctx.region, database, engine, mode; source = source)
    return _post(ctx, PATH_TRANSACTION; query = query(tx), body = body(tx), kw...)
end

# Execute the given query string, using any given optioanl query inputs.
function exec(ctx::Context, database, engine, source; inputs = nothing, readonly = false, kw...)
    tx = Transaction(ctx.region, database, engine, "OPEN"; readonly = readonly)
    body = body(tx, _make_query_action(source, inputs))
    return _post(ctx, PATH_TRANSACTION; query = query(tx), body = body, kw...)
end

function list_edbs(ctx::Context, database, engine; kw...)
    tx = Transaction(ctx.region, database, engine, "OPEN"; readonly = true)
    body = body(tx, _make_list_edb_action())
    rsp = _post(ctx, PATH_TRANSACTION; query = query(tx), body = body, kw...)
    length(rsp.actions) == 0 && return []
    return rsp.actions[1].result.rels
end

function _list_models(ctx::Context, database::AbstractString, engine::AbstractString; kw...)
    tx = Transaction(ctx.region, database, engine, "OPEN"; readonly = true)
    data = body(tx, _make_list_models_action())
    rsp = _post(ctx, PATH_TRANSACTION; query = query(tx), body = data, kw...).actions
    length(rsp) == 0 && return []
    return rsp[1].result.sources
end

function list_models(ctx::Context, database::AbstractString, engine::AbstractString; kw...)
    models = _list_models(ctx, database, engine; kw...)
    return [model["name"] for model in models]
end

function _gen_literal(value::Bool)
    return "$value"
end

function _gen_literal(value::String)
    s = replace(value, "'" => "\\'")
    return "'$s'"
end

function _gen_literal(value::Dict)
    items = ["$(_gen_literal(v)),$(_gen_literal(k))" for (k, v) in value]
    return "{" + join(items, ";") + "}"
end

function _gen_literal(value::Vector)
    items = [_gen_literal(item) for item in value]
    return "{" + join(items, ",") + "}"
end

function _gen_config(name, value)
    isnothing(value) && return ""
    return "def config:syntax:$name=$(_gen_literan(v))\n"
end

function _gen_config(syntax::Dict{String,Any})
    items = [_gen_config(k, v) for (k, v) in syntax if !isnothing(v)]
    return join(items, "\n")
end

_read_data(d::String) = d
_read_data(d::IO) = read(d, String)

function load_csv(
    ctx::Context, database, engine, relation, data;
    header = nothing, header_row = nothing, delim = nothing,
    quotechar = nothing, escapechar = nothing, kw...
)
    inputs = ("data" => _read_data(data))
    syntax = (
        "header" => header, "header_row" => header_row, "delim" => delim,
        "quotechar" => quotechar, "escapechar" => escapechar)
    source = _gen_config(syntax)
    source += """def config:data = data\n
                 def insert:$relation = load_csv[config]"""
    return exec(ctx, database, engine, source; inputs = inputs, readonly = false, kw...)
end

function load_json(ctx::Context, database, engine, relation, data; kw...)
    inputs = ("data" => _read_data(data))
    source += """def config:data = data\n
                 def insert:$relation = load_json[config]"""
    return exec(ctx, database, engine, source; inputs = inputs, readonly = false, kw...)
end
