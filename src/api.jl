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
const PATH_USER = "/users"

function _mkpath(parts...)
    return join(parts, "/")
end

function _mkurl(ctx::Context, path)
    return "$(ctx.scheme)://$(ctx.host):$(ctx.port)$path"
end

function _get(ctx::Context, path; query = nothing, kw...)
    rsp = request(ctx, "GET", _mkurl(ctx, path); query = query, kw...)
    return JSON3.read(rsp.body)
end

function create_database(ctx::Context, database, engine, source, overwrite = false)
    # todo
end

function create_engine(ctx::Context, engine, size = "XS")
    # todo
end

function create_user(ctx::Context, email, roles = nothing)
    # todo
end

function create_oauth_client(ctx::Context, name, permissions)
    # todo
end

function delete_engine(ctx::Context, engine)
    # todo
end

function delete_database(ctx::Context, database)
    # todo
end

function delete_oauth_client(ctx::Context, id)
    # todo
end

function delete_user(ctx::Context, userid)
    # todo
end

function disable_user(ctx::Context, userid)
    # todo
end

function enable_user(ctx::Context, userid)
    # todo
end

function get_engine(ctx::Context, engine; kw...)
    query = ("name" => engine)
    rsp = _get(ctx, PATH_ENGINE; query = query, kw...)
    length(rsp) == 0 && throw(HTTPError(404))
    return rsp[1]
end

function get_database(ctx::Context, database; kw...)
    query = ("name" => database)
    rsp = _get(ctx, PATH_DATABASE; query = query, kw...).engine
    length(rsp) == 0 && throw(HTTPError(404))
    return rsp[1]
end

function get_oauth_client(ctx::Context, id; kw...)
    return _get(ctx, _mkpath(PATH_OAUTH_CLIENTS, id); kw...).client
end

function get_user(ctx::Context, userid; kw...)
    return _get(ctx, _mkpath(PATH_USER, userid); kw...).user
end

function list_engines(ctx::Context; query = nothing, kw...)
    return _get(ctx, PATH_ENGINE; query = query, kw...).computes
end

function list_databases(ctx::Context; query = nothing, kw...)
    return _get(ctx, PATH_DATABASE; query = query, kw...).databases
end

function list_oauth_clients(ctx::Context; query = nothing, kw...)
    return _get(ctx, PATH_OAUTH_CLIENTS; query = query, kw...).clients
end

function list_users(ctx::Context; query = nothing, kw...)
    return _get(ctx, PATH_USER; query = query, kw...).users
end

function update_user(ctx::Context, userid, status = nothing, roles = nothing)
    # todo
end

# todo: transaction
