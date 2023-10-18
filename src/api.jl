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

# The primary API level interface to the RAI REST API. These entry points
# provide convient Juilia language bindings to the corresponding opeartions,
# but a minimum of additional functionality. The purpose is to present service
# functionality as direclty as possible, but in a way that is natural for the
# Julia language.

import Arrow
import ProtoBuf
using Base.Threads: @spawn
import Dates
import JSON3
using ExceptionUnwrapping: has_wrapped_exception, unwrap_exception_to_root

using Mocking: Mocking, @mock  # For unit testing, by mocking API server responses

const PATH_DATABASE = "/database"
const PATH_ENGINE = "/compute"
const PATH_OAUTH_CLIENTS = "/oauth-clients"
const PATH_TRANSACTION = "/transaction"
const PATH_ASYNC_TRANSACTIONS = "/transactions"
const PATH_USERS = "/users"
const ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"

const TXN_POLLING_OVERHEAD = 0.10

struct HTTPError <: Exception
    status_code::Int
    status_text::String
    details::Union{String,Nothing}
    HTTPError(status_code) = new(status_code, HTTP.statustext(status_code), nothing)
    HTTPError(status_code, details) = new(status_code, HTTP.statustext(status_code), details)
end

function Base.show(io::IO, e::HTTPError)
    println(io, "$(e.status_code) $(e.status_text)")
    isnothing(e.details) && return
    try
        println(io, JSON3.read(e.details))
    catch
        println(io, e.details)
    end
end

"""
    wait_until_done(ctx::Context, rsp::TransactionResponse) -> TransactionResponse
    wait_until_done(ctx::Context, transaction) -> TransactionResponse
    wait_until_done(ctx::Context, txn_id::String) -> TransactionResponse

Block until the `transaction` has reached a terminal state, and return the response.

Continuously polls get_transaction() for the transaction's state, until the transaction has
finished. A transaction has finished once it has reached one of the terminal states:
`COMPLETED` or `ABORTED`. The polling uses a low-overhead exponential backoff in order to
ensure low-latency results without overloading network traffic.
"""
function wait_until_done(ctx::Context, rsp::TransactionResponse;
    start_time_ns = nothing,  # deprecated
    start_time = nothing,
)
    wait_until_done(ctx, rsp.transaction; start_time_ns, start_time)
end
function wait_until_done(ctx::Context, txn::JSON3.Object;
    start_time_ns = nothing,  # deprecated
    start_time = nothing,
)
    if start_time_ns !== nothing
        start_time = start_time_ns / 1e9,
        @warn "wait_until_done(): start_time_ns= is deprecated; please pass start_time= as a unix timestamp instead."
    end

    # If the user is calling this manually, read the start time from the transaction object.
    if start_time === nothing &&
            # NOTE: the fast-path txn may not include the created_on key.
            haskey(txn, :created_on)
        start_time = _transaction_start_time(txn)
    end
    wait_until_done(ctx, transaction_id(txn); start_time)
end
function _transaction_start_time(txn::JSON3.Object)
    # The API returns *milliseconds* since the epoch
    return txn[:created_on] / 1e3
end
function wait_until_done(ctx::Context, id::AbstractString;
    start_time_ns = nothing,  # deprecated
    start_time = nothing,
)
    if start_time_ns !== nothing
        start_time = start_time_ns / 1e9,
        @warn "wait_until_done(): start_time_ns= is deprecated; please pass start_time= as a unix timestamp instead."
    end

    # If the user is calling this manually, read the start time from the transaction object.
    if start_time === nothing
        txn = get_transaction(ctx, id)
        start_time = _transaction_start_time(txn)
    end
    try
        _poll_with_specified_overhead(; overhead_rate = TXN_POLLING_OVERHEAD, start_time) do
            txn = get_transaction(ctx, id)
            return transaction_is_done(txn)
        end
        m = @spawn get_transaction_metadata(ctx, id)
        p = @spawn get_transaction_problems(ctx, id)
        r = @spawn get_transaction_results(ctx, id)
        try
            return TransactionResponse(txn, fetch(m), fetch(p), fetch(r))
        catch e
            # (We use has_wrapped_exception to unwrap the TaskFailedException.)
            if has_wrapped_exception(e, HTTPError) &&
                unwrap_exception_to_root(e).status_code == 404
                # This is an (unfortunately) expected case if the engine crashes during a
                # transaction, or the transaction is cancelled. The transaction is marked
                # as ABORTED, but it has no results.
                return TransactionResponse(txn, nothing, nothing, nothing)
            else
                rethrow()
            end
        end
    catch
        # Always print out the transaction id so that users can still get the txn ID even
        # if there's an error during polling (such as an InterruptException).
        @error "Client-side error while executing transaction." transaction_id=id
        rethrow()
    end
end

# Polls until the execution `f()` is truthy or the maximum number of polls is reached.
# Polling frequency is computed to minimize overhead: we carefully set the sleep time
# between polls to a fraction of the time waited so far, so that after any given sleep, we
# cannot have missed the upstream result by more than `overhead_rate` of the _actual time_.
# If `throw_on_timeout` is set to true, this will throw if the maximum number of iterations
# are reached.
function _poll_with_specified_overhead(
    f;
    overhead_rate,  # Add xx% overhead through polling.
    start_time = time(),  # Optional start time, otherwise defaults to now()
    n = typemax(Int), # Maximum number of polls
    max_delay = 120, # 2 min
    timeout_secs = Inf,  # no timeout by default
    throw_on_timeout = false,
)
    @debug "start time: $start_time"
    @assert overhead_rate >= 0.0
    local iter
    for i in 1:n
        iter = i
        @debug "polling: sending request"
        done = f()
        @debug "polling: request complete" done
        if done
            return nothing
        end
        t = @mock(time())
        @debug "time: $t"
        current_delay_s = t - start_time
        if current_delay_s > timeout_secs
            break
        end
        duration = current_delay_s * overhead_rate
        duration = min(duration, max_delay)  # clamp the duration as specified.
        @mock sleep(duration)
    end

    # We have exhausted the iterator.
    current_delay_secs = time() - start_time
    throw_on_timeout && error("Timed out after $iter iterations, $current_delay_secs seconds in `_poll_with_specified_overhead`.")

    return nothing
end


# Returns a `Dict` constructed from the given pairs, filtering out pairs where
# the value is `nothing`.
function _filter(pairs::Pair...)
    d = Dict((k => v) for (k, v) in pairs if !isnothing(v))
    return length(d) > 0 ? d : nothing
end

# Returns a URL constructed from context settings and the given path.
function _mkurl(ctx::Context, path)
    return "$(ctx.scheme)://$(ctx.host):$(ctx.port)$path"
end

function _print_request(method, path, query, body)
    println("$method $path")
    !isnothing(query) && for (k, v) in query
        println("$k: $v")
    end
    !isnothing(body) && println(String(body))
end

function _request(ctx::Context, method, path; query = nothing, body = UInt8[], kw...)
    # _print_request(method, path, query, body);
    try
        rsp = @mock request(ctx, method, _mkurl(ctx, path); query = query, body = body, kw...)
        if length(rsp.body) == 0
            return Dict()
        else
            return JSON3.read(rsp.body)
        end
    catch e
        if e isa HTTP.ExceptionRequest.StatusError
            throw(HTTPError(e.status, String(e.response.body)))
        else
            rethrow()
        end
    end
end

_delete(ctx::Context, path; body = nothing, kw...) =
    _request(ctx, "DELETE", path; body = body, kw...)

_get(ctx::Context, path; query = nothing, kw...) =
    _request(ctx, "GET", path; query = query, kw...)

_patch(ctx::Context, path; body = nothing, kw...) =
    _request(ctx, "PATCH", path; body = body, kw...)

_post(ctx::Context, path; body = nothing, kw...) =
    _request(ctx, "POST", path; body = body, kw...)

_put(ctx::Context, path; body = nothing, kw...) =
    _request(ctx, "PUT", path; body = body, kw...)

function create_engine(ctx::Context, engine::AbstractString; size = nothing, kw...)
    isnothing(size) && (size = "XS")
    data = Dict("region" => ctx.region, "name" => engine, "size" => size)
    return _put(ctx, PATH_ENGINE; body = JSON3.write(data), kw...)
end

function suspend_engine(ctx::Context, engine::AbstractString; kw...)
    payload=Dict("suspend" => true)
    return _patch(ctx, "$PATH_ENGINE/$engine"; body=JSON3.write(payload), kw...)
end

function resume_engine(ctx::Context, engine::AbstractString; kw...)
    payload=Dict("suspend" => false)
    return _patch(ctx, "$PATH_ENGINE/$engine"; body=JSON3.write(payload), kw...)
end

function create_oauth_client(ctx::Context, name::AbstractString, permissions; kv...)
    isnothing(permissions) && (permissions = [])
    data = Dict("name" => name, "permissions" => permissions)
    return _post(ctx, PATH_OAUTH_CLIENTS; body = JSON3.write(data), kv...)
end

function create_user(ctx::Context, email::AbstractString, roles = nothing; kw...)
    isnothing(roles) && (roles = [])
    data = Dict("email" => email, "roles" => roles)
    return _post(ctx, PATH_USERS; body = JSON3.write(data), kw...)
end

function delete_database(ctx::Context, database::AbstractString; kw...)
    data = Dict("name" => database)
    return _delete(ctx, PATH_DATABASE; body = JSON3.write(data), kw...)
end

function delete_engine(ctx::Context, engine::AbstractString; kw...)
    data = Dict("name" => engine)
    return _delete(ctx, PATH_ENGINE; body = JSON3.write(data), kw...)
end

# escape rel special string
_escape_string_for_rel(str) = replace(repr(str), '%' => "\\%")

function delete_oauth_client(ctx::Context, id::AbstractString; kw...)
    return _delete(ctx, joinpath(PATH_OAUTH_CLIENTS, id); kw...)
end

function delete_user(ctx::Context, userid::AbstractString; kw...)
    return _delete(ctx, joinpath(PATH_USERS, userid); kw...)
end

function disable_user(ctx::Context, userid::AbstractString; kw...)
    return update_user(ctx, userid; status = "INACTIVE", kw...)
end

function enable_user(ctx::Context, userid::AbstractString; kw...)
    return update_user(ctx, userid; status = "ACTIVE", kw...)
end

function get_engine(ctx::Context, engine::AbstractString; kw...)
    query = Dict("name" => engine, "deleted_on" => "")
    rsp = _get(ctx, PATH_ENGINE; query = query, kw...).computes
    length(rsp) == 0 && throw(HTTPError(404))
    return rsp[1]
end

function get_database(ctx::Context, database::AbstractString; kw...)
    query = Dict("name" => database)
    rsp = _get(ctx, PATH_DATABASE; query = query, kw...).databases
    length(rsp) == 0 && throw(HTTPError(404))
    return rsp[1]
end

function get_oauth_client(ctx::Context, id::AbstractString; kw...)
    return _get(ctx, joinpath(PATH_OAUTH_CLIENTS, id); kw...).client
end

# Returns the user with the given email.
function find_user(ctx::Context, email::AbstractString; kw...)
    rsp = list_users(ctx)
    for item in rsp
        item["email"] == email && return item
    end
    return nothing
end

function get_user(ctx::Context, userid::AbstractString; kw...)
    return _get(ctx, joinpath(PATH_USERS, userid); kw...).user
end

function list_databases(ctx::Context; state = nothing, kw...)
    query = _filter("state" => state)
    return _get(ctx, PATH_DATABASE; query = query, kw...).databases
end

function list_engines(ctx::Context; state = nothing, kw...)
    query = _filter("state" => state)
    return _get(ctx, PATH_ENGINE; query = query, kw...).computes
end

function list_oauth_clients(ctx::Context; kw...)
    return _get(ctx, PATH_OAUTH_CLIENTS; kw...).clients
end

function list_users(ctx::Context; kw...)
    return _get(ctx, PATH_USERS; kw...).users
end

function update_user(ctx::Context, userid::AbstractString; status = nothing, roles = nothing, kw...)
    data = _filter("status" => status, "roles" => roles)
    return _patch(ctx, joinpath(PATH_USERS, userid); body = JSON3.write(data), kw...)
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

    Transaction(region, database, engine, mode; source = nothing, readonly = false) =
        new(region, database, engine, mode, false, false, readonly, source, 0)
end

# Returns the serialized request body for the given transaction.
function body(tx::Transaction, actions...)::String
    data = _filter(
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
    return _filter(
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
        item = Dict(
            "name" => "action$i",
            "type" => "LabeledAction",
            "action" => action)
        push!(result, item)
    end
    return result
end

function _make_list_edb_action()
    return Dict("type" => "ListEdbAction")
end

function _make_query_action(source, ::Nothing)
    return Dict(
        "type" => "QueryAction",
        "source" => _make_query_source("query", source),
        "persist" => [], # todo: remove
        "inputs" => [],
        "outputs" => [])
end

function _make_query_action(source, inputs::Dict)
    return Dict(
        "type" => "QueryAction",
        "source" => _make_query_source("query", source),
        "persist" => [], # todo: remove
        "inputs" => [_make_query_action_input(k, v) for (k, v) in inputs],
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

"""
    create_database(ctx, name::String[, source::String])

Create a database with the specified `name`, optionally cloning from an existing `source`.
NOTE: It is an error (`HTTPError(400)`) to create a database that already exists. To
overwrite a database, you must first `delete_database(ctx, name)`, then create it.
"""
function create_database(
    ctx::Context,
    database::AbstractString,
    # The `engine` argument is no longer needed. This will be removed in a future release.
    engine::AbstractString = "";
    source = nothing,
    # The `overwrite` argument is no longer supported. Will be removed in a future release.
    overwrite = false,
    kw...
)
    ### Deprecation support: remove these parameters and warnings in a future release. ###
    if !isempty(engine)
        @warn "DEPRECATED: Passing an `engine` is no longer required for creating a" *
            " database. This will be removed in a future release. Please update your call" *
            " `create_database(ctx, name)`."
    end
    if overwrite == true
        @warn "DEPRECATED: The `overwrite` option is no longer supported for creating a" *
            " database. This will be removed in a future release. Please delete an" *
            " existing database before attempting to create it."
        @assert engine !== "" "`overwrite` is not supported in the new engineless API."
    end
    if !isempty(engine) || overwrite == true
        # If they were calling via the old API, continue to call the old method, to prevent
        # a breaking change in the return value format.
        return _create_database_v1(ctx, database, engine; source, overwrite, kw...)
    end
    ### End deprecation support ##########################################################
    data = Dict("name" => database)
    if source !== nothing
        data["source_name"] = source
    end
    return _put(ctx, PATH_DATABASE; body = JSON3.write(data), kw...)
end

# This function only exists to support the old, deprecated `overwrite=true` mode.
# We can delete it once we remove the deprecated `overwrite` option, above.
function _create_database_v1(ctx::Context, database::AbstractString, engine::AbstractString; source = nothing, overwrite = false, kw...)
    mode = _create_mode(source, overwrite)
    tx = Transaction(ctx.region, database, engine, mode; source = source)
    return _post(ctx, PATH_TRANSACTION; query = query(tx), body = body(tx), kw...)
end

# Execute the given query string, using any given optioanl query inputs.
# todo: consider create_transaction
# todo: consider create_transaction to better align with future transaction
#   resource model
function exec_v1(ctx::Context, database::AbstractString, engine::AbstractString, source; inputs = nothing, readonly = false, kw...)
    source isa IO && (source = read(source, String))
    tx = Transaction(ctx.region, database, engine, "OPEN"; readonly = readonly)
    data = body(tx, _make_query_action(source, inputs))
    return _post(ctx, PATH_TRANSACTION; query = query(tx), body = data, kw...)
end
# todo: when we have async transactions, add a variation that dispatches and
#   waits .. consider creating two entry points for readonly and readwrite.

"""
    exec(ctx, "database", "engine", "query source"; kwargs...)

Synchronously execute a provided query string in the supplied database. This function
creates a Transaction using the supplied engine, and then polls the Transaction until it has
completed, and returns a Dict holding the Transaction resource and its results and metadata.

## Keyword Arguments:
- `readonly = false`: If true, this is a "read-only query", and the effects of this
   transaction will not be committed to the database.
- `inputs`: Optional dictionary of input pairs, mapping a relation name to a value.
   (Deprecated - the format of the inputs Dict will change in upcoming releases.)

# Examples:
```julia
julia> exec(ctx, "my_database", "my_engine", "2 + 2")
Dict{String, Any} with 4 entries:
  "metadata"    => Union{}[]
  "problems"    => Union{}[]
  "results"     => Pair{String, Arrow.Table}["/:output/Int64"=>Arrow.Table with 1 r…
  "transaction" => {…

julia> exec(ctx, "my_database", "my_engine", \"""
           def insert:my_relation = 1, 2, 3
           \""",
           readonly = false,
       )
Dict{String, Any} with 4 entries:
  "metadata"    => Union{}[]
  "problems"    => Union{}[]
  "results"     => Any[]
  "transaction" => {…
```
"""
function exec(ctx::Context, database::AbstractString, engine::AbstractString, source; inputs = nothing, readonly = false, kw...)
    # Record the initial start time so that we include the time to create the transaction
    # in our exponential backoff in `wait_until_done()`.
    start_time = time()
    # Create an Async transaction:
    transactionResponse = exec_async(ctx, database, engine, source; inputs=inputs, readonly=readonly, kw...)
    if transactionResponse.results !== nothing
        return transactionResponse
    end
    # Poll until the transaction is done, and return the results.
    return wait_until_done(ctx, transactionResponse; start_time = start_time)
end

function exec_async(ctx::Context, database::AbstractString, engine::AbstractString, source; inputs = nothing, readonly = false, kw...)
    source isa IO && (source = read(source, String))
    tx_body = Dict(
        "dbname" => database,
        "engine_name" => engine,
        "query" => source,
        #"nowait_durable" => self.nowait_durable, # TODO: currently unsupported
        "readonly" => readonly,
        # "sync_mode" => "async"
    )
    if inputs !== nothing
        tx_body["v1_inputs"] = [_make_query_action_input(k, v) for (k, v) in inputs]
    end
    body = JSON3.write(tx_body)
    path = _mkurl(ctx, PATH_ASYNC_TRANSACTIONS)
    headers = _ensure_proto_accept_header(get(kw, :headers, []))
    rsp = @mock request(ctx, "POST", path; body = body, kw...)
    return _parse_response(rsp)
end

# We **only** support ProtoBuf metadata, so we overwrite the `Accept` header.
_ensure_proto_accept_header(headers) =
    collect(merge(Dict(headers), Dict("Accept" => "application/x-protobuf")))

function _parse_response(rsp)
    content_type = HTTP.header(rsp, "Content-Type")
    if lowercase(content_type) == "application/json"
        content = HTTP.body(rsp)
        # async mode
        txn = JSON3.read(content)
        return TransactionResponse(txn, nothing, nothing, nothing)
    elseif occursin("multipart/form-data", lowercase(content_type))
        # sync mode
        return _parse_multipart_fastpath_sync_response(rsp)
    else
        error("Unknown response content-type, for response:\n$(rsp)")
    end
end

function get_transaction(ctx::Context, id::AbstractString; kw...)
    path = PATH_ASYNC_TRANSACTIONS * "/$id"
    rsp = _get(ctx, path; kw...)
    return rsp.transaction
end

function get_transaction_events(ctx::Context, txn_id::AbstractString; kw...)
    # TODO: support a cancellation token
    path = PATH_ASYNC_TRANSACTIONS * "/$txn_id/events"
    continuation_token = "0"
    events = Channel()
    @spawn begin
        try
            while continuation_token != ""
                @info "requesting events" continuation_token txn_id
                rsp = _get(ctx, path * "/profiler&continuation_token=$(continuation_token)"; kw...)
                for event in rsp.events
                    put!(events, event)
                end
                continuation_token = rsp.continuation_token
                if !rsp.more_available
                    sleep(2)
                end
            end
            close(events)
        catch e
            close(chan, e)
        end
    end
    return events
end

function transaction_is_done(txn)
    if haskey(txn, "transaction")
        txn = txn["transaction"]
    end
    return txn["state"] ∈ ("COMPLETED", "ABORTED")
end

function transaction_id(txn)
    if haskey(txn, "transaction")
        txn = txn["transaction"]
    end
    return txn["id"]
end

function get_transaction_metadata(ctx::Context, id::AbstractString; kw...)
    path = PATH_ASYNC_TRANSACTIONS * "/$id/metadata"
    path = _mkurl(ctx, path)
    headers = _ensure_proto_accept_header(get(kw, :headers, []))
    rsp = @mock request(ctx, "GET", path; kw..., headers)
    d = ProtoBuf.ProtoDecoder(IOBuffer(rsp.body));
    metadata = ProtoBuf.decode(d, protocol.MetadataInfo)
    return metadata
end

function get_transaction_problems(ctx::Context, id::AbstractString; kw...)
    path = PATH_ASYNC_TRANSACTIONS * "/$id/problems"
    rsp = _get(ctx, path; kw...)
    return rsp
end

function get_transaction_results(ctx::Context, id::AbstractString; kw...)
    path = PATH_ASYNC_TRANSACTIONS * "/$id/results"
    path = _mkurl(ctx, path)
    rsp = @mock request(ctx, "GET", path; kw...)
    content_type = HTTP.header(rsp, "Content-Type")
    if !occursin("multipart/form-data", content_type)
        throw(HTTPError(400, "Unexpected response content-type for rsp:\n$rsp"))
    end
    return _parse_multipart_results_response(rsp)
end

function cancel_transaction(ctx::Context, id::AbstractString; kw...)
    path = PATH_ASYNC_TRANSACTIONS * "/$id/cancel"
    return _post(ctx, path)
end

function _parse_multipart_fastpath_sync_response(msg)
    # TODO: in-place conversion to Arrow without copying the bytes.
    #   ... HTTP.parse_multipart_form() copies the bytes into IOBuffers.
    parts = _parse_multipart_form(msg)
    @assert parts[1].name == "transaction"

    transaction = JSON3.read(parts[1])

    metadata_idx = findfirst(p->p.name == "metadata.proto", parts)
    d = ProtoBuf.ProtoDecoder(parts[metadata_idx].data);
    metadata = ProtoBuf.decode(d, protocol.MetadataInfo)

    problems_idx = findfirst(p->p.name == "problems", parts)
    problems = JSON3.read(parts[problems_idx])

    results = _extract_multipart_results_response(parts)

    return TransactionResponse(transaction, metadata, problems, results)
end

function _parse_multipart_results_response(msg)
    # TODO: in-place conversion to Arrow without copying the bytes.
    #   ... HTTP.parse_multipart_form() copies the bytes into IOBuffers.
    parts = _parse_multipart_form(msg)
    return _extract_multipart_results_response(parts)
end
function _extract_multipart_results_response(parts)
    return [
        (part.name => Arrow.Table(part.data)) for part in parts
            if part.contenttype == ARROW_CONTENT_TYPE
    ]
end


function list_edbs(ctx::Context, database::AbstractString, engine::AbstractString; kw...)
    tx = Transaction(ctx.region, database, engine, "OPEN"; readonly = true)
    data = body(tx, _make_list_edb_action())
    rsp = _post(ctx, PATH_TRANSACTION; query = query(tx), body = data, kw...)
    length(rsp.actions) == 0 && return []
    return rsp.actions[1].result.rels
end

function _gen_literal(value)
    return "$value"
end

function _gen_literal(value::Dict)
    items = ["$(_gen_literal(v)),$(_gen_literal(k))" for (k, v) in value]
    return "{" + join(items, ";") + "}"
end

function _gen_literal(value::String)
    s = replace(value, "'" => "\\'")
    return "'$s'"
end

_gen_literal(value::Symbol) = ":$value"

function _gen_literal(value::Vector)
    items = [_gen_literal(item) for item in value]
    return "{" + join(items, ",") + "}"
end

function _gen_config(name, value)
    isnothing(value) && return ""
    return "def config:syntax:$name=$(_gen_literal(value))"
end

function _gen_config(syntax::Dict)
    length(syntax) == 0 && return ""
    items = [_gen_config(k, v) for (k, v) in syntax if !isnothing(v)]
    return join(items, '\n') * '\n'
end

_gen_config(::Nothing) = ""

_read_data(d::AbstractString) = d
_read_data(d::IO) = read(d, String)

# todo: need to uniquify config and data so it doesn't conflict with those
#   names if they already exist in the database.
# todo: add support for config:path
function load_csv(
    ctx::Context, database, engine, relation, data;
    delim = nothing, header = nothing, header_row = nothing,
    escapechar = nothing, quotechar = nothing, kw...
)
    inputs = Dict("data" => _read_data(data))
    syntax = Dict{String,String}()
    syntax = _filter(
        "delim" => delim,
        "header" => header,
        "header_row" => header_row,
        "escapechar" => escapechar,
        "quotechar" => quotechar)
    source = _gen_config(syntax)
    source *= """def config:data = data
                 def insert:$relation = load_csv[config]"""
    return exec(ctx, database, engine, source; inputs = inputs, readonly = false, kw...)
end

# todo: need to uniquify config and data
# todo: add support for config:path
# todo: data should be string or io
function load_json(ctx::Context, database::AbstractString, engine::AbstractString, relation::AbstractString, data; kw...)
    inputs = Dict("data" => _read_data(data))
    source = """def config:data = data\n
                def insert:$relation = load_json[config]"""
    return exec(ctx, database, engine, source; inputs = inputs, readonly = false, kw...)
end

function load_models(ctx::Context, database::AbstractString, engine::AbstractString, models::Dict; kw...)
    queries = []
    queries_inputs = Dict()
    rand_uint = rand(UInt64)

    index = 0
    for (name, value) in models
        input_name = string("input_", rand_uint, "_", index)
        push!(queries, """
            def delete:rel:catalog:model["$name"] = rel:catalog:model["$name"]
            def insert:rel:catalog:model["$name"] = $input_name
        """)

        queries_inputs[input_name] = value
        index+=1
    end

    return exec(ctx, database, engine, join(queries, "\n"); inputs = queries_inputs, readonly = false, kw...)
end

function load_models_async(ctx::Context, database::AbstractString, engine::AbstractString, models::Dict; kw...)
    queries = []
    queries_inputs = Dict()
    rand_uint = rand(UInt64)

    index = 0
    for (name, value) in models
        input_name = string("input_", rand_uint, "_", index)
        push!(queries, """
            def delete:rel:catalog:model["$name"] = rel:catalog:model["$name"]
            def insert:rel:catalog:model["$name"] = $input_name
        """)

        queries_inputs[input_name] = value
        index+=1
    end

    return exec_async(ctx, database, engine, join(queries, "\n"); inputs = queries_inputs, readonly = false, kw...)
end

function list_models(ctx::Context, database::AbstractString, engine::AbstractString; kw...)
    out_name = "model$(rand(UInt64))"
    query = """ def output:$out_name[name] = rel:catalog:model(name, _) """
    resp = exec(ctx, database, engine, query)
    for result in resp.results
        if occursin("/:output/:$out_name", result.first)
            return [name for name in result.second.v1]
        end
    end
end

function get_model(ctx::Context, database::AbstractString, engine::AbstractString, name::AbstractString; kw...)
    out_name = "model$(rand(UInt64))"
    query = """def output:$out_name = rel:catalog:model[$(_escape_string_for_rel(name))]"""
    resp = exec(ctx, database, engine, query)
    for result in resp.results
        if occursin("/:output/:$out_name", result.first)
            return first(result.second.v1)
        end
    end
    throw(HTTPError(404))
end

function delete_models(ctx::Context, database::AbstractString, engine::AbstractString, models::Vector{String}; kw...)
    queries = ["""
            def delete:rel:catalog:model[$(_escape_string_for_rel(model))] = rel:catalog:model[$(_escape_string_for_rel(model))]
        """ for model in models]
    return exec(ctx, database, engine, join(queries, "\n"); readonly=false, kw...)
end

function delete_models_async(ctx::Context, database::AbstractString, engine::AbstractString, model::AbstractString; kw...)
    queries = ["""
            def delete:rel:catalog:model[$(_escape_string_for_rel(model))] = rel:catalog:model[$(_escape_string_for_rel(model))
        """ for model in models]
    return exec_async(ctx, database, engine, join(queries, "\n"); readonly=false, kw...)
end

# --- utils -------------------------
# Patch for older versions of HTTP package that don't support parsing multipart responses:
if hasmethod(HTTP.MultiPartParsing.parse_multipart_form, (HTTP.Response,))
    # Available as of HTTP v0.9.18:
    _parse_multipart_form = HTTP.MultiPartParsing.parse_multipart_form
else
    # This function is copied directly from this PR: https://github.com/JuliaWeb/HTTP.jl/pull/817
    function _parse_multipart_form(msg::HTTP.Message)
        # parse boundary from Content-Type
        m = match(r"multipart/form-data; boundary=(.*)$", msg["Content-Type"])
        m === nothing && return nothing

        boundary_delimiter = m[1]

        # [RFC2046 5.1.1](https://tools.ietf.org/html/rfc2046#section-5.1.1)
        length(boundary_delimiter) > 70 && error("boundary delimiter must not be greater than 70 characters")

        return HTTP.MultiPartParsing.parse_multipart_body(HTTP.payload(msg), boundary_delimiter)
    end
end
# -----------------------------------
