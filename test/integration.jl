using Test
using RAI
using RAI: transaction_id, _poll_with_specified_overhead

import UUIDs

# -----------------------------------
# context & setup

# These are fairly unaggressive testing parameters, to try to not be too expensive on the
# cloud. Time out after ten minutes of silence.
const POLLING_KWARGS = (; overhead_rate = 0.20, timeout_secs = 10*60, throw_on_timeout = true)

function test_context(profile_name = nothing)
    # If the ENV isn't configured for testing (local development), try using the local
    # Config file!
    if !haskey(ENV, "CLIENT_ID")
        if isfile(homedir()*"/.rai/config")
            if profile_name !== nothing
                cfg = load_config(; profile = profile_name)
            else
                cfg = load_config()
            end
            return Context(cfg)
        end
    end

    # Otherwise, we are testing using the secrets specified in ENV variables.

    @assert all(
        key -> haskey(ENV, key),
        ["CLIENT_ID", "CLIENT_SECRET", "CLIENT_CREDENTIALS_URL"],
    )

    client_id = ENV["CLIENT_ID"]
    client_secret = ENV["CLIENT_SECRET"]
    client_credentials_urls = ENV["CLIENT_CREDENTIALS_URL"]
    audience = get(ENV, "CLIENT_AUDIENCE", nothing)

    credentials = ClientCredentials(client_id, client_secret, client_credentials_urls)
    config = Config("us-east", "https", "azure.relationalai.com", "443", credentials, audience)

    return Context(config)
end

rnd_test_name() = "julia-sdk-" * string(UUIDs.uuid4())

# Creates an engine and executes `f` when the engine is ready. Deletes the engine when
# finished. An already existing engine can be supplied to improve local iteration times.
function with_engine(f, ctx; existing_engine=nothing)
    engine_name = rnd_test_name()
    if isnothing(existing_engine)
        start_time_ns = time_ns()
        create_engine(ctx, engine_name)
        _poll_with_specified_overhead(; POLLING_KWARGS..., start_time_ns) do
            get_engine(ctx, engine_name)[:state] == "PROVISIONED"
        end
    else
        engine_name = existing_engine
    end
    try
        f(engine_name)
    finally
        # Engines cannot be deleted if they are still provisioning. We have to at least wait
        # until they are ready.
        if isnothing(existing_engine)
            start_time_ns = time_ns() - 2e9  # assume we started 2 seconds ago
            _poll_with_specified_overhead(; POLLING_KWARGS..., start_time_ns) do
                get_engine(ctx, engine_name)[:state] == "PROVISIONED"
            end
            delete_engine(ctx, engine_name)
        end
    end
end

# Creates a database and executes `f` with the name of the created database.  Deletes the
# database when finished. An already existing database can be supplied to improve local
# iteration times.
function with_database(f, ctx, engine_name; existing_database=nothing)
    isnothing(existing_database) &&
        create_database(ctx, engine_name, engine_name; overwrite=true)
    try
        f(engine_name)
    finally
        isnothing(existing_database) && delete_database(ctx, engine_name)
    end
end

# If the env vars are not properly set this will fail!
const CTX = test_context()

# -----------------------------------
# engine
@testset "engine" begin end

# -----------------------------------
# database
@testset "database" begin
    dbname = rnd_test_name()
    rsp = create_database(CTX, dbname)
    @test rsp.database.name == dbname
    @test rsp.database.state == "CREATED"

    # TODO: https://github.com/RelationalAI/relationalai-infra/issues/2542
    # In order to clone from a database, you currently need to "touch" it, to materialize
    # it. Remove this once that is fixed.
    with_engine(CTX) do engine_name
        _ = exec(CTX, dbname, engine_name, "")
    end

    dbname_clone = "$dbname-clone"
    rsp = create_database(CTX, dbname_clone, source=dbname)
    @test rsp.database.name == dbname_clone
    @test rsp.database.state == "CREATED"

    # Already exists
    @test_throws RAI.HTTPError create_database(CTX, dbname_clone)
    @test_throws RAI.HTTPError create_database(CTX, dbname_clone, source=dbname)

    rsp = delete_database(CTX, dbname)
    @test rsp.name == dbname
    @test delete_database(CTX, dbname_clone).name == dbname_clone

    # Doesn't exists
    @test_throws RAI.HTTPError delete_database(CTX, dbname)
end

# -----------------------------------
# transactions

with_engine(CTX) do engine_name
    with_database(CTX, engine_name) do database_name

        # -----------------------------------
        # execution
        @testset "execution" begin
            @testset "exec" begin
                # Test the synchronous path. We expect a response that contains `metadata`,
                # `problems`, `results` and the `transaction` information.
                query_string = "x, x^2, x^3, x^4 from x in {1; 2; 3; 4; 5}"
                resp = exec(CTX, database_name, engine_name, query_string)

                # transaction
                @test resp.transaction[:state] == "COMPLETED"

                # metadata
                @test length(resp.metadata.relations) == 1
                # /ConstantType(Symbol, :output)/Int64/Int64/Int64/Int64
                @test length(resp.metadata.relations[1].relation_id.arguments) == 5
                for rel_type in resp.metadata.relations[1].relation_id.arguments[2:end]
                    @test rel_type == RAI.protocol.RelType(
                        RAI.protocol.Kind.PRIMITIVE_TYPE,
                        RAI.protocol.PrimitiveType.INT_64,
                        nothing,
                        nothing,
                    )
                end

                # problems
                @test length(resp.problems) == 0

                # results
                @test length(resp.results) == 1
                @test collect(resp.results[1][2]) == [
                    [1, 2, 3, 4, 5],
                    [1, 4, 9, 16, 25],
                    [1, 8, 27, 64, 125],
                    [1, 16, 81, 256, 625],
                ]
            end

            @testset "exec_async" begin
                query_string = "x, x^2, x^3, x^4 from x in {1; 2; 3; 4; 5}"
                resp = exec_async(CTX, database_name, engine_name, query_string)
                txn = resp.transaction

                @test txn[:state] == "COMPLETED"
                txn_id = transaction_id(txn)

                # Poll until the transaction completes.
                wait_until_done(CTX, txn_id)

                # transaction
                @test RAI.transaction_is_done(get_transaction(CTX, txn_id))

                # Test calling this after the transaction already _is_ done:
                wait_until_done(CTX, txn_id)
                # Test all the API variants:
                wait_until_done(CTX, txn_id)
                wait_until_done(CTX, txn)
                wait_until_done(CTX, resp)
                wait_until_done(CTX, get_transaction(CTX, txn_id))

                # metadata
                # TODO (dba): Test new ProtoBuf metadata.

                # problems
                @test length(get_transaction_problems(CTX, txn_id)) == 0

                # results
                results = get_transaction_results(CTX, txn_id)
                @test length(resp.results) == 1
                @test collect(resp.results[1][2]) == [
                    [1, 2, 3, 4, 5],
                    [1, 4, 9, 16, 25],
                    [1, 8, 27, 64, 125],
                    [1, 16, 81, 256, 625],
                ]
            end

            @testset "load_csv" begin
                csv_data =
                    "" *
                    "cocktail,quantity,price,date\n" *
                    "\"martini\",2,12.50,\"2020-01-01\"\n" *
                    "\"sazerac\",4,14.25,\"2020-02-02\"\n" *
                    "\"cosmopolitan\",4,11.00,\"2020-03-03\"\n" *
                    "\"bellini\",3,12.25,\"2020-04-04\"\n"

                csv_relation = "csv"

                resp = load_csv(
                    CTX,
                    database_name,
                    engine_name,
                    Symbol(csv_relation),
                    csv_data,
                )

                @test resp.transaction[:state] == "COMPLETED"

                results = Dict(
                    exec(CTX, database_name, engine_name, "def output = $csv_relation").results,
                )

                # `v2` contains the `String` columm, `v1` contains the FilePos column.
                @test collect(results["/:output/:quantity/FilePos/String"].v2) ==
                      ["2", "4", "4", "3"]
                @test collect(results["/:output/:date/FilePos/String"].v2) ==
                      ["2020-01-01", "2020-02-02", "2020-03-03", "2020-04-04"]
                @test collect(results["/:output/:price/FilePos/String"].v2) ==
                      ["12.50", "14.25", "11.00", "12.25"]
                @test collect(results["/:output/:cocktail/FilePos/String"].v2) ==
                      ["martini", "sazerac", "cosmopolitan", "bellini"]
            end

            @testset "load_json" begin end

            @testset "list_edb" begin end

            @testset "show_result" begin
                function show_result_str(rsp)
                    io = IOBuffer()
                    show_result(io, rsp)
                    return String(take!(io))
                end
                @testset "empty arrow file" begin
                    query_string = "def output = true"
                    resp = exec(CTX, database_name, engine_name, query_string)
                    @test show_result_str(resp) === """[:output]
                     ()
                    """
                end
                @testset "multiple physical relations" begin
                    query_string = ":a, 1;  :b, 2,3;  :b, 4,5"
                    resp = exec(CTX, database_name, engine_name, query_string)
                    @test show_result_str(resp) === """[:output, :a, Int64]
                     (1,)

                    [:output, :b, Int64, Int64]
                     (2, 3)
                     (4, 5)
                    """
                end
            end

        end

        # -----------------------------------
        # models
        @testset "models" begin end
    end
end

# -----------------------------------
# client
@testset "oauth" begin end

# -----------------------------------
# users
@testset "users" begin end
