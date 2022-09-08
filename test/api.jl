using RAI
using Test
import HTTP, Arrow
using JSON3
using Mocking
using Dates
using RAI.protocol
using RAI: _poll_with_specified_overhead

using RAI: TransactionResponse

import ProtoBuf

Mocking.activate()

# -----------------------------------
# v2 transactions

make_patch(response) = @patch RAI.request(ctx::Context, args...; kw...) = response

function make_proto_metadata()
    # Corresponding to the following JSON metadata:
    # [{"relationId":"/:output/Int64","types":[":output","Int64"]}]
    return MetadataInfo(
        RelationMetadata[RelationMetadata(
            RelationId(RelType[
                RelType{ConstantType,Nothing}(
                    Kind.CONSTANT_TYPE,
                    PrimitiveType.UNSPECIFIED_TYPE,
                    nothing,
                    ConstantType(
                        RelType{Nothing,Nothing}(
                            Kind.PRIMITIVE_TYPE,
                            PrimitiveType.STRING,
                            nothing,
                            nothing,
                        ),
                        RelTuple(
                            PrimitiveValue[PrimitiveValue(
                                PrimitiveType.STRING,
                                ProtoBuf.OneOf{Vector{UInt8}}(
                                    :string_val,
                                    UInt8[0x6f, 0x75, 0x74, 0x70, 0x75, 0x74], # output
                                ),
                            )],
                        ),
                    ),
                ),
                RelType{Nothing,Nothing}(
                    Kind.PRIMITIVE_TYPE,
                    PrimitiveType.INT_64,
                    nothing,
                    nothing,
                ),
            ],),
            "0.arrow",
        )],
    )
end
function make_proto_metadata_string()
    metadata = make_proto_metadata()
    io = IOBuffer()
    e = ProtoBuf.ProtoEncoder(io)
    ProtoBuf.encode(e, metadata)
    return String(take!(io))
end

const v2_async_response = HTTP.Response(
    200,
    ["Content-Type" => "application/json"],
    body="""{"id":"1fc9001b-1b88-8685-452e-c01bc6812429","state":"CREATED"}""",
)

const v2_get_results_response() = join(
    [
        "--8a89e52be8efe57f0b68ea75388314a3",
        "Content-Disposition: form-data; name=\"/:output/Int64\"; filename=\"/:output/Int64\"",
        "Content-Type: application/vnd.apache.arrow.stream",
        "",
        "\xff\xff\xff\xffx\0\0\0\x10\0\0\0\0\0\n\0\f\0\n\0\b\0\x04\0\n\0\0\0\x10\0\0\0\x01\0\x04\0\b\0\b\0\0\0\x04\0\b\0\0\0\x04\0\0\0\x01\0\0\0\x14\0\0\0\x10\0\x14\0\x10\0\0\0\x0e\0\b\0\0\0\x04\0\x10\0\0\0\x10\0\0\0\x18\0\0\0\0\0\x02\0\x1c\0\0\0\0\0\0\0\b\0\f\0\b\0\a\0\b\0\0\0\0\0\0\x01@\0\0\0\x02\0\0\0v1\0\0\xff\xff\xff\xff\x88\0\0\0\x14\0\0\0\0\0\0\0\f\0\x16\0\x14\0\x12\0\f\0\x04\0\f\0\0\0\b\0\0\0\0\0\0\0\x14\0\0\0\0\0\x03\0\x04\0\n\0\x18\0\f\0\b\0\x04\0\n\0\0\0\x14\0\0\08\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\b\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x04\0\0\0\0\0\0\0\xff\xff\xff\xff\0\0\0\0",
    ], "\r\n")

const v2_get_relation_count_response() = join([
        "--8a89e52be8efe57f0b68ea75388314a3",
        "Content-Disposition: form-data; name=\"relation-count\"; filename=\"\"",
        "Content-Type: application/json",
        "",
        "{\"relation_count\":1}",
    ], "\r\n")

const v2_get_transaction_results_response = HTTP.Response(200, [
        "Content-Type" => "Content-Type: multipart/form-data; boundary=8a89e52be8efe57f0b68ea75388314a3",
        "Transfer-Encoding" => "chunked",
    ],
    body = join([
    "",
    v2_get_results_response(),
    v2_get_relation_count_response(),
    "--8a89e52be8efe57f0b68ea75388314a3--",
    "",
], "\r\n"))

const v2_get_transaction_json_completed = """{"id":"a3e3bc91-0a98-50ba-733c-0987e160eb7d","results_format_version":"2.0.1","state":"COMPLETED"}"""
const v2_get_transaction_response_completed() = HTTP.Response(200,
    """
    {"transaction": $(v2_get_transaction_json_completed)}
    """)

const v2_fastpath_response = HTTP.Response(200, [
        "Content-Type" => "Content-Type: multipart/form-data; boundary=8a89e52be8efe57f0b68ea75388314a3",
        "Transfer-Encoding" => "chunked",
    ],
    body=join(
        [
            "",
            "--8a89e52be8efe57f0b68ea75388314a3",
            "Content-Disposition: form-data; name=\"transaction\"; filename=\"\"",
            "Content-Type: application/json",
            "",
            v2_get_transaction_json_completed,
            "--8a89e52be8efe57f0b68ea75388314a3",
            "Content-Disposition: form-data; name=\"metadata.proto\"; filename=\"\"",
            "Content-Type: application/x-protobuf",
            "",
            make_proto_metadata_string(),
            "--8a89e52be8efe57f0b68ea75388314a3",
            "Content-Disposition: form-data; name=\"problems\"; filename=\"\"",
            "Content-Type: application/json",
            "",
            """[]""",
            v2_get_results_response(),
            "--8a89e52be8efe57f0b68ea75388314a3--",
            "",
        ],
        "\r\n",
    ),
)

function make_arrow_table(vals)
    io = IOBuffer()
    Arrow.write(io, (v1=vals,))
    seekstart(io)
    return Arrow.Table(io)
end

@testset "_poll_with_specified_overhead" begin
    @test isnothing(_poll_with_specified_overhead(() -> true; overhead_rate = 0.01))
    @test isnothing(_poll_with_specified_overhead(() -> false; overhead_rate = 0.01, n=0))
    @test isnothing(_poll_with_specified_overhead(() -> false; overhead_rate = 0.01, n=1))
    @test isnothing(_poll_with_specified_overhead(() -> true; overhead_rate = 0.01, n=1, throw_on_timeout=true))
    @test_throws ErrorException _poll_with_specified_overhead(() -> false; overhead_rate = 0.01, n=1, throw_on_timeout=true)
end

@testset "exec_async" begin
    ctx = Context("region", "scheme", "host", "2342", nothing, "audience")

    @testset "async response" begin
        patch = make_patch(v2_async_response)

        apply(patch) do
            rsp = RAI.exec_async(ctx, "engine", "database", "2+2")
            @test rsp.transaction == JSON3.read("""{"id":"1fc9001b-1b88-8685-452e-c01bc6812429","state":"CREATED"}""")
        end
    end

    @testset "sync response" begin
        patch = make_patch(v2_fastpath_response)

        apply(patch) do
            rsp = RAI.exec_async(ctx, "engine", "database", "2+2")
            @test rsp.transaction == JSON3.read("""{
                    "id": "a3e3bc91-0a98-50ba-733c-0987e160eb7d",
                    "results_format_version": "2.0.1",
                    "state": "COMPLETED"
                }""")
            # We unfortunately cannot test equality directly!
            @test string(rsp.metadata) == string(make_proto_metadata())
            @test rsp.problems == Union{}[]

            # Test for the expected arrow data:
            expected_data = make_arrow_table([4])
            # Arrow.Tables can't be compared via == (https://github.com/apache/arrow-julia/issues/310)
            @test length(rsp.results) == 1
            @test rsp.results[1][1] == "/:output/Int64"
            @test collect(rsp.results[1][2]) == collect(expected_data)
        end
    end

    @testset "get_transaction_results" begin
        patch = make_patch(v2_get_transaction_results_response)

        apply(patch) do
            rsp = RAI.get_transaction_results(ctx, "fake-txn-id")
            @test !isempty(rsp)
            @test rsp[1][2] isa Arrow.Table
        end
    end
end

@testset "show_result" begin
    ctx = Context("region", "scheme", "host", "2342", nothing, "audience")
    patch = make_patch(v2_fastpath_response)

    apply(patch) do
        rsp = RAI.exec_async(ctx, "engine", "database", "2+2")
        @test rsp isa TransactionResponse

        io = IOBuffer()
        show_result(io, rsp)
        @test String(take!(io)) === """[:output, Int64]
         (4,)
        """
    end
end

struct NetworkError code::Int end
make_fail_second_time_patch(args...) =
    make_fail_nth_time_patch(2, args...)
function make_fail_nth_time_patch(n, first_response, exception)
    request_idx = 0
    return (ctx::Context, args...; kw...) -> begin
        request_idx += 1
        if request_idx == n
            throw(exception)
        else
            return first_response
        end
    end
end

@testset "error handling" begin
    ctx = Context("region", "scheme", "host", "2342", nothing, "audience")
    patch = @patch RAI.request(ctx::Context, args...; kw...) = throw(NetworkError(404))

    apply(patch) do
        @test_throws NetworkError(404) RAI.exec(ctx, "engine", "db", "2+2")
    end

    @testset "test that txn ID is logged for txn errors while polling" begin
        # Test for an error thrown _after_ the transaction is created, before it completes.
        sync_error_patch = Mocking.Patch(RAI.request,
            make_fail_second_time_patch(v2_async_response, NetworkError(500)))

        # See https://discourse.julialang.org/t/how-to-test-the-value-of-a-variable-from-info-log/37380/3
        # for an explanation of this logs-testing pattern.
        logs, _ = Test.collect_test_logs() do
            apply(sync_error_patch) do
                @test_throws NetworkError(500) RAI.exec(ctx, "engine", "db", "2+2")
            end
        end
        sym, val = collect(pairs(logs[1].kwargs))[1]
        @test sym ≡ :transaction_id
        @test val == "1fc9001b-1b88-8685-452e-c01bc6812429"
    end

    @testset "Handle Aborted Txns with no metadata" begin
        # Test for the _specific case_ of a 404 from the RelationalAI service, once the txn
        # completes.

        # Attempt to wait until a txn is done. This will attempt to fetch the metadata &
        # results once it's finished.
        metadata_404_patch = Mocking.Patch(RAI.request,
            make_fail_second_time_patch(
                # get_transaction() returns a completed Transaction resource
                v2_get_transaction_response_completed(),
                # So then we attempt to fetch the metadata or results or problems, and error
                RAI.HTTPError(404)
            )
        )

        apply(metadata_404_patch) do
            RAI.wait_until_done(ctx, "<txn-id>", start_time_ns=0)
        end
    end

end

@testset "exec with fast-path response only makes one request" begin
    # Throw an error if the SDK attempts to make two requests to RAI API:
    only_1_request_patch = Mocking.Patch(RAI.request,
        make_fail_second_time_patch(v2_fastpath_response, NetworkError(500)))

    ctx = Context("region", "scheme", "host", "2342", nothing, "audience")
    apply(only_1_request_patch) do
        @test RAI.exec(ctx, "engine", "db", "2+2") isa RAI.TransactionResponse
    end
end

@testset "hide client secrets in repl" begin
    access_token = AccessToken("abc_token", "run:transaction", 3600, DateTime("2022-08-12T17:49:51.365"))
    creds = ClientCredentials("client_id", "xyz_client_secret", "https://login.relationalai.com/oauth/token")
    creds.access_token = access_token

    io = IOBuffer()
    show(io, creds)
    @test String(take!(io)) === "(client_id, xyz..., (abc..., run:transaction, 3600, 2022-08-12T17:49:51.365), https://login.relationalai.com/oauth/token)"
end
