using RAI
using Test
import HTTP, Arrow
using Mocking
using RAI: _poll_until

Mocking.activate()

# -----------------------------------
# v2 transactions

make_patch(response) = @patch RAI.request(ctx::Context, args...; kw...) = response

const v2_async_response = HTTP.Response(200, [
        "Content-Type" => "application/json",
    ],
    body = """{"id":"1fc9001b-1b88-8685-452e-c01bc6812429","state":"CREATED"}""")

const v2_get_results_response() = join([
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

const v2_fastpath_response = HTTP.Response(200, [
        "Content-Type" => "Content-Type: multipart/form-data; boundary=8a89e52be8efe57f0b68ea75388314a3",
        "Transfer-Encoding" => "chunked",
    ],
    body = join([
    "",
    "--8a89e52be8efe57f0b68ea75388314a3",
    "Content-Disposition: form-data; name=\"transaction\"; filename=\"\"",
    "Content-Type: application/json",
    "",
    """{"id":"a3e3bc91-0a98-50ba-733c-0987e160eb7d","results_format_version":"2.0.1","state":"COMPLETED"}""",
    "--8a89e52be8efe57f0b68ea75388314a3",
    "Content-Disposition: form-data; name=\"metadata\"; filename=\"\"",
    "Content-Type: application/json",
    "",
    """[{"relationId":"/:output/Int64","types":[":output","Int64"]}]""",
    "--8a89e52be8efe57f0b68ea75388314a3",
    "Content-Disposition: form-data; name=\"problems\"; filename=\"\"",
    "Content-Type: application/json",
    "",
    """[]""",
    v2_get_results_response(),
    "--8a89e52be8efe57f0b68ea75388314a3--",
    "",
], "\r\n"))

function make_arrow_table(vals)
    io = IOBuffer()
    Arrow.write(io, (v1=vals,))
    seekstart(io)
    return Arrow.Table(io)
end

@testset "_poll_until" begin
    @test isnothing(_poll_until(() -> true))
    @test isnothing(_poll_until(() -> false; n=0))
    @test isnothing(_poll_until(() -> false; n=1))
    @test isnothing(_poll_until(() -> true; n=1, throw_on_max_n=true))
    @test_throws String _poll_until(() -> false; n=1, throw_on_max_n=true)
end

@testset "exec_async" begin
    ctx = Context("region", "scheme", "host", "2342", nothing)

    @testset "async response" begin
        patch = make_patch(v2_async_response)

        apply(patch) do
            rsp = RAI.exec_async(ctx, "engine", "database", "2+2")
            @test rsp == Dict(
                "transaction" => JSON3.read("""{"id":"1fc9001b-1b88-8685-452e-c01bc6812429","state":"CREATED"}""")
            )
        end
    end

    @testset "sync response" begin
        patch = make_patch(v2_fastpath_response)

        apply(patch) do
            rsp = RAI.exec_async(ctx, "engine", "database", "2+2")
            @test rsp["transaction"] == JSON3.read("""{
                    "id": "a3e3bc91-0a98-50ba-733c-0987e160eb7d",
                    "results_format_version": "2.0.1",
                    "state": "COMPLETED"
                }""")
            @test rsp["metadata"] == [JSON3.read("""{
                "relationId": "/:output/Int64",
                    "types": [
                                ":output",
                                "Int64"
                            ]
            }""")]
            @test rsp["problems"] == Union{}[]

            # Test for the expected arrow data:
            expected_data = make_arrow_table([4])
            # Arrow.Tables can't be compared via == (https://github.com/apache/arrow-julia/issues/310)
            @test length(rsp["results"]) == 1
            @test rsp["results"][1][1] == "/:output/Int64"
            @test collect(rsp["results"][1][2]) == collect(expected_data)
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
