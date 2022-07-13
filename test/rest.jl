using Test

using RAI
import HTTP

@testset "custom headers" begin
    ctx = Context("us-east", "https", "host", "2342", nothing)
    rsp = RAI.request(ctx, "GET", "https://www.example.com", headers = ["test" => "value"])
    @test rsp isa HTTP.Response
end
