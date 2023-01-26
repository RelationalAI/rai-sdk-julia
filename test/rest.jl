using Test

using RAI
import HTTP

@testset "custom headers" begin
    ctx = Context("us-east", "https", "host", "2342", nothing, "audience")
    rsp = RAI.request(ctx, "GET", "https://www.example.com", headers = ["test" => "value"])
    @test rsp isa HTTP.Response
end

@testset "_ensure_headers" begin
    h1 = RAI._ensure_headers()
    ks = first.(h1)
    @test in("Accept", ks)
    @test in("Content-Type", ks)
    @test in("User-Agent", ks)
    # Do not mutate the given headers
    h2 = RAI._ensure_headers(h1)
    @test h1 !== h2
end
