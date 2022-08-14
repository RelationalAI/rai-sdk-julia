using RAI
using Test

@testset "config creation" begin
    conf = load_config()
    @test conf isa RAI.Config 
end
