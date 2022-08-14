using RAI
using Test

@testset "config creation" begin
    if (isfile(joinpath(homedir(),".rai","config")))
        conf = load_config()
        @test conf isa RAI.Config 
    end
end
