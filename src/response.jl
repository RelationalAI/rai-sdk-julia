using Arrow

struct TransactionResponse
    transaction::JSON3.Object
    metadata::Union{JSON3.Array,Nothing}
    problems::Union{JSON3.Array,Nothing}
    results::Union{Vector{Pair{String, Arrow.Table}},Nothing}
end

