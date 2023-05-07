function collectresult(result)
    keys = result.metadata.relations
    keys = [sprint.(RAI.show_rel_type, key.relation_id.arguments) for key in keys]
    vals = [isempty(v) ? [()] : collect(zip(v...)) for (k, v) in result.results]
    return Dict(zip(keys, vals))
end

replshow(io, result::RAI.TransactionResponse) = replshow(io, collectresult(result))

function replshow(io, result)
    first = true
    isempty(result) && println("{}") # false / empty relation
    for (key, values) in result
        key[1] == ":output" || continue
        first || println(io)
        first = false
        showkey(io, key[2:end])
        showrelation(io, values)
    end
end

function showkey(io, ks)
    isempty(ks) && (ks = ["Unit"]) # single empty tuple
    for k in ks
        printstyled(io, string("/", k), bold = true)
    end
    println(io)
end

function showrelation(io, rel)
    for row in rel
        print(io, "  ")
        isempty(row) && print(io, "()")
        for i = 1:length(row)
            i != 1 && printstyled(io, ", ", color=:light_blue)
            show(io, row[i])
        end
        println(io)
    end
end
