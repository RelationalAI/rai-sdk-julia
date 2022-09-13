function replshow(io, result)
    first = true
    isempty(result) && println("{}") # false / empty relation
    for (key, values) in result
        key.name == :output || continue
        first || println(io)
        first = false
        showkey(io, key)
        showrelation(io, values)
    end
end

function showkey(io, k)
    ks = vcat(k.keys, k.values)
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
