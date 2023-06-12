@testitem "user-agent" begin
    @test RAI.PROJECT_VERSION isa String
    @test !isempty(RAI.PROJECT_VERSION)
    @test occursin(r"^rai-sdk-julia/\d+\.\d+\.\d+$", RAI._user_agent())
end

# def output =
#     1, "foo", 3.4, :foo;
#     2, "bar", 5.6, :foo
@testsetup module TestResponse
    export BODY
    BODY = """{
    "output": [{
        "rel_key": {
            "name": "output",
            "values": [],
            "keys": ["Int64", "String", "Float64", ":foo"],
            "type": "RelKey"
        },
        "type": "Relation",
        "columns": [[1, 2], ["foo", "bar"], [3.4, 5.6]]
    }],
    "problems": [],
    "actions": [{
        "name": "action1",
        "type": "LabeledActionResult",
        "result": {
            "output": [],
            "type": "QueryActionResult"}
    }],
    "debug_level": 0,
    "aborted": false,
    "type": "TransactionResult"
}
"""
end

@testitem "JSON Response" setup=[TestResponse] begin
    import JSON3
    rsp = JSON3.read(BODY)
    @test rsp.type == "TransactionResult"
    @test rsp.aborted == false
    @test length(rsp.problems) == 0
    @test length(rsp.output) == 1
    @test rsp.output[1].type == "Relation"
    @test rsp.output[1].rel_key.type == "RelKey"
    @test length(rsp.output[1].columns) == 3
    @test length(rsp.output[1].columns[1]) == 2
    @test rsp.output[1].columns[1] == [1, 2]
    @test rsp.output[1].columns[2] == ["foo", "bar"]
    @test rsp.output[1].columns[3] == [3.4, 5.6]
end

@testitem "TransactionResult" setup=[TestResponse] begin
    import JSON3
    rsp = JSON3.read(BODY)
    result = TransactionResult(rsp)
    @test result.type == "TransactionResult"
    @test result.aborted == false
    @test length(result.problems) == 0
    @test length(result.output) == 1
    @test rsp.output[1].type == "Relation"
    @test rsp.output[1].rel_key.type == "RelKey"
    @test length(result.output[1].columns) == 3
    @test length(result.output[1].columns[1]) == 2
    @test result.output[1].columns[1] == [1, 2]
    @test result.output[1].columns[2] == ["foo", "bar"]
    @test result.output[1].columns[3] == [3.4, 5.6]
end

@testitem "TransactionResult.relations" setup=[TestResponse] begin
    import JSON3
    rsp = JSON3.read(BODY)
    result = TransactionResult(rsp)
    @test length(result.relations) == 1
    @test length(result.relations[1]) == 2
    @test schema(result.relations[1]) == [Int64, String, Float64, Symbol]
    @test getrow(result.relations[1], 1) == (1, "foo", 3.4, :foo)
    @test getrow(result.relations[1], 2) == (2, "bar", 5.6, :foo)
    @test result.relations[1][1] == (1, "foo", 3.4, :foo)
    @test result.relations[1][2] == (2, "bar", 5.6, :foo)
end

# Instantiate `Relation` on the selected output.
@testitem "Relation" setup=[TestResponse] begin
    import JSON3
    rsp = JSON3.read(BODY)
    rel = Relation(rsp.output[1])
    @test length(rel) == 2  # two rows
    @test schema(rel) == [Int64, String, Float64, Symbol]
    @test getrow(rel, 1) == (1, "foo", 3.4, :foo)
    @test getrow(rel, 2) == (2, "bar", 5.6, :foo)
    @test rel[1] == (1, "foo", 3.4, :foo)
    @test rel[2] == (2, "bar", 5.6, :foo)
end

@testitem "Tables.AbstractRow" setup=[TestResponse] begin
    import JSON3
    import Tables
    rsp = JSON3.read(BODY)
    rel = Relation(rsp.output[1])
    @test Tables.istable(typeof(rel))
    @test Tables.rowaccess(typeof(rel))
    @test Tables.rows(rel) === rel
    row = first(rel)
    @test eltype(rel) == typeof(row)
    @test Tables.getcolumn(row, :Column1) == 1
    @test Tables.getcolumn(row, 1) == 1
    @test row.values == (1, "foo", 3.4, :foo)
    @test row.Column1 == 1
    @test row.Column2 == "foo"
    @test row.Column3 == 3.4
    @test row.Column4 == :foo
    @test row[1] == 1
    @test row[2] == "foo"
    @test row[3] == 3.4
    @test row[4] == :foo
    @test Tables.columnnames(rel) == [:Column1, :Column2, :Column3, :Column4]
    @test Tables.columnnames(row) == [:Column1, :Column2, :Column3, :Column4]
end

@testitem "Tables.dictrowtable" setup=[TestResponse] begin
    import JSON3
    import Tables
    rsp = JSON3.read(BODY)
    rel = Relation(rsp.output[1])
    table = Tables.dictrowtable(rel)
    row = first(table)
    @test row.Column1 == 1
    @test row.Column2 == "foo"
    @test row.Column3 == 3.4
    @test row.Column4 == :foo
end
