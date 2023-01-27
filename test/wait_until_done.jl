using ExceptionUnwrapping: unwrap_exception_to_root

# This test is _pretty complicated_ since it's trying to test something that depends on
# timing: testing that wait_until_done() polls for the expected amount of time in between
# calls to get_transaction.
# Testing anything to do with timing is always complicated. We tackle it here by mocking
# both sleep() and time(), and injecting fake times, and then making sure that the
# function is computing the correct duration to sleep, based on those times.
@testset "wait_until_done polls correctly" begin
    now_ms = round(Int, time() * 1e3)
    txn_str = """{
            "id": "a3e3bc91-0a98-50ba-733c-0987e160eb7d",
            "results_format_version": "2.0.1",
            "state": "RUNNING",
            "created_on": $(now_ms)
        }"""
    txn = JSON3.read(txn_str)

    ctx = Context("region", "scheme", "host", "2342", nothing, "audience")

    start = now_ms / 1e3
    # Simulate OVERHEAD of 0.1 + round-trip-time of 0.5
    times = [
        start + 2,            # First call takes 2 seconds then returns async
        start + 2.2 + 0.5,    # So we slept 0.2 seconds, then get_txn takes 0.5 secs
        start + 2.97 + 0.5    # Now we sleep 2.7 * 1.1 ≈ 2.97, then again 0.5 RTT.
    ]
    i = 1
    time_patch = @patch function Base.time()
        v = times[i]
        i += 1
        return v
    end
    # Here, we test that each call to sleep is the correct calculation of current "time"
    # minus start time * the overhead.
    sleep_patch = @patch function Base.sleep(duration)
        @info "Mock sleep for $duration"
        @test duration ≈ (times[i-1] - start) * RAI.TXN_POLLING_OVERHEAD
    end

    # This is returned on each get_txn() request.
    unfinished_response = HTTP.Response(
        200,
        ["Content-Type" => "application/json"],
        body = """{"transaction": $(txn_str)}"""
    )

    # Stop the test after 3 polls.
    ABORT = :ABORT_TEST

    request_patch = @patch function RAI.request(ctx::Context, args...; kw...)
        if i <= 3
            return unfinished_response
        else
            # Finish the test
            throw(ABORT)
        end
    end

    # Call the function with the patches. Assert that it ends with our ABORT exception.
    apply([time_patch, sleep_patch, request_patch]) do
        try
            wait_until_done(ctx, txn)
        catch e
            @assert unwrap_exception_to_root(e) == ABORT
        end
    end

    # Test that we made it through all the expected polls, so that we know the above
    # `@test`s all triggered.
    @test i == 4
end
