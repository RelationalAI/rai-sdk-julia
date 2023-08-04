# Copyright 2022 RelationalAI, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

# List engines, optionally filtered by state.

using RAI: Context, HTTPError, load_config, get_transaction_events
using HTTP

include("parseargs.jl")

function run(; id, profile, streams=10, queries_per_stream=3)
    conf = load_config(; profile = profile)
    ctx = Context(conf)
    
    successes = 0
    failures = 0
    
    Threads.@sync begin
        for t in 1:streams
            Threads.@spawn begin
                for i in 1:queries_per_stream
                    @info "starting $t:$i"
                    try
                        resp_timed = @timed get_with_exp_backoff(ctx, id, 1, t, i)
                        resp = resp_timed.value
                        @info(
                            "finished $t:$i",
                            resp_timed.time,
                            resp.status,
                            length(resp.body),
                            HTTP.header(resp, "x-request-id"),
                        )
                        successes += 1
                    catch err
                        @error "request $t:$i failed" err
                        failures += 1
                    end
                end
            end
        end
    end
    
    return (; successes, failures)
end

max_delay = 10

function get_with_exp_backoff(ctx, id, attempt, t, i)
    try
        return get_transaction_events(ctx, id)
    catch err
        if err isa HTTP.StatusError
            sleep_dur = min(max_delay, 2 ^ attempt) + rand()
            @info "backoff $t:$i for $sleep_dur sec"
            sleep(sleep_dur)
            get_with_exp_backoff(ctx, id, attempt + 1, t, i)
        else
            rethrow(err)
            failures += 1
        end
    end
end

function main()
    args = parseargs(
        "--id", Dict(:help => "transaction id"),
        "--profile", Dict(:help => "config profile (default: default)"))
    try
        run(; id = args.id, profile = args.profile)
    catch e
        e isa HTTPError ? show(e) : rethrow()
    end
end

main()
