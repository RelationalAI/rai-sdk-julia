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

# Returns the user with the given email.

using RAI: Context, HTTPError, load_config, find_user

include("parseargs.jl")

function run(email; profile)
    cfg = load_config(; profile = profile)
    ctx = Context(cfg)
    rsp = find_user(ctx, email)
    println(rsp)
end

function main()
    args = parseargs(
        "email", Dict(:help => "user email", :required => true),
        "--profile", Dict(:help => "config profile (default: default)"))
    try
        run(args.email; profile = args.profile)
    catch e
        e isa HTTPError ? show(e) : rethrow()
    end
end

main()
