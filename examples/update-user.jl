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

# Update the given user.

using RAI: Context, HTTPError, load_config, update_user

include("parseargs.jl")

function run(userid; status, roles, profile)
    cfg = load_config(profile = profile)
    ctx = Context(cfg)
    rsp = update_user(ctx, userid; status = status, roles = roles, profiler = profile)
    println(rsp)
end

function main()
    args = parseargs(
        "userid", Dict(:help => "user id"),
        "--status", Dict(:help => "updated user status"),
        "--roles", Dict(:help => "updated user roles (default: user)", :nargs => '*'),
        "--profile", Dict(:help => "config profile (default: default)"))
    try
        run(args.userid; status = args.status, roles = args.roles, profile = args.profile)
    catch e
        e isa HTTPError ? show(e) : rethrow(e)
    end
end

main()
