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
# limitations under the License.

import JSON3

"""
    show_problems([io::IO], rsp)

Print the problems associated with the given response `rsp` to the output
stream `io`.
"""
function show_problems(io::IO, rsp)
    isnothing(rsp) && return
    problems = get(rsp, "problems", nothing)
    isnothing(problems) && return
    for problem in problems
        if get(problem, "is_error", false)
            kind = "error"
        elseif get(problem, "is_exception", false)
            kind = "exception"
        else
            kind = "warning"  # ?
        end
        println(io, "$kind: $(problem["message"])")
        report = get(problem, "report", nothing)
        isnothing(report) && continue
        println(io, strip(report))
    end
    return nothing
end

show_problems(rsp) = show_problems(stdout, rsp)
