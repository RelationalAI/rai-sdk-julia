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

# Implementation of the Tables.jl interface for transaction results.

import JSON3
import Tables

struct Result
    data::JSON3.Object
end

data(r::Result) = getfield(r, :data)

function Base.getproperty(r::Result, name::Symbol)
    name == :data && return data(r)
    getfield(data(r), name)
end
