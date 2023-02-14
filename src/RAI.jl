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

"""
    RAI

The `RAI` module provides functionality for accessing the RelationalAI REST
APIs.
"""
module RAI

import Pkg

# Used for sending the User Agent.
const PROJECT_VERSION = Pkg.project().version

export
    AccessToken,
    ClientCredentials,
    Config,
    Context,
    HTTPError,
    load_config

export
    create_database,
    delete_database,
    get_database,
    list_databases

export
    create_engine,
    delete_engine,
    get_engine,
    list_engines

export
    delete_models,
    get_model,
    list_models,
    load_models

export
    create_oauth_client,
    delete_oauth_client,
    get_oauth_client,
    list_oauth_clients

export
    create_user,
    disable_user,
    enable_user,
    get_user,
    list_users,
    update_user

export
    delete_models,
    get_model,
    list_models

export
    exec,
    exec_async,
    wait_until_done,
    list_edbs,
    load_csv,
    load_json

export
    cancel_transaction,
    get_transaction,
    get_transaction_metadata,
    get_transaction_problems,
    get_transaction_results

export
    Relation,
    TransactionResult,
    getcolumn,
    getrow,
    schema,
    show_problems,
    show_result

include("creds.jl")
include("config.jl")
include("rest.jl")
# Generated with:
# ProtoBuf.protojl(readdir("src/proto"), "src/proto", "src/gen", parametrize_oneofs=true)
include("gen/relationalai/relationalai.jl")
import .relationalai.protocol
include("metadata.jl")
include("response.jl")
include("api.jl")
include("results.jl")

end
