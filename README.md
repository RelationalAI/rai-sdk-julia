# The RelationalAI Software Development Kit for Julia

The RelationalAI (RAI) SDK for Julia enables developers to access the RAI REST APIs from Julia.

* You can find RelationalAI Julia SDK documentation at <https://docs.relational.ai/rkgms/sdk/julia-sdk> 
* You can find RelationalAI product documentation at <https://docs.relational.ai> 
* You can learn more about RelationalAI at <https://relational.ai> 

## Getting started

### Installation

The package can be installed from source using the Julia REPL, through the package manager.

```julia
julia> using Pkg; Pkg.add("RAI")
```
or
```
] add RAI
```

### Create a configuration file

In order to run the examples you will need to create an SDK config file.
The default location for the file is `$HOME/.rai/config` and the file should
include the following:

Sample configuration using OAuth client credentials:

```conf
[default]
host = azure.relationalai.com
port = <api-port>      # optional, default: 443
scheme = <scheme>      # optional, default: https
client_id = <your client_id>
client_secret = <your client secret>
client_credentials_url = <account login URL>  # optional
# default: https://login.relationalai.com/oauth/token
```

Client credentials can be created using the RAI console at https://console.relationalai.com/login

You can copy `config.spec` from the root of this repo and modify as needed.

## Examples

Each of the examples in the `./examples` folder can be run from the command
line, eg:

```console
$ julia --project=. examples/list-engines.jl
```

## Support

You can reach the RAI developer support team at `support@relational.ai`

## Contributing

We value feedback and contributions from our developer community. Feel free
to submit an issue or a PR here.

## License

The RelationalAI Software Development Kit for Julia is licensed under the
Apache License 2.0. See:
https://github.com/RelationalAI/rai-sdk-julia/blob/master/LICENSE
