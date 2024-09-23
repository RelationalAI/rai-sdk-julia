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
client_id = <your client_id>
client_secret = <your client secret>

# the following are all optional, with default values shown
# port = 443
# scheme = https
# client_credentials_url = https://login.relationalai.com/oauth/token
```

Client credentials can be created using the RAI console at https://console.relationalai.com/login

You can copy `config.spec` from the root of this repo and modify as needed.

## Examples

Each of the examples in the `./examples` folder can be run from the command
line, eg:

```console
$ julia --project=. examples/list-engines.jl
```

### Releases

- Link to detailed instructions [Julia Registrator](https://github.com/JuliaRegistries/Registrator.jl?tab=readme-ov-file#via-the-github-app)

The procedure for registering a new package is the same as for releasing a new version.
If the registration bot is not added to the repository, `@JuliaRegistrator` register will not result in package registration.

 Quick Summary:

1. Set the `(Julia)Project.toml` version field in your repository to your new desired `version`.
2. Comment `@JuliaRegistrator register()` on the commit/branch you want to register (e.g. like [here](https://github.com/JuliaRegistries/Registrator.jl/issues/61#issuecomment-483486641) or [here](https://github.com/chakravala/Grassmann.jl/commit/3c3a92610ebc8885619f561fe988b0d985852fce#commitcomment-33233149)).
**Note: Comment should be made on main after PR is merged.**
3. If something is incorrect, adjust, and redo step 1
4. If the automatic tests pass, but a moderator makes suggestions (e.g., manually updating your `(Julia)Project.toml` to include a [compat] section with version requirements for dependencies), then incorporate suggestions as you see fit into a new commit, and redo step 2 for the new commit. You don't need to do anything to close out the old request.
5. Finally, either rely on the [TagBot GitHub Action](https://github.com/marketplace/actions/julia-tagbot) to tag and make a github release or alternatively tag the release manually.
6. Check [juliahub](https://juliahub.com/ui/Packages/General/RAI) to make sure package is published successfully


## Support

You can reach the RAI developer support team at `support@relational.ai`

## Contributing

We value feedback and contributions from our developer community. Feel free
to submit an issue or a PR here.

## License

The RelationalAI Software Development Kit for Julia is licensed under the
Apache License 2.0. See:
https://github.com/RelationalAI/rai-sdk-julia/blob/master/LICENSE
