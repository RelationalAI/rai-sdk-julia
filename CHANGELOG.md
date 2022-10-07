# Changelog

## main
* Update models actions to use v2 protocol
* Update `load_model` to `load_models`
## v0.2.1
* Increased `connection_limit` to 4096

## v0.2.0
* Deprecate metadata JSON format.
* Add support to deserialize ProtoBuf metadata.
* `get_transaction_metadata` returns a `MetadataInfo`, see `src/proto`
  and `src/gen/relationalai/protocol/` for more information.

## v0.1.5
* Add support for optional audience field to Config

## v0.1.4
* Retry retryable HTTP errors

## v0.1.0
* Implement V2 `show_result` method

## v0.0.4
* Properly filter transaction results based on multi-part content type
* Set `HTTP.jl` compat to `1.0`

## v0.0.3
* New access token for each request
* Anticipate access token expiration

## v0.0.2

* Added synchronous `exec()` function that polls the v2 `exec_async()` function until completion ([#25](https://github.com/RelationalAI/rai-sdk-julia/pull/25)).
    - Uses the "v2 protocol", so the transactions will show up in your transaction log.
    - If you cancel the polling via `ctrl-C`, the error log will print the transaction ID, so you can still
      recover the transaction or cancel it.
* Consistent return format (`Dict`) from `exec_async()` and `exec()`, regardless of whether you get synchronous results ([#24](https://github.com/RelationalAI/rai-sdk-julia/pull/24)).

## main

* Add find_user to api.jl
* Rename examples/get-userid.jl to examples/find-user.jl
* Fixed bug in support for custom extra `headers` in SDK. For example:
```
create_engine(ctx, engine; size = size, headers=["my-custom-header" => "custom header value"])
```
