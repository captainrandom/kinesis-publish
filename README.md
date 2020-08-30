# kinesis-publish
Read lines from stdin sends them as messages to Kinesis

This is still sub alpha. Once I have most of the todo's finished we will probably have an initial version of the alpha.
Next version should use async a bit better.

## Install
```bash
cargo install kinesis-publish
```

## Run

Here is the options:
```
echo message | kpub --stream-name <stream name> --profile <aws profile>
```

or by using the short hand:

```
echo message | kpub -s <stream name> -p <aws profile>
```


## TODO:
1. Still need to add the cli options
1. Need to figure out how to properly create a package
1. Need to add some unit tests
1. Possibly add github actions to build and push updates (when I push a major version tag)
