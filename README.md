# dgraph-loader

Fast live loader for dgraph.

Format accepted is line-delimited json, different from `dgraph live`.

```
dgraph-loader 

USAGE:
    dgraph-loader [OPTIONS] --alpha <ALPHA> --chunk-size <CHUNK_SIZE> --concurrency <CONCURRENCY>

OPTIONS:
    -a, --alpha <ALPHA>                HTTP or HTTPS url of gRPC endpoint for Alpha
    -c, --concurrency <CONCURRENCY>    How many transactions to run at once
    -h, --help                         Print help information
    -q, --quiet                        Disable progress output
    -s, --chunk-size <CHUNK_SIZE>      Number of documents to load in each transaction
    -U, --upsert-keys <UPSERT_KEYS>    Keys that should be used to find existing documents for
                                       upsert (can be specified multiple times)
```
