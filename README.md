# dgraph-loader

Fast live loader for dgraph.

Format accepted is line-delimited json, different from `dgraph live`.

Upsert keys can be specified by regex, and it's possible to have more than one
of them. Nicer than having to have a blank node formatted key in a `uid` field,
and having that end up in some large `xid` predicate - but you can still do
that if you want. Just set the upsert pattern to `^xid$` and have a field
called `xid` instead.

Explicitly specifying the `uid` is not currently supported, for now. Nor is the
blank node to `xid` conversion that `dgraph live` supports.

If desired, I may try to improve compatibility with the input that `dgraph
live` accepts, but I probably won't support RDF input because I can't wrap my
mind around how upsert can reasonably work with that.

Currently I've found it mostly to be faster, but sometimes it causes
instability which I'm still working on.

```
dgraph-loader 

USAGE:
    dgraph-loader [OPTIONS] --alpha <ALPHA> --chunk-size <CHUNK_SIZE> --concurrency <CONCURRENCY>

OPTIONS:
    -a, --alpha <ALPHA>
            HTTP or HTTPS url of gRPC endpoint for Alpha

    -c, --concurrency <CONCURRENCY>
            How many transactions to run at once

    -h, --help
            Print help information

    -q, --quiet
            Disable progress output

    -s, --chunk-size <CHUNK_SIZE>
            Number of documents to load in each transaction

    -U, --upsert-pattern <UPSERT_PATTERNS>
            Regex for keys that should be used to find existing documents for upsert (can be
            specified more than once)
```
