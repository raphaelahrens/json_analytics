`json_analyzer` reads in a all JSON files in a directory, collects all the object members of each JSON files and combines them so that they can be studied.

The problem `json_analyzer` solves is, when you have multiple JSON files, which all hold similar data but you have no schema for it.
BUt still you want to figure out what are the possible queries you can use with this dataset, or you want to know what datatype a specific query will have.
`json_analyzer` is here to help.

If you have two JSON files `a.json`

```
{
  "test": true,
  "a"{
      "a": "this is file a.json"
  }
}
```

and `b.json`

```
{
  "test": 1,
  "b": ["this", "is", "file", "b.json"]
}
```

`json_analyzer` creates an internal datastructure similiar to this JSON

```
{
  "test": {
      "count": 2,
      "types": ["Bool", "Number"],
      "files": [
        "./a.json",
        "./b.json"
      ]
  },
  "a": {
      "a": {
          "count": 1,
          "types": ["String"],
          "files": [
            "./a.json"
          ]
      }
  },
  "b": {
      "count": 1,
      "types": ["Array"],
      "files": [
        "./b.json"
      ]
  }
}
```

# Usage

```
json_analytics 0.1.0

USAGE:
    json_analytics <DIR> <SUBCOMMAND>

ARGS:
    <DIR>    

OPTIONS:
    -h, --help       Print help information
    -V, --version    Print version information

SUBCOMMANDS:
    help     Print this message or the help of the given subcommand(s)
    keys     List all member keys with types and how often this member is in the dataset
    query    Query the analytics of a specific member
```

```
json_analytics-keys 
List all member keys with types and how often this member is in the dataset

USAGE:
    json_analytics <DIR> keys [OPTIONS]

OPTIONS:
    -h, --help                       Print help information
        --type-count <TYPE_COUNT>    filter all member which have at lest [TYPE_COUNT] types
                                     [default: 1]
```

```
json_analytics-query 
Query the analytics of a specific member

USAGE:
    json_analytics <DIR> query <QUERY>

ARGS:
    <QUERY>    the query is similar to a jq query ".a.b.c"

OPTIONS:
    -h, --help    Print help information
```
