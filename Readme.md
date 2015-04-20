kafka_bisect
============

kafak_bisect is a tool to search for timestamps in kafka messages. It assumes the following message format:

`{IP} {TIMESTAMP} {MESSAGE}`

If you have a different format you need to change `kafka_bisect_binary:seek_and_value/2`.

# Prerequisites
[Erlang](http://www.erlang.org/) needs to be installed.

# Installation

```
git clone git@github.com:wooga/kafka_bisect.git
cd kafka_bisect
./rebar get-deps com
```

# Running

From the command line you can specify a host, port and ISO 8601 Timestamp to retrieve offsets.
Topics are defined in `./config/default.config`.

```
./bin/kafka_bisect run kafka.sys11 9092 2014-02-17T00:00:00
Erlang R16B (erts-5.10.1) [source-05f1189] [smp:8:8] [async-threads:64] [hipe] [kernel-poll:false]

Eshell V5.10.1  (abort with ^G)
1>
{"query":[{"time":"2014-02-17T00:00:00Z","host":"kafka.sys11","port":9092}],"head_time":"2014-02-17T13:41:17Z","results":[{"topic":"topic1","offset":2732945670,"time":"2014-02-17T13:41:17Z"},{"topic":"topic2","offset":18992525,"time":"2014-02-17T13:41:17Z"}]}
```

# Output

The returned JSON containes the folloging data:

* `query`: your query
* `head_time`: the time which corresponds to the current head. If this time is returned for one of the topics, it means all its messages are older than the query and the offset points to the head of the topic.
* `results`: the closest offset and corresponding time for each topic.

# Note

The algorithm has an accuracy of one second. That means that if there are several messages with a matching timestamp the offset that is returned can be from any of those.
