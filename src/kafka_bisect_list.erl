-module(kafka_bisect_list).
-export([init/1, first_index/1, last_index/1, previous_index/2, next_index/2, value/2]).

init(InitArgs) -> InitArgs.

first_index(_) -> 1.

last_index(Data) -> length(Data).

next_index(LastIndex, Data) -> min(length(Data) + 1, LastIndex + 1).

previous_index(IndexCandidate, _) -> IndexCandidate - 1.

value(Index, Data) when Index > length(Data) ->
    head;
value(Index, Data) ->
    lists:nth(Index, Data).
