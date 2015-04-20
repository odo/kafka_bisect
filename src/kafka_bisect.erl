-module(kafka_bisect).

-export([find/3]).

find(Key, CallbackModule, StartArgs) ->
    Data = CallbackModule:init(StartArgs),
    case find(CallbackModule:first_index(Data), CallbackModule:last_index(Data), Key, CallbackModule, Data) of
        {Index, KeyFound} when KeyFound < Key ->
            NextIndex = CallbackModule:next_index(Index, Data),
            {NextIndex, CallbackModule:value(NextIndex, Data)};
        {Index, KeyFound} ->
            {Index, KeyFound}
    end.

find(MinIndex, MaxIndex, _, CallbackModule, Data) when MaxIndex < MinIndex ->
    {MinIndex, CallbackModule:value(MinIndex, Data)};
find(SameIndex, SameIndex, _, CallbackModule, Data) ->
    {SameIndex, CallbackModule:value(SameIndex, Data)};
find(MinIndex, MaxIndex, Key, CallbackModule, Data) ->
    MiddleIndex = CallbackModule:next_index(((MaxIndex - MinIndex) div 2) + MinIndex, Data),
    MiddleValue = CallbackModule:value(MiddleIndex, Data),
    if
        MiddleValue =:= Key ->
            {MiddleIndex, MiddleValue};
        MiddleValue > Key ->
            find(MinIndex, CallbackModule:previous_index(MiddleIndex, Data), Key, CallbackModule, Data);
        MiddleValue < Key ->
            find(CallbackModule:next_index(MiddleIndex + 1, Data), MaxIndex, Key, CallbackModule, Data)
    end.
