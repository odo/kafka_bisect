-module(kafka_bisect_kafka).
-export([find_all/1, find_all/3, find/5, find_and_reply/7]).
-export([init/1, first_index/1, last_index/1, previous_index/2, next_index/2, value/2]).

-define(STRETCHSIZE, 5 * 1024).

-record(state, {topic, partition, socket, first_index, last_index}).

find_all([Host, Port, DateTime]) ->
    find_all(atom_to_list(Host), list_to_integer(atom_to_list(Port)), iso8601:parse(atom_to_list(DateTime))),
    init:stop().

find_all(Host, Port, DateTime) ->
    application:load(kafka_bisect),
    {ok, TopicsStrings} = application:get_env(kafka_bisect, topics),
    Topics              = [list_to_binary(T) || T <- TopicsStrings],
    Ref                 = make_ref(),
    [spawn(?MODULE, find_and_reply, [self(), Ref, DateTime, Host, Port, Topic, 0]) || Topic <- Topics],
    Replies = [get_reply(Ref) || _ <- Topics],
    Now = now(),
    Struct = [{[{topic, Topic}, {offset, Offset}, {time, format_time(Time, Now)}]}||{Topic, Offset, Time} <- Replies],
    io:format("\n~s\n", [jiffy:encode(
        {[
            {query, [{[{time, iso8601:format(DateTime)}, {host, list_to_binary(Host)}, {port, Port}]}]},
            {head_time, format_time(0, Now)},
            {results, Struct}
        ]}
    )]).

format_time(0, DefaultTime) ->
    iso8601:format(DefaultTime);
format_time(Time, _) ->
    iso8601:format(Time).

get_reply(Ref) ->
    receive
        {Ref, Result} -> Result
    after 10000 ->
        timeout
    end.

find_and_reply(From, Ref, DateTime, Host, Port, Topic, Partition) ->
   Result = find(DateTime, Host, Port, Topic, Partition),
   From ! {Ref, Result}.

find(DateTime, Host, Port, Topic, Partition) ->
    TimestampQuery = datetime_to_timestamp(DateTime),
    {Offset, TimestampResult} = kafka_bisect:find(
        TimestampQuery, kafka_bisect_kafka, {Host, Port, Topic, Partition}),
    DateTimeResult = case TimestampResult of
        head -> 0;
        _    -> timestamp_to_datetime(TimestampResult)
    end,
    {Topic, Offset, DateTimeResult}.

init({Host, Port, Topic, Partition}) ->
    {ok, Socket} = gen_tcp:connect(
        Host, Port, [binary, {active, false}, {packet, raw}], infinity),
    State1 = #state{ topic = Topic, partition = Partition, socket = Socket},
    State1#state{ first_index = first_index(State1), last_index = last_index(State1) }.


first_index(#state{ topic = Topic, partition = Partition, socket = Socket }) ->
    index_from_kafka(-2, Topic, Partition, Socket).

last_index(#state{ topic = Topic, partition = Partition, socket = Socket }) ->
    index_from_kafka(-1, Topic, Partition, Socket).

next_index(LastIndex, #state{ topic = Topic, partition = Partition, socket = Socket }) ->
    Data        = binary_from_kafka(LastIndex, Topic, Partition, Socket),
    IndexInData = kafka_bisect_binary:next_index(0, Data),
    IndexInData + LastIndex.

previous_index(IndexCandidate,
    #state{ topic = Topic, partition = Partition, socket = Socket, first_index = FirstIndex }) ->
    Start       = max(FirstIndex, IndexCandidate - ?STRETCHSIZE),
    Data        = binary_from_kafka(Start, Topic, Partition, Socket),
    IndexInData = kafka_bisect_binary:previous_index(IndexCandidate - Start, Data),
    IndexInData + Start.

value(Index, #state{ topic = Topic, partition = Partition, socket = Socket }) ->
    Data = binary_from_kafka(Index, Topic, Partition, Socket),
    kafka_bisect_binary:value(0, Data).

index_from_kafka(Mode, Topic, Partition, Socket) ->
    Req = kafka_protocol:offset_request(Topic, Partition, Mode, 1),
    ok = gen_tcp:send(Socket, Req),

    case gen_tcp:recv(Socket, 6) of
        {ok, <<L:32/integer, 0:16/integer>>} ->
            {ok, Data} = gen_tcp:recv(Socket, L-2),
            hd(kafka_protocol:parse_offsets(Data));
        {ok, B} ->
            {error, B}
    end.

binary_from_kafka(Offset, Topic, Partition, Socket) ->
    Req = kafka_protocol:fetch_request(Topic, Partition, Offset, ?STRETCHSIZE),
    ok = gen_tcp:send(Socket, Req),
    case gen_tcp:recv(Socket, 6) of
        {ok, <<2:32/integer, 0:16/integer>>} ->
            <<>>;
        {ok, <<L:32/integer, 0:16/integer>>} ->
            {ok, Data} = gen_tcp:recv(Socket, L-2),
            Data;
        {ok, <<_:32/integer, ErrorCode:16/integer>>} ->
            {error, kafka_protocol:error(ErrorCode)}
    end.

timestamp_to_datetime(Timestamp) ->
   BaseDate      = calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
   Seconds       = BaseDate + Timestamp,
   calendar:gregorian_seconds_to_datetime(Seconds).

datetime_to_timestamp(DateTime) ->
    BaseDate      = calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
    calendar:datetime_to_gregorian_seconds(DateTime) - BaseDate.

