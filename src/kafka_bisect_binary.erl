-module(kafka_bisect_binary).
-compile(export_all).

init(StartArgs) ->
    StartArgs.

first_index(Data) ->
    seek(Data, first).

last_index(Data) ->
    seek(Data, {before, byte_size(Data)}).

next_index(LastIndex, Data) ->
    % we are jumping ahead the minimal length of the
    % IP and timestamp to avoid detecting pattern at
    % LastIndex (IP has variable length)
    Shift = 19,
    FirstCandidate = LastIndex + Shift,
    << _:FirstCandidate/binary, Rest/binary >> = Data,
    FirstCandidate + seek(Rest, first).

previous_index(IndexCandidate, Data) ->
    seek(Data, {before, IndexCandidate}).

value(Index, Data) when Index > byte_size(Data) ->
    head;
value(Index, Data) ->
    << _:Index/binary, Rest/binary >> = Data,
    {0, Value} = seek_and_value(Rest, first),
    Value.

seek(Data, Mode) ->
    {Offset, _} = seek_and_value(Data, Mode),
    Offset.

seek_and_value(<<>>, first) ->
    {0, head};
seek_and_value(Data, Mode) ->
    << _Prefix:9/binary, DataBody/binary >> = Data,
    {ok, RE} = re:compile("[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3} ([0-9]{10})"),
    REOptions = case Mode of
        first       -> [];
        {before, _} -> [global]
    end,
    case re:run(DataBody, RE, REOptions) of
        nomatch ->
            {byte_size(Data), undefined};
        {match, Matches} ->
            {IPOffset, TimestampOffset} = case Mode of
                first ->
                    [{IPOff, _}, {TimestampOff, _}] = Matches,
                    {IPOff, TimestampOff};
                {before, Pos} ->
                    [{IPOff, _}, {TimestampOff, _}] = smaller_than(Pos, Matches),
                    {IPOff, TimestampOff}
            end,
            TimestampOffsetBytes = TimestampOffset * 8,
            << _:TimestampOffsetBytes/bitstring, Timestamp:80/bitstring, _/bitstring >> = DataBody,
            {IPOffset, list_to_integer(binary_to_list(Timestamp))}
    end.

% returns the largest match position
% which is still smaller than Pos
smaller_than(Pos, Candidates) ->
    smaller_than(Pos, undefined, Candidates).

smaller_than(Pos, _, [Next = [{MsgOffset, _}, _] | Rest]) when MsgOffset < Pos ->
    smaller_than(Pos, Next, Rest);
smaller_than(_, Last, _List) ->
    Last.

