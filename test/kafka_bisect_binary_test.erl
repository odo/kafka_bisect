-module(kafka_bisect_binary_test).
-compile(export_all).

% Include etest's assertion macros.
-include_lib("etest/include/etest.hrl").

% The example data has gaps and dups.
% Message Offsets and Timestamps:
% {99, 1391788582},
% {599, 1391788584},
% {1142, 1391788586},
% {1506, 1391788588},
% {1883, 1391788590},
% {2376, 1391788590},
% {2897, 1391788592}

-define(EXAMPLEDATA, <<"152 1391788580 /w/funnel/ jailbroken=0&tscreated=1391788579&st2version=1.8.0&tscreated=1391788580��8Z�208.54.32.213 1391788582 /w/event/ jailbroken=0&coins=0&st2=EventApplicationOpened&osversion=7.0.4&mid=6767106982122623120&device=iPhone6,1&version=1.8.0&type=user_interaction&sbsuserid=I4r0XivNnsHCmEhJHrjgHcyPvPP&level_number=58&files_tampered=0&networkstate=PLNetworkServiceStateOnlineWithoutFacebookUpdatingUser&max_level_number=57&adid=633F366B-60E7-4F5C-A9A8-A9F5C62E4E37&appversion=30&stars=99&round_id=116&language=en&online=1&st1=Game&lives=4&remote_config_version=11&tscreated=1391735728�Z68.39.238.205 1391788584 /w/event/ jailbroken=0&remote_config_version=11&total_friends=181&st2=EventApplicationClosed&osversion=7.0.4&mid=6767089120267808643&device=iPad3,4&s=1568323580&version=1.8.0&type=user_interaction&sbsuserid=Lfll6VeMxQSFuZMijc7sShW29YK&level_number=36&files_tampered=0&networkstate=PLNetworkServiceStateOnlineWithFacebookIdle&max_level_number=35&adid=6156228A-BCF9-414F-8E4D-9A6A7D8AB565&appversion=30&stars=66&playing_friends=8&round_id=169&language=en&online=1&st1=GameLost&lives=4&coins=0&tscreated=1391788578k�Le89.204.138.103 1391788586 /w/pgr/ appversion=30&mid=6767208462008178109&device=iPhone4,1&adid=92735511-55F7-4C34-840E-40A50F665E21&language=de&files_tampered=0&remote_config_version=11&osversion=7.0.4&version=1.8.0&networkstate=PLNetworkServiceStateStartupSBSRequestCredentials&jailbroken=0&online=1&sbsuserid=AWbt5wWlPbV7cdQ1XZCHVuDB9ri&tscreated=1391788553sK�%178.193.102.235 1391788588 /w/pgr/ appversion=30&mid=6767001332051283568&device=iPhone4,1&adid=184FE793-226E-4907-A58A-635567325A0D&language=it&files_tampered=0&remote_config_version=11&osversion=7.0.4&version=1.8.0&networkstate=PLNetworkServiceStateOnlineWithFacebookIdle&s=1345187303&jailbroken=0&online=1&sbsuserid=1MYiFgJDUBqMAWEVDrjhJkzzmev&tscreated=1391788575�_f��94.96.237.5 1391788590 /w/event/ jailbroken=1&coins=0&st2=EventButtonStart&osversion=6.0.1&mid=6667064399817417998&device=iPhone4,1&version=1.7.2&type=user_interaction&sbsuserid=HE3ZbFwblM6muDkvq0s7fyYjCjU&level_number=2&files_tampered=0&networkstate=PLNetworkServiceStateOnlineWithoutFacebookUpdatingUser&max_level_number=22&adid=F4D3BFBC-ADCC-4DF6-A12B-347E348DA98B&appversion=27&stars=39&round_id=91&language=ar&online=1&st1=Map&lives=5&remote_config_version=9&tscreated=1391173831(`4��50.153.130.8 1391788590 /w/event/ jailbroken=0&remote_config_version=11&total_friends=151&st2=EventButtonPlay_withLives&osversion=7.0.4&mid=6767189087771201359&device=iPhone4,1&s=100006986258942&version=1.8.0&type=user_interaction&sbsuserid=ApDfpi31hWerrE2ut6M3fkXwChF&level_number=8&files_tampered=0&networkstate=PLNetworkServiceStateOnlineWithFacebookIdle&max_level_number=8&adid=D49F42E8-7E61-4F01-9856-72C62BA73419&appversion=30&stars=20&playing_friends=4&round_id=150&language=en&online=1&st1=LevelDetails&lives=3�96.231.212.116 1391788592 /w/event/ jailbroken=0&coins=70&st2=EventButtonRetry&osversi">>).

test_seek_and_value() ->
    ?assert_equal({99,   1391788582}, kafka_bisect_binary:seek_and_value(?EXAMPLEDATA, first)),
    ?assert_equal({2376, 1391788590}, kafka_bisect_binary:seek_and_value(?EXAMPLEDATA, {before, 2897})),
    ?assert_equal({0,    1391788582}, kafka_bisect_binary:seek_and_value(<<"XXXXXXXXX208.54.32.213 1391788582 /w/event">>, first)),
    ?assert_equal({0,    1391788582}, kafka_bisect_binary:seek_and_value(<<"XXXXXXXX208.54.32.213 1391788582 /w/event">>, first)).

test_next_index() ->
    I1 = kafka_bisect_binary:first_index(?EXAMPLEDATA),
    ?assert_equal({99, 1391788582}, {I1, kafka_bisect_binary:value(I1, ?EXAMPLEDATA)}),
    I2 = kafka_bisect_binary:next_index(I1, ?EXAMPLEDATA),
    ?assert_equal({599, 1391788584}, {I2, kafka_bisect_binary:value(I2, ?EXAMPLEDATA)}),
    I2Prev = kafka_bisect_binary:previous_index(I2, ?EXAMPLEDATA),
    ?assert_equal({99, 1391788582}, {I2Prev, kafka_bisect_binary:value(I2Prev, ?EXAMPLEDATA)}),
    I3 = kafka_bisect_binary:next_index(I2, ?EXAMPLEDATA),
    ?assert_equal({1142, 1391788586}, {I3, kafka_bisect_binary:value(I3, ?EXAMPLEDATA)}),
    I3Prev = kafka_bisect_binary:previous_index(I3, ?EXAMPLEDATA),
    ?assert_equal({599, 1391788584}, {I3Prev, kafka_bisect_binary:value(I3Prev, ?EXAMPLEDATA)}),
    I4 = kafka_bisect_binary:next_index(I3, ?EXAMPLEDATA),
    ?assert_equal({1506, 1391788588}, {I4, kafka_bisect_binary:value(I4, ?EXAMPLEDATA)}),
    I5 = kafka_bisect_binary:next_index(I4, ?EXAMPLEDATA),
    ?assert_equal({1883, 1391788590}, {I5, kafka_bisect_binary:value(I5, ?EXAMPLEDATA)}),
    I6 = kafka_bisect_binary:next_index(I5, ?EXAMPLEDATA),
    ?assert_equal({2376, 1391788590}, {I6, kafka_bisect_binary:value(I6, ?EXAMPLEDATA)}),
    I7 = kafka_bisect_binary:next_index(I6, ?EXAMPLEDATA),
    ?assert_equal({2897, 1391788592}, {I7, kafka_bisect_binary:value(I7, ?EXAMPLEDATA)}).

test_smaller_than() ->
    ?assert_equal(undefined,       kafka_bisect_binary:smaller_than(10, [[{20, '_'}, '_']])),
    ?assert_equal(undefined,       kafka_bisect_binary:smaller_than(10, [])),
    ?assert_equal(undefined,       kafka_bisect_binary:smaller_than(10, [[{10, '_'}, '_']])),
    ?assert_equal([{9, '_'}, '_'], kafka_bisect_binary:smaller_than(10, [[{9, '_'}, '_'], [{10, '_'}, '_']])),
    ?assert_equal([{9, '_'}, '_'], kafka_bisect_binary:smaller_than(12, [[{9, '_'}, '_'], [{15, '_'}, '_']])).

test_value() ->
    ?assert_equal(head,       kafka_bisect_binary:value(100, <<"too small">>)),
    ?assert_error(_,          kafka_bisect_binary:value(3,  <<"not what you expect or anticipate">>)),
    ?assert_error(_,          kafka_bisect_binary:value(3,  <<"not what you expect 208.54.32.213 1391788580 /w">>)),
    ?assert_equal(1391788580, kafka_bisect_binary:value(10, <<"expected: XXXXXXXXX208.54.32.213 1391788580 /w">>)).

test_find() ->
    ?assert_equal({99,  1391788582}, kafka_bisect:find(0, kafka_bisect_binary, ?EXAMPLEDATA)),
    ?assert_equal({99,  1391788582}, kafka_bisect:find(1391788582, kafka_bisect_binary, ?EXAMPLEDATA)),
    ?assert_equal({99,  1391788582}, kafka_bisect:find(1391788581, kafka_bisect_binary, ?EXAMPLEDATA)),
    ?assert_equal({599, 1391788584}, kafka_bisect:find(1391788583, kafka_bisect_binary, ?EXAMPLEDATA)),
    ?assert_equal({1883, 1391788590}, kafka_bisect:find(1391788590, kafka_bisect_binary, ?EXAMPLEDATA)),
    ?assert_equal({2897, 1391788592}, kafka_bisect:find(1391788591, kafka_bisect_binary, ?EXAMPLEDATA)),
    ?assert_equal({2897, 1391788592}, kafka_bisect:find(1391788592, kafka_bisect_binary, ?EXAMPLEDATA)),
    ?assert_equal({byte_size(?EXAMPLEDATA), head}, kafka_bisect:find(1391788593, kafka_bisect_binary, ?EXAMPLEDATA)).
