-module(kafka_bisect_test).
-compile(export_all).

% Include etest's assertion macros.
-include_lib("etest/include/etest.hrl").

test_find() ->
    L = [E * 2 || E <- lists:seq(1, 100)],
    ?assert_equal({1, 2},   kafka_bisect:find(2, kafka_bisect_list, L)),
    ?assert_equal({100, 200}, kafka_bisect:find(200, kafka_bisect_list, L)),
    ?assert_equal({42, 84},  kafka_bisect:find(84, kafka_bisect_list, L)).

test_find_off_limit() ->
    L = [E * 2 || E <- lists:seq(1, 100)],
    ?assert_equal({1, 2},     kafka_bisect:find(-10, kafka_bisect_list, L)),
    ?assert_equal({101, head}, kafka_bisect:find(10000, kafka_bisect_list, L)),
    ?assert_equal({100, 200}, kafka_bisect:find(200, kafka_bisect_list, L)),
    ?assert_equal({42,  84},  kafka_bisect:find(84, kafka_bisect_list, L)).

test_find_inbetween() ->
    L = [E * 2 || E <- lists:seq(1, 100)],
    ?assert_equal({43, 86},   kafka_bisect:find(85, kafka_bisect_list, L)).
