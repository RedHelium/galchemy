-module(galchemy_test_support).
-export([query_sql/1, query_parameters/1, query_timeout/1]).

query_sql({'query', Sql, _Parameters, _Decoder, _Timeout}) ->
    Sql.

query_parameters({'query', _Sql, Parameters, _Decoder, _Timeout}) ->
    lists:reverse(Parameters).

query_timeout({'query', _Sql, _Parameters, _Decoder, Timeout}) ->
    Timeout.
