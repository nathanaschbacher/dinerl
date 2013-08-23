-module(bench).
-export([setup/0, spawn_and_try/2, execute/2, main/1]).


main(N) ->
    crypto:start(),
    % Start = erlang:now(),
    % run(N),
    % End = erlang:now(),
    % io:format("~p~n", [timer:now_diff(End, Start)]).
    Start1 = erlang:now(),
    run2(N),
    End1 = erlang:now(),
    io:format("~p~n", [timer:now_diff(End1, Start1)]).


run(0) ->
    dynamodb:signature_header_4("FDSFDSAADFS", "AFSDAFSDADFSAFSD", "CIAO", undefined, "20130821T191720Z", "dynamodb.us-east-1.amazonaws.com", "SOMEOMASOMDAOFSDAASDFADFSSSSSSSSSSSSSSSSSSSSSSSS");
run(N) ->
    dynamodb:signature_header_4("FDSFDSAADFS", "AFSDAFSDADFSAFSD", "CIAO", undefined, "20130821T191720Z", "dynamodb.us-east-1.amazonaws.com", "SOMEOMASOMDAOFSDAASDFADFSSSSSSSSSSSSSSSSSSSSSSSS"),
    run(N-1).

run2(0) ->
    dynamodb:signature_header("FDSFDSAADFS", "AFSDAFSDADFSAFSD", "CIAO", "foobar", "20130821T191720Z", "dynamodb.us-east-1.amazonaws.com", "SOMEOMASOMDAOFSDAASDFADFSSSSSSSSSSSSSSSSSSSSSSSS");
run2(N) ->
    dynamodb:signature_header("FDSFDSAADFS", "AFSDAFSDADFSAFSD", "CIAO", "foobar", "20130821T191720Z", "dynamodb.us-east-1.amazonaws.com", "SOMEOMASOMDAOFSDAASDFADFSSSSSSSSSSSSSSSSSSSSSSSS"),
    run(N-1).

setup() ->
    ets:new(dinerl_data, [named_table, public]),
    ets:insert(dinerl_data,
        {args, {<<"API_ACCESS_KEY">>, <<"API_SECRET_KEY">>, "us-east-1b",
                "API_TOKEN", "2012-02-21T12:33:45.4123Z", 34567890453345534}}).


spawn_and_try(0, Times) ->
    receive_all(Times);
spawn_and_try(Times, Total) ->
    case Times == 50 of
        true ->
            timer:sleep(200);
        false ->
            pass
    end,
    erlang:spawn(?MODULE, execute, [Times, self()]),
    spawn_and_try(Times-1, Total).

execute(Id, Father) ->
    io:format("calling ~p~n", [Id]),
    dinerl:update_item(
        <<"Products">>,
        [{<<"HashKeyElement">>, [{<<"S">>, <<"AKEY">>}]}],
        [{update, [{<<"Visited">>, [{value, [{<<"SS">>, [<<"AVALUE">>]}]},
                            {action, add}]},
                   {<<"Updated">>, [{value, [{<<"N">>,
                                        list_to_binary(integer_to_list(123456))}]},
                            {action, put}]},
                   {<<"TTL">>, [{value, [{<<"N">>,
                                        list_to_binary(integer_to_list(180))}]},
                            {action, put}]}]},
            {expected, [{<<"Updated">>, [{value, [{<<"N">>, list_to_binary(integer_to_list(123456))}]}]}]}],
        500),
    io:format("done ~p~n", [Id]),
    Father ! {done, Id}.

receive_all(0) ->
    pass;
receive_all(Times) ->
    receive
        {done, Id} ->
            io:format("Done ~p: ~p left~n", [Id, Times-1]),
            receive_all(Times-1);

        R ->
            io:format("Received ~p~n", [R])
    end.
