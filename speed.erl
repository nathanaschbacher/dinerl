#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ebin -pa deps/lhttpc/ebin -smp enable -s timer -s inets -s ssl -s lhttpc -sname speed

-mode(compile).

main([STimes]) ->
    %ok = application:start(lhttpc),
    bench:setup(),
    Times = list_to_integer(STimes),
    bench:spawn_and_try(Times, Times).

