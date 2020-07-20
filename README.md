emr
=====

A small local-node map-reduce framework for chunked aggregations.

A process is spawned for each chunk being aggregated 

Build
-----

    $ rebar3 compile

Usage
-----

Map-reduce where reduces uses simple addition.
The simple version always returns a map

```erlang
MapFun = fun({X,Y,Z,V1,V2,V3}) -> {{X,Y,Z}, {V1,V2,V3}} end.
{ok, C} = emr:start(<<"test1">>, MapFun, 3).
ok = C([{a,b,c,1,1,1},{a,b,c,1,1,1},{c,d,e,3,4,5},{a,b,c,5,5,5}]).                                
ok = C([{a,b,c,1,1,1},{a,b,c,1,1,1},{c,d,e,3,4,5},{a,b,c,5,5,5}]).
ok = C([{a,b,c,1,1,1},{a,b,c,1,1,1},{c,d,e,3,4,5},{a,b,c,5,5,5}]).
ok = C([{a,b,c,1,1,1},{a,b,c,1,1,1},{c,d,e,3,4,5},{a,b,c,5,5,5}]).
C(finalize).
{ok,#{{a,b,c} => {28,28,28},{c,d,e} => {12,16,20}}}
``` 

Map-reduce with contrived reducer which keeps track of max Date
and adds two value columns

```erlang
MapFun = fun({X,Y,D1,V1,V2}) -> {{X,Y}, {D1,V1,V2}} end.
ReduceFun = fun({Key, {Date, Val1, Val2} = R}, Acc) ->
                 case lists:keytake(Key,1,Acc) of
                    false ->
                        [{Key, R} | Acc];
                    {value, {Key, {D0,V1,V2} = Old}, Acc1} when Date > D0 ->
                        [{Key, {Date, Val1+V1, Val2+V2}} | Acc1];
                    {value, {Key, {D0, V1, V2} = Old}, Acc1} ->
                        [{Key, {D0, Val1+V1, Val2+V2}} | Acc1]
                end
              end.
{ok, C} = emr:start(<<"test1">>, MapFun, ReduceFun, []).
C([{a,b,{2020,1,1},1,1},{a,b,{2020,1,1},1,1},{c,d,{2020,1,2},4,5},{a,b,{2020,1,3},5,5}]).
C([{a,b,{2020,2,1},1,1},{a,b,{2020,1,1},1,1},{c,d,{2020,1,2},4,5},{a,b,{2020,1,10},5,5}]).
C(finalize).
{ok,#{{a,b,c} => {28,28,28},{c,d,e} => {12,16,20}}}
``` 
