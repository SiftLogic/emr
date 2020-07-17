-module(emr_job_worker).

-behaviour(gen_server).

-export([start_link/3]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([map_fun_/3]).
%%-export([reduce_fun_/3]).

-define(SERVER, ?MODULE).

-record(state, {name :: binary(), reply_to, workers = [], map_fun, tuple_size, empty, accum = #{}}).

start_link(JobName, MapFun, TupleSize) ->
    gen_server:start_link({via, gproc, {n, l, <<JobName/binary, "_worker">>}}, ?MODULE, [JobName, MapFun, TupleSize], []).

init([JobName, MapFun, TupleSize]) ->
    process_flag(trap_exit, true),
    Empty = list_to_tuple(lists:duplicate(TupleSize, 0)),
    State = #state{
               name = JobName,
               map_fun = MapFun,
               tuple_size = TupleSize,
               empty = Empty
              },
    {ok, State}.

handle_call(finalize, _From, #state{name = _JobName, workers = [], accum = Accum} = State) ->
    %% lager:info("~p Reply with Accum and stop process", [_JobName]),
    {stop, normal, {ok, Accum}, State};
handle_call(finalize, From, #state{name = _JobName} = State) ->
    %% lager:info("~p Not yet complete, so wait", [_JobName]),
    {noreply, State#state{reply_to = From}};
handle_call({queue, Data}, _From,
            #state{map_fun = MapFun, workers = Workers} = State) ->
    {Ref, Pid} = spawn_monitor(?MODULE, map_fun_, [Data, MapFun, []]),
    {reply, ok, State#state{workers = [{Ref, Pid} | Workers]}};
handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast({reduce, Key, AddValues},
            #state{tuple_size = TSize, empty = Empty, accum = Acc} = State)
  when tuple_size(AddValues) =:= TSize ->
    OldValues = maps:get(Key, Acc, Empty),
    NewValues = add_tuples_(AddValues, OldValues),
    {noreply, State#state{accum = maps:put(Key, NewValues, Acc)}};
handle_cast(stop, #state{} = State) ->
    {stop, normal, State};
handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, Pid, {normal, Intermediary}},
            #state{name = _JobName, workers = Workers,
                   empty = Empty, tuple_size = TupleSize,
                   accum = Acc, reply_to = ReplyTo} = State) ->
    NewState = case lists:member({Pid, Ref}, Workers) of
                   false ->
                       State;
                   true ->
                       %% lager:info("Map process ~p for job ~p completed", [Pid, _JobName]),
                       State#state{
                         workers = lists:delete({Pid, Ref}, Workers),
                         accum = reduce_(Intermediary, Empty, TupleSize, Acc)
                        }
               end,
    case NewState#state.workers of
        [] when ReplyTo =/= undefined ->
            gen_server:reply(ReplyTo, {ok, NewState#state.accum}),
            {stop, normal, NewState};
        _ ->
            {noreply, NewState}
    end;
handle_info(_Info, #state{} = State) ->
    lager:info("Unknown info: ~p ~p", [_Info, State]),
    {noreply, State}.

terminate(_Reason, #state{} = State) ->
    %%lager:info("Got terminate: ~p ~p", [_Reason, State]),
    ok.

code_change(_OldVsn, #state{} = State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

map_fun_([], _MapFun, Acc) ->
    exit({normal, Acc});
map_fun_([Row | Rest], MapFun, Acc) ->
    map_fun_(Rest, MapFun, [MapFun(Row) | Acc]).

reduce_([], _Empty, _TupleSize, Acc) ->
    Acc;
reduce_([{Key, AddValues} | Rest], Empty, TupleSize, Acc)
  when tuple_size(AddValues) =:= TupleSize ->
    OldValues = maps:get(Key, Acc, Empty),
    NewValues = add_tuples_(AddValues, OldValues),
    reduce_(Rest, Empty, TupleSize, maps:put(Key, NewValues, Acc)).

%% only supports up to 25 elements to add
%% generated using (with a little replacement afterwards)
%% for x in $(seq 1 25); do
%%   echo -n "add_tuples_({";
%%   for y in $(seq 1 $x); do
%%     echo -n "X${y},";
%%   done;
%%   echo -n "}, {";
%%   for y in $(seq 1 $x); do
%%     echo -n "Y${y},";
%%   done;
%%   echo -n "}";
%%   echo -n ") ->";
%%   echo "  ";
%%   echo -n "    {";
%%   for y in $(seq 1 $x); do
%%     echo -n "X${y} + Y${y}, ";
%%   done;
%%   echo -n "};";
%%   echo "";
%% done
add_tuples_({X1}, {Y1}) ->
    {X1 + Y1};
add_tuples_({X1,X2}, {Y1,Y2}) ->
    {X1 + Y1, X2 + Y2};
add_tuples_({X1,X2,X3}, {Y1,Y2,Y3}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3};
add_tuples_({X1,X2,X3,X4}, {Y1,Y2,Y3,Y4}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4};
add_tuples_({X1,X2,X3,X4,X5}, {Y1,Y2,Y3,Y4,Y5}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5};
add_tuples_({X1,X2,X3,X4,X5,X6}, {Y1,Y2,Y3,Y4,Y5,Y6}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6};
add_tuples_({X1,X2,X3,X4,X5,X6,X7}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10,X11}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10,Y11}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10, X11 + Y11};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10,X11,X12}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10,Y11,Y12}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10, X11 + Y11, X12 + Y12};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10,X11,X12,X13}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10,Y11,Y12,Y13}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10, X11 + Y11, X12 + Y12, X13 + Y13};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10,X11,X12,X13,X14}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10,Y11,Y12,Y13,Y14}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10, X11 + Y11, X12 + Y12, X13 + Y13, X14 + Y14};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10,X11,X12,X13,X14,X15}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10,Y11,Y12,Y13,Y14,Y15}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10, X11 + Y11, X12 + Y12, X13 + Y13, X14 + Y14, X15 + Y15};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10,X11,X12,X13,X14,X15,X16}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10,Y11,Y12,Y13,Y14,Y15,Y16}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10, X11 + Y11, X12 + Y12, X13 + Y13, X14 + Y14, X15 + Y15, X16 + Y16};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10,X11,X12,X13,X14,X15,X16,X17}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10,Y11,Y12,Y13,Y14,Y15,Y16,Y17}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10, X11 + Y11, X12 + Y12, X13 + Y13, X14 + Y14, X15 + Y15, X16 + Y16, X17 + Y17};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10,X11,X12,X13,X14,X15,X16,X17,X18}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10,Y11,Y12,Y13,Y14,Y15,Y16,Y17,Y18}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10, X11 + Y11, X12 + Y12, X13 + Y13, X14 + Y14, X15 + Y15, X16 + Y16, X17 + Y17, X18 + Y18};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10,X11,X12,X13,X14,X15,X16,X17,X18,X19}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10,Y11,Y12,Y13,Y14,Y15,Y16,Y17,Y18,Y19}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10, X11 + Y11, X12 + Y12, X13 + Y13, X14 + Y14, X15 + Y15, X16 + Y16, X17 + Y17, X18 + Y18, X19 + Y19};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10,X11,X12,X13,X14,X15,X16,X17,X18,X19,X20}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10,Y11,Y12,Y13,Y14,Y15,Y16,Y17,Y18,Y19,Y20}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10, X11 + Y11, X12 + Y12, X13 + Y13, X14 + Y14, X15 + Y15, X16 + Y16, X17 + Y17, X18 + Y18, X19 + Y19, X20 + Y20};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10,X11,X12,X13,X14,X15,X16,X17,X18,X19,X20,X21}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10,Y11,Y12,Y13,Y14,Y15,Y16,Y17,Y18,Y19,Y20,Y21}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10, X11 + Y11, X12 + Y12, X13 + Y13, X14 + Y14, X15 + Y15, X16 + Y16, X17 + Y17, X18 + Y18, X19 + Y19, X20 + Y20, X21 + Y21};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10,X11,X12,X13,X14,X15,X16,X17,X18,X19,X20,X21,X22}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10,Y11,Y12,Y13,Y14,Y15,Y16,Y17,Y18,Y19,Y20,Y21,Y22}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10, X11 + Y11, X12 + Y12, X13 + Y13, X14 + Y14, X15 + Y15, X16 + Y16, X17 + Y17, X18 + Y18, X19 + Y19, X20 + Y20, X21 + Y21, X22 + Y22};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10,X11,X12,X13,X14,X15,X16,X17,X18,X19,X20,X21,X22,X23}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10,Y11,Y12,Y13,Y14,Y15,Y16,Y17,Y18,Y19,Y20,Y21,Y22,Y23}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10, X11 + Y11, X12 + Y12, X13 + Y13, X14 + Y14, X15 + Y15, X16 + Y16, X17 + Y17, X18 + Y18, X19 + Y19, X20 + Y20, X21 + Y21, X22 + Y22, X23 + Y23};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10,X11,X12,X13,X14,X15,X16,X17,X18,X19,X20,X21,X22,X23,X24}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10,Y11,Y12,Y13,Y14,Y15,Y16,Y17,Y18,Y19,Y20,Y21,Y22,Y23,Y24}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10, X11 + Y11, X12 + Y12, X13 + Y13, X14 + Y14, X15 + Y15, X16 + Y16, X17 + Y17, X18 + Y18, X19 + Y19, X20 + Y20, X21 + Y21, X22 + Y22, X23 + Y23, X24 + Y24};
add_tuples_({X1,X2,X3,X4,X5,X6,X7,X8,X9,X10,X11,X12,X13,X14,X15,X16,X17,X18,X19,X20,X21,X22,X23,X24,X25}, {Y1,Y2,Y3,Y4,Y5,Y6,Y7,Y8,Y9,Y10,Y11,Y12,Y13,Y14,Y15,Y16,Y17,Y18,Y19,Y20,Y21,Y22,Y23,Y24,Y25}) ->
    {X1 + Y1, X2 + Y2, X3 + Y3, X4 + Y4, X5 + Y5, X6 + Y6, X7 + Y7, X8 + Y8, X9 + Y9, X10 + Y10, X11 + Y11, X12 + Y12, X13 + Y13, X14 + Y14, X15 + Y15, X16 + Y16, X17 + Y17, X18 + Y18, X19 + Y19, X20 + Y20, X21 + Y21, X22 + Y22, X23 + Y23, X24 + Y24, X25 + Y25}.
