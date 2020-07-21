-module(emr_job_worker).

-behaviour(gen_server).

-export([start_link/3]).
-export([start_link/4]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([map_fun_/3]).
-export([map_reduce_fun_/4]).

-include("emr.hrl").

-define(SERVER, ?MODULE).

-record(state,
        {name                           :: binary(),
         max_workers    = ?MAX_WORKERS  :: pos_integer(),
         reply_to                       :: undefined | reference(),
         workers        = []            :: [] | list(pidref()),
         waiters        = queue:new()   :: queue:queue(),
         map_fun                        :: map_fun(),
         tuple_size                     :: undefined | pos_integer(),
         empty                          :: undefined | tuple(),
         reduce_fun                     :: undefined | reduce_fun(),
         reduce_acc                     :: undefined | reduce_accumulator(),
         accum                          :: any()
        }).

start_link(JobName, MapFun, TupleSize) ->
    gen_server:start_link({via, gproc, {n, l, <<JobName/binary, "_worker">>}}, ?MODULE, [JobName, MapFun, TupleSize], []).

start_link(JobName, MapFun, ReduceFun, ReduceAccumulator) ->
    gen_server:start_link({via, gproc, {n, l, <<JobName/binary, "_worker">>}}, ?MODULE, [JobName, MapFun, ReduceFun, ReduceAccumulator], []).

init([JobName, MapFun, TupleSize]) ->
    process_flag(trap_exit, true),
    MaxWorkers = application:get_env(emr, max_workers, ?MAX_WORKERS),
    Empty = list_to_tuple(lists:duplicate(TupleSize, 0)),
    ReduceFun = fun({Key, AddValues}, OAcc)
                      when tuple_size(AddValues) =:= TupleSize ->
                        OldValues = maps:get(Key, OAcc, Empty),
                        NewValues = add_tuples_(AddValues, OldValues),
                        maps:put(Key, NewValues, OAcc)
                end,
    State = #state{
               name = JobName,
               max_workers = MaxWorkers,
               map_fun = MapFun,
               reduce_fun = ReduceFun,
               reduce_acc = #{},
               tuple_size = TupleSize,
               empty = Empty,
               accum = #{}
              },
    {ok, State};
init([JobName, MapFun, ReduceFun, ReduceAccumulator]) ->
    process_flag(trap_exit, true),
    MaxWorkers = application:get_env(emr, max_workers, ?MAX_WORKERS),
    State = #state{
               name = JobName,
               max_workers = MaxWorkers,
               map_fun = MapFun,
               reduce_fun = ReduceFun,
               reduce_acc = ReduceAccumulator,
               accum = ReduceAccumulator
              },
    {ok, State}.

handle_call(finalize, _From, #state{name = _JobName, workers = [], waiters = {[], []}, accum = Accum} = State) ->
    %% lager:info("~p Reply with Accum and stop process", [_JobName]),
    {stop, normal, {ok, Accum}, State};
handle_call(finalize, From, #state{name = _JobName} = State) ->
    %% lager:info("~p Not yet complete, so wait", [_JobName]),
    {noreply, State#state{reply_to = From}};
handle_call({queue, Data}, From, #state{} = State) ->
    queue_or_wait(From, Data, State);
handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(stop, #state{} = State) ->
    {stop, normal, State};
handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, Pid, {normal, Intermediary}}, #state{} = State0) ->
    State1 = do_remove_worker({Ref, Pid}, State0),
    State2 = do_accum(Intermediary, State1),
    State3 = maybe_dequeue(State2),
    reply_after_accum(State3);
handle_info(_Info, #state{} = State) ->
    lager:info("Unknown info: ~p ~p", [_Info, State]),
    {noreply, State}.

terminate(_Reason, #state{} = _State) ->
    %%lager:info("Got terminate: ~p ~p", [_Reason, _State]),
    ok.

code_change(_OldVsn, #state{} = State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_spawn(Data, #state{map_fun = MapFun, reduce_fun = ReduceFun, reduce_acc = ReduceAccum}) ->
    spawn_monitor(?MODULE, map_reduce_fun_, [Data, MapFun, ReduceFun, ReduceAccum]).


map_fun_([], _MapFun, Acc) ->
    exit({normal, Acc});
map_fun_([Row | Rest], MapFun, Acc) ->
    map_fun_(Rest, MapFun, [MapFun(Row) | Acc]).

map_reduce_fun_([], _MapFun, _ReduceFun, Acc) ->
    timer:sleep(timer:seconds(5)),
    exit({normal, Acc});
map_reduce_fun_([Row | Rest], MapFun, ReduceFun, Acc) ->
    NewAcc = ReduceFun(MapFun(Row), Acc),
    map_reduce_fun_(Rest, MapFun, ReduceFun, NewAcc).

reduce_(InMap, ReduceFun, Acc) when is_map(InMap) ->
    maps:fold(fun(K, V, InAcc) ->
                      ReduceFun({K,V}, InAcc)
              end,
              Acc, InMap);
reduce_([], _ReduceFun, Acc) ->
    Acc;
reduce_([Item | Rest], ReduceFun, Acc) ->
    reduce_(Rest, ReduceFun, ReduceFun(Item, Acc)).

queue_or_wait(_From, Data, #state{workers = Workers, max_workers = MaxWorkers} = State)
  when length(Workers) < MaxWorkers ->
    {Pid, Ref} = do_spawn(Data, State),
    {reply, ok, State#state{workers = [{Ref, Pid} | Workers]}};
queue_or_wait(From, Data, #state{waiters = Waiters} = State) ->
    %% blocks the caller and will reply when the queued
    %% set of data is assigned to a worker slot
    NWaiters = queue:in({From, Data}, Waiters),
    {noreply, State#state{waiters = NWaiters}}.

do_remove_worker(RefPid, #state{workers = Workers} = State) ->
    State#state{workers = lists:delete(RefPid, Workers)}.

do_accum(Intermediary, #state{reduce_fun = ReduceFun,
                              accum = Acc} = State) ->
    State#state{accum = reduce_(Intermediary, ReduceFun, Acc)}.

reply_after_accum(#state{workers = [], reply_to = ReplyTo, accum = Accum} = State)
  when ReplyTo =/= undefined ->
    gen_server:reply(ReplyTo, {ok, Accum}),
    {stop, normal, State};
reply_after_accum(#state{} = State) ->
    {noreply, State}.

%% Where there are waiting callers and available slots
%% a new process is spawned and the waiting caller
%% will receive an 'ok' response
maybe_dequeue(#state{waiters = Queue, workers = Workers, max_workers = MaxWorkers} = State) ->
    case queue:out(Queue) of
        {empty, Queue} ->
            State;
        {{value, {From, Data}}, NQueue} ->
            NState0 = State#state{waiters = NQueue},
            {Pid, Ref} = do_spawn(Data, NState0),
            NState = NState0#state{workers = [{Ref, Pid} | Workers]},
            gen_server:reply(From, ok),
            case length(Workers) + 1 < MaxWorkers of
                true ->
                    maybe_dequeue(NState);
                false ->
                    NState
            end
    end.

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
