-module(emr).

-behaviour(gen_server).

%% Public API
-export([start/3,
         stop/1]).

%% gen_server callbacks
-export([start_link/0]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {jobs = []}).

-type job_name() :: binary().
-type key() :: any().
-type map_fun() :: fun((any()) -> {key(), tuple()}).
-type consumer() :: fun((complete | list(any())) -> ok | {ok, map()}).

-spec start(job_name(), map_fun(), pos_integer()) -> {ok, consumer()} | {error, job_already_exists}.
start(JobName, MapFun, TupleSize)
  when is_binary(JobName)
       andalso is_function(MapFun, 1)
       andalso TupleSize > 0 ->
    gen_server:call(?MODULE, {job_start, JobName, MapFun, TupleSize}).

-spec stop(job_name()) -> ok.
stop(JobName) ->
    gen_server:call(?MODULE, {job_stop, JobName}).

%%% Server

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    %% init for the manager/api process
    {ok, #state{}}.

handle_call({job_start, JobName, MapFun, TupleSize}, From, #state{jobs = Jobs} = State) ->
    case lists:keyfind(JobName, 1, Jobs) of
        false ->
            {Pid, ConsumerFun} = create_job_(JobName, MapFun, TupleSize),
            {reply, {ok, ConsumerFun}, State#state{jobs = [{JobName, Pid, From} | Jobs]}};
        _ ->
            {error, job_already_exists}
    end;
handle_call({job_stop, JobName}, _From, #state{jobs = Jobs} = State) ->
    case lists:keytake(JobName, 1, Jobs) of
        false ->
            try Pid = gproc:lookup_pid({n,l,<<JobName/binary, "_worker">>}),
                 gen_server:cast(Pid, stop)
            catch
                _:_ ->
                    ok
            end,
            {reply, {error, not_found}, State};
        {value, {JobName, Pid, _OFrom}, NJobs} ->
            gen_server:cast(Pid, stop),
            {reply, ok, State#state{jobs = NJobs}}
    end;
handle_call(_Request, _From, State) ->
    lager:error("Unknown call: ~p ~p ~p", [_Request, _From, State]),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    lager:error("Unknown cast: ~p ~p", [_Request, State]),
    {noreply, State}.

handle_info(_Info, State) ->
    lager:error("Unknown info: ~p ~p", [_Info, State]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

create_job_(JobName, MapFun, TupleSize) ->
    {ok, Pid} = emr_jobs_sup:add_job(JobName, MapFun, TupleSize),
    Consumer = create_consumer_(JobName),
    {Pid, Consumer}.

create_consumer_(JobName) ->
    Via = {via, gproc, {n, l, <<JobName/binary, "_worker">>}},
    fun(Data) when is_list(Data) ->
            %% Deliver the data to the worker for processing
            gen_server:call(Via, {queue, Data});
       (finalize) ->
            %% Wait for the results from the worker
            Res = gen_server:call(Via, finalize, infinity),
            gen_server:call(?MODULE, {job_stop, JobName}),
            Res
    end.
