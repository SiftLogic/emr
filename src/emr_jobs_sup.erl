%% Each Map-Reduce job has its own supervision tree
-module(emr_jobs_sup).

-behaviour(supervisor).

%% API
-export([add_job/3,
         remove_job/1,
         start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Spec = {emr_job_worker,
            {emr_job_worker, start_link, []},
            temporary, 1000, worker, [emr_job_worker]},
    {ok, { {simple_one_for_one, 5, 10}, [Spec]} }.

add_job(JobName, MapFun, TupleSize) ->
    %%    Spec = {{emr_job_worker, binary_to_atom(JobName, utf8)},
    %%            {emr_job_worker, start_link, [JobName, MapFun, TupleSize]},
    %%            transient, 1000, supervisor, [emr_job_worker]},
    try supervisor:start_child(?MODULE, [JobName, MapFun, TupleSize]) of
        %%
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            lager:info("already started: ~p ~p", [Pid, JobName]),
            {ok, Pid};
        {error, already_present} ->
            Pid = gproc:lookup_pid({n,l,JobName}),
            supervisor:terminate_child(?MODULE, Pid);
        OtherError ->
            lager:error("Failed to start job ~p ~p", [JobName, OtherError]),
            OtherError
    catch
        E:M ->
            lager:error("Failed to start job ~p ~p", [JobName, {E,M}]),
            {error, {E, M}}
    end.

remove_job(JobName) ->
    T = supervisor:terminate_child(?MODULE, {emr_job_sup, JobName}),
    D = supervisor:delete_child(?MODULE, {emr_job_sup, JobName}),
    lager:info("T: ~p D: ~p", [T,D]),
    ok.
