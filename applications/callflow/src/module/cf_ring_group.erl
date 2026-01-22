%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2011-2022, 2600Hz
%%% @doc
%%% @author Karl Anderson
%%% @author James Aimonetti
%%% @author SIPLABS LLC (Ilya Ashchepkov)
%%% @end
%%%-----------------------------------------------------------------------------
-module(cf_ring_group).

-behaviour(gen_cf_action).

-include("callflow.hrl").
-include_lib("kazoo_amqp/src/api/kapi_dialplan.hrl").

-export([handle/2]).

-ifdef(TEST).
-export([weighted_random_sort/1
        ,resolve_endpoint_ids/2
        ,get_endpoints/2
        ]).
-endif.

-type group_weight() :: 1..100.

-type attempt_result() :: 'stop' |
                          'continue' |
                          'no_endpoints' |
                          'fail'.

-type endpoint() :: {kz_term:ne_binary(), kz_json:object()}.
-type endpoints() :: [endpoint()].

-type weighted_endpoint() :: {integer(), {kz_term:ne_binary(), kz_json:object()}}.
-type weighted_endpoints() :: [weighted_endpoint()].

-type delay() :: non_neg_integer().
-type timeout_t() :: non_neg_integer().
-type endpoint_intermediate() :: {kz_term:ne_binary()  %% group
                                 ,kz_term:ne_binary()  %% id
                                 ,delay()              %% delay
                                 ,timeout_t()            %% timeout
                                 ,group_weight()       %% weight
                                 ,kz_term:api_object() %% member obj
                                 }.
-type endpoint_intermediates() :: [endpoint_intermediate()].

%%%------------------------------------------------------------------------------
%%% Private logging helpers
%%%------------------------------------------------------------------------------

-spec log_start_context(kz_json:object(), kapps_call:call()) -> ok.
log_start_context(Data, Call) ->
    CallId         = safe_bin(kapps_call:call_id(Call)),
    AccountId      = safe_bin(kapps_call:account_id(Call)),
    AuthorizingId  = safe_bin(kapps_call:authorizing_id(Call)),
    Inception      = kapps_call:inception(Call),

    Strategy       = strategy(Data),
    Repeats        = repeats(Data),
    Timeout        = kz_json:get_integer_value(<<"timeout">>, Data, ?DEFAULT_TIMEOUT_S),
    Ringback       = kz_json:get_ne_binary_value(<<"ringback">>, Data, 'undefined'),
    IgnoreForward  = kz_json:get_binary_boolean(<<"ignore_forward">>, Data, <<"true">>),
    FailOnReject   = kz_json:is_true(<<"fail_on_single_reject">>, Data, 'undefined'),

    lager:debug(
      "ring_group start: call_id=~s account_id=~s authorizing_id=~s inception=~p",
      [CallId, AccountId, AuthorizingId, Inception]
    ),
    lager:debug(
      "ring_group opts: strategy=~s repeats=~p timeout=~p ringback=~p ignore_forward=~p fail_on_single_reject=~p",
      [Strategy, Repeats, Timeout, Ringback, IgnoreForward, FailOnReject]
    ),
    ok.

%% Guardrails for very long binaries in logs
-spec safe_bin(kz_term:api_ne_binary()) -> binary().
safe_bin('undefined') -> <<"undefined">>;
safe_bin(Bin) when is_binary(Bin) ->
    truncate_bin(Bin, 64);
safe_bin(Other) ->
    kz_term:to_binary(Other).

-spec truncate_bin(binary(), non_neg_integer()) -> binary().
truncate_bin(Bin, Max) when byte_size(Bin) =< Max ->
    Bin;
truncate_bin(Bin, Max) ->
    <<Front:Max/binary, _/binary>> = Bin,
    <<Front/binary, "...">>.

%%%------------------------------------------------------------------------------
%%% New lightweight format helpers (debug-only use)
%%%------------------------------------------------------------------------------

%% Accept [{Id, Member}] OR [{Weight, {Id, Member}}]
-spec fmt_members([term()]) -> iolist().
fmt_members(List) ->
    Pairs = [normalize_pair(E) || E <- List],
    lists:join(", ", [fmt_member(Id, Member) || {Id, Member} <- Pairs]).

-spec normalize_pair(term()) -> {kz_term:ne_binary() | iolist(), kz_json:object()}.
%% 1) Weighted shape FIRST so it doesn't get shadowed
normalize_pair({_Weight, {Id, Member}}) ->
    {Id, Member};
%% 2) Plain {Id, Member}, but only when Member is NOT a 2-tuple (to avoid catching weighted)
normalize_pair({Id, Member}) when not is_tuple(Member) ->
    {Id, Member};
normalize_pair({Id, Member}) when is_tuple(Member), tuple_size(Member) =/= 2 ->
    {Id, Member};
%% 3) Fallback – keep logging resilient
normalize_pair(Other) ->
    {io_lib:format("~p", [Other]), kz_json:new()}.

%% Defensive formatter (unchanged logic)
-spec fmt_member(kz_term:ne_binary() | iolist(), term()) -> iolist().
fmt_member(Id, Member0) ->
    Member = case is_list(Member0) orelse is_tuple(Member0) of
                 true  -> Member0;
                 false -> kz_json:new()
             end,
    Delay   = kz_json:get_integer_value(<<"delay">>,   Member, 0),
    Timeout = kz_json:get_integer_value(<<"timeout">>, Member, 20),
    Weight  = kz_json:get_integer_value(<<"weight">>,  Member, 'undefined'),
    Type    = kz_json:get_ne_binary_value(<<"endpoint_type">>, Member, <<"device">>),
    io_lib:format("~ts(type=~ts delay=~p timeout=~p weight=~p)",
                  [Id, Type, Delay, Timeout, Weight]).

-spec fmt_delays(kz_json:objects()) -> iolist().
fmt_delays(Endpoints) ->
    lists:join(", ", [io_lib:format("~p", [kz_json:get_integer_value(<<"Endpoint-Delay">>, E, 0)])
                      || E <- Endpoints]).


%%------------------------------------------------------------------------------
%% @doc Entry point for this module, attempts to call an endpoint as defined
%% in the Data payload.  Returns continue if fails to connect or
%% stop when successful.
%% @end
%%------------------------------------------------------------------------------
-spec handle(kz_json:object(), kapps_call:call()) -> 'ok'.
handle(Data, Call) ->
    log_start_context(Data, Call),
    repeat(Data, maybe_set_alert(Data, Call), repeats(Data)).

-spec maybe_set_alert(kz_json:object(), kapps_call:call()) -> kapps_call:call().
maybe_set_alert(Data, Call) ->
    AlertPath = custom_alert_path(kapps_call:inception(Call)),
    case kz_json:get_ne_binary_value([<<"ringtones">>, AlertPath], Data) of
        'undefined' -> Call;
        Alert ->
            lager:debug("setting alert to ~s", [Alert]),
            kapps_call:kvs_store(<<"Override-Ringtone">>, Alert, Call)
    end.

-spec custom_alert_path(kz_term:api_ne_binary()) -> kz_term:ne_binary().
custom_alert_path('undefined') -> <<"internal">>;
custom_alert_path(_Inception) -> <<"external">>.

-spec repeat(kz_json:object(), kapps_call:call(), non_neg_integer()) -> 'ok'.
repeat(_Data, Call, 0) ->
    cf_exe:continue(Call);
repeat(Data, Call, N) ->
    Next = case get_endpoints(Data, Call) of
               [] ->
                   lager:notice("ring group has no endpoints, moving to next callflow element"),
                   'no_endpoints';
               Endpoints ->
                   attempt_endpoints(Endpoints, Data, Call)
           end,
    repeat(Data, Call, N, Next).

-spec repeat(kz_json:object(), kapps_call:call(), pos_integer(), attempt_result()) -> 'ok'.
repeat(_Data, Call, _N, 'stop') ->
    cf_exe:stop(Call);
repeat(Data, Call, N, 'continue') ->
    repeat(Data, Call, N-1);
repeat(_Data, Call, _N, 'no_endpoints') ->
    cf_exe:continue(Call);
repeat(_Data, _Call, _N, 'fail') ->
    'ok'.

-spec attempt_endpoints(kz_json:objects(), kz_json:object(), kapps_call:call()) ->
          'stop' | 'fail' | 'continue'.
attempt_endpoints(Endpoints, Data, Call) ->
    FailOnSingleReject = kz_json:is_true(<<"fail_on_single_reject">>, Data, 'undefined'),
    Timeout = kz_json:get_integer_value(<<"timeout">>, Data, ?DEFAULT_TIMEOUT_S),
    Strategy = freeswitch_strategy(Data),
    Ringback = kz_json:get_ne_binary_value(<<"ringback">>, Data),
    IgnoreForward = kz_json:get_binary_boolean(<<"ignore_forward">>, Data, <<"true">>),

    lager:info("attempting ring group of ~b endpoints with strategy ~s", [length(Endpoints), Strategy]),
    case kapps_call_command:b_bridge(Endpoints, Timeout, Strategy, <<"true">>
                                    ,kz_media_util:media_path(Ringback, kapps_call:account_id(Call))
                                    ,'undefined', IgnoreForward, FailOnSingleReject, Call
                                    )
    of
        {'ok', _} ->
            lager:info("completed successful bridge to the ring group - call finished normally"),
            'stop';
        {'fail', _}=F ->
            case cf_util:handle_bridge_failure(F, Call) of
                'ok' -> lager:debug("bridge failure handled"), 'fail';
                'not_found' -> 'continue'
            end;
        {'error', 'timeout'} ->
            lager:debug("bridge timed out waiting for someone to answer"),
            'continue';
        {'error', _R} ->
            lager:info("error bridging to ring group: ~p"
                      ,[kz_json:get_value(<<"Error-Message">>, _R)]
                      ),
            'continue'
    end.

-spec get_endpoints(kz_json:object(), kapps_call:call()) -> kz_json:objects().
get_endpoints(Data, Call) ->
    Strategy = strategy(Data),
    %% Step 1: resolve (device-only), in member order (may already be ordered by strategy)
	Candidates = resolve_endpoint_ids(Data, Call),
	lager:debug("get_endpoints: resolved ~b candidate endpoint(s) [strategy=~s]: ~s",
	            [length(Candidates), Strategy, fmt_members(Candidates)]),

    %% Step 2: spawn builders (one per {Id, Member})
    Builders = start_builders_from_pairs(Candidates, Call),
    lager:debug("get_endpoints: started ~b builder process(es)", [length(Builders)]),

    %% Step 3: receive built endpoints (may be list or single)
    Built = receive_endpoints(Builders),
    lager:debug("get_endpoints: received ~b built endpoint object(s) (pre-ordering)",
                [length(Built)]),

    %% Step 4: maybe order by delay (simul only)
    Final = maybe_order_endpoints_by_delay(Strategy, Built),
    case Strategy of
        ?DIAL_METHOD_SIMUL ->
            lager:debug("get_endpoints: post-delay-ordering delays: [~s]", [fmt_delays(Final)]);
        _ ->
            ok
    end,

    %% Step 5: final summary
    lager:debug("get_endpoints: returning ~b endpoint object(s)", [length(Final)]),
    Final.

-spec maybe_order_endpoints_by_delay(kz_term:ne_binary(), kz_json:objects()) -> kz_json:objects().
maybe_order_endpoints_by_delay(?DIAL_METHOD_SIMUL, Endpoints) ->
    lager:debug("get_endpoints: ordering by Endpoint-Delay (ascending)"),
    Key = <<"Endpoint-Delay">>,
    F = fun(AObj, BObj) ->
                kz_json:get_integer_value(Key, AObj, 0) =<
                    kz_json:get_integer_value(Key, BObj, 0)
        end,
    lists:sort(F, Endpoints);
maybe_order_endpoints_by_delay(_, Endpoints) ->
    Endpoints.

-spec receive_endpoints(kz_term:pid_refs()) -> kz_json:objects().
receive_endpoints(Builders) ->
    %% Builders are in member order; since receive_endpoint_fold/2 prepends
    %% we foldr to build the list to maintain member order
    lists:foldr(fun receive_endpoint_fold/2, [], Builders).

-spec receive_endpoint_fold({pid(), reference()}, kz_json:objects()) -> kz_json:objects().
receive_endpoint_fold({Pid, Ref}, Acc) ->
    receive
        {Pid, EndpointId, {'ok', EP}} when is_list(EP) ->
            lager:debug("get_endpoints: recv ~b endpoint(s) for ~s", [length(EP), EndpointId]),
            EP ++ Acc;
        {Pid, EndpointId, {'ok', EP}} ->
            Delay = kz_json:get_integer_value(<<"Endpoint-Delay">>, EP, 0),
            Timeout = kz_json:get_integer_value(<<"Endpoint-Timeout">>, EP, 'undefined'),
            lager:debug("get_endpoints: recv single endpoint for ~s (delay=~p timeout=~p)",
                        [EndpointId, Delay, Timeout]),
            [EP | Acc];
        {Pid, EndpointId, Error} ->
            lager:info("get_endpoints: building endpoint ~s failed: ~p", [EndpointId, Error]),
            Acc;
        {'DOWN', Ref, 'process', Pid, 'normal'} ->
            Acc;
        {'DOWN', Ref, 'process', Pid, Reason} ->
            lager:info("get_endpoints: builder ~p exited abnormally: ~p", [Pid, Reason]),
            Acc
    end.

-spec start_builder(kz_term:ne_binary(), kz_json:object(), kapps_call:call()) -> {pid(), reference()}.
start_builder(EndpointId, Member, Call) ->
    kz_util:spawn_monitor(fun builder/4, [EndpointId, Member, Call, self()]).

-spec builder(kz_term:ne_binary(), kz_json:object(), kapps_call:call(), pid()) -> any().
builder(EndpointId, Member, Call, Parent) ->
    kz_util:put_callid(kapps_call:call_id(Call)),
    lager:debug("get_endpoints: attempting to build endpoint ~s", [EndpointId]),
    Parent ! {self(), EndpointId, kz_endpoint:build(EndpointId, Member, Call)}.

-spec start_builders_from_pairs([{kz_term:ne_binary(), kz_json:object()}], kapps_call:call()) ->
          kz_term:pid_refs().
start_builders_from_pairs(Pairs, Call) ->
    [start_builder(EndpointId, Member, Call) || {EndpointId, Member} <- Pairs].

-spec is_member_active(kz_json:object()) -> boolean().
is_member_active(Member) ->
    DisableUntil = kz_json:get_value(<<"disable_until">>, Member, 0),
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()) > DisableUntil.

-spec resolve_endpoint_ids(kz_json:object(), kapps_call:call()) -> endpoints().
resolve_endpoint_ids(Data, Call) ->
    resolve_endpoint_ids(Data, Call, kz_json:get_list_value(<<"endpoints">>, Data)).

-spec resolve_endpoint_ids(kz_json:object(), kapps_call:call(), kz_json:api_objects()) -> endpoints().
resolve_endpoint_ids(_Data, _Call, 'undefined') ->
    lager:warning("undefined endpoints in the ring group data: ~p", [_Data]),
    [];
resolve_endpoint_ids(_Data, _Call, []) ->
    lager:warning("no configured endpoints in the ring group data: ~p", [_Data]),
    [];
resolve_endpoint_ids(Data, Call, Members) ->
    FilteredMembers = lists:filter(fun is_member_active/1, Members),
    lager:debug("resolve_endpoint_ids: active members ~p", [FilteredMembers]),
    ResolvedEndpoints = resolve_endpoint_ids(FilteredMembers, [], Data, Call),
    lager:debug("resolve_endpoint_ids: expanded members into ~b intermediate endpoint(s)",
                [length(ResolvedEndpoints)]),

	FilteredEndpoints = [{Weight, {Id, kz_json:set_values(
	                                   [{<<"source">>, kz_term:to_binary(?MODULE)}
	                                   ,{<<"weight">>, Weight}],
	                                   Member)}}
	                     || {Type, Id, _Delay, _Timeout, Weight, Member} <- ResolvedEndpoints,
	                        Type =:= <<"device">>,
	                        Id =/= kapps_call:authorizing_id(Call)],
    lager:debug("resolve_endpoint_ids: filtered device endpoints (pre-order): ~p", [FilteredEndpoints]),
    Strategy = strategy(Data),
    Endpoints = order_endpoints(Strategy, FilteredEndpoints),
    lager:debug("resolve_endpoint_ids: ordered ~b endpoint(s) for strategy ~s: ~s",
                [length(Endpoints), Strategy, fmt_members(Endpoints)]),
    Endpoints.

-spec order_endpoints(kz_term:ne_binary(), weighted_endpoints()) -> endpoints().
order_endpoints(Method, Endpoints)
  when Method =:= ?DIAL_METHOD_SIMUL
       orelse Method =:= ?DIAL_METHOD_SINGLE ->
    [{Id, Endpoint} || {_Weight, {Id, Endpoint}} <- Endpoints];
order_endpoints(<<"weighted_random">>, Endpoints) ->
    %% Convert the randomly-ordered weighted list to plain {Id, Member} pairs
    [{Id, Endpoint} || {_W, {Id, Endpoint}} <- weighted_random_sort(Endpoints)].

-spec resolve_endpoint_ids(kz_json:objects(), endpoint_intermediates(), kz_json:object(), kapps_call:call()) ->
          endpoint_intermediates().
resolve_endpoint_ids(Members, EndpointIds, Data, Call) ->
    %% resolve the members in reverse order because `resolve_endpoint_id/1` prepends member
    %% endpoints onto the accumulator
    lists:foldr(fun(Member, Acc) ->
                        resolve_endpoint_id(Member, Acc, Data, Call)
                end
               ,EndpointIds
               ,Members
               ).

%% @doc check if there is already an endpoint with the same `EndpointId', `Delay', and
%% `Timeout' within the given list of `Endpoints'.
%% @end
-spec is_endpoint_resolved(kz_json:api_binary(), delay(), timeout_t(), endpoint_intermediates() | endpoint_intermediate()) -> boolean().
is_endpoint_resolved(EndpointId, Delay, Timeout, Endpoints) when is_list(Endpoints) ->
    F = fun(Endpoint) -> is_endpoint_resolved(EndpointId, Delay, Timeout, Endpoint) end,
    lists:any(F, Endpoints);
is_endpoint_resolved(Id, Delay, Timeout, {_EG, Id, Delay, Timeout, _EW, _EM}) -> 'true';
is_endpoint_resolved(Id, Delay, Timeout, {_EG, Id, Delay, Timeout, _EW}) -> 'true';
is_endpoint_resolved(_Id, _Delay, _Timeout, _) -> 'false'.

-spec resolve_endpoint_id(kz_json:object(), endpoint_intermediates(), kz_json:object(), kapps_call:call()) ->
          endpoint_intermediates().
resolve_endpoint_id(Member, EndpointIds, Data, Call) ->
    Id = kz_doc:id(Member),
    Delay = get_member_delay(Member),
    Timeout = get_member_timeout(Member),
    Type = kz_json:get_ne_binary_value(<<"endpoint_type">>, Member, <<"device">>),
    Weight = group_weight(Member, 20),
    case kz_term:is_empty(Id)
        orelse is_endpoint_resolved(Id, Delay, Timeout, EndpointIds)
        orelse Type
    of
        'true' -> EndpointIds;
        <<"group">> ->
            lager:info("member ~s is a group, merge the group's members", [Id]),
            GroupMembers = get_group_members(Member, Id, Weight, Data, Call),
            Ids = resolve_endpoint_ids(GroupMembers, EndpointIds, Data, Call),
            [{Type, Id, Delay, Timeout, 'undefined'}|Ids];
        <<"user">> ->
            lager:info("member ~s is a user, get all the user's endpoints", [Id]),
            get_user_endpoint_ids(Member, EndpointIds, Id, Weight, Call);
        <<"device">> ->
            lager:info("resolved device ~s", [Id]),
            [{Type, Id, Delay, Timeout, Weight, Member}|EndpointIds]
    end.

-spec get_user_endpoint_ids(kz_json:object(), endpoint_intermediates(), kz_term:ne_binary(), group_weight(), kapps_call:call()) ->
          endpoint_intermediates().
get_user_endpoint_ids(Member, EndpointIds, Id, GroupWeight, Call) ->
    Delay = get_member_delay(Member),
    Timeout = get_member_timeout(Member),
    lists:foldr(fun(EndpointId, Acc) ->
                        case is_endpoint_resolved(EndpointId, Delay, Timeout, Acc) of
                            'true' -> Acc;
                            'false' ->
                                lager:info("resolved user ~s device ~s", [Id, EndpointId]),
                                [{<<"device">>, EndpointId, Delay, Timeout, GroupWeight, Member} | Acc]
                        end
                end
               ,[{<<"user">>, Id, Delay, Timeout, 'undefined'} | EndpointIds]
               ,kz_attributes:owned_by(Id, <<"device">>, Call)
               ).

-spec get_group_members(kz_json:object(), kz_term:ne_binary(), group_weight(), kz_json:object(), kapps_call:call()) -> kz_json:objects().
get_group_members(Member, Id, GroupWeight, Data, Call) ->
    AccountDb = kapps_call:account_db(Call),
    case kz_datamgr:open_cache_doc(AccountDb, Id) of
        {'ok', JObj} ->
            maybe_order_group_members(GroupWeight, Member, JObj, Data);
        {'error', _R} ->
            lager:warning("unable to lookup members of group ~s: ~p", [Id, _R]),
            []
    end.

-spec maybe_order_group_members(group_weight(), kz_json:object(), kz_json:object(), kz_json:object()) ->
          kz_json:objects().
maybe_order_group_members(Weight, Member, JObj, Data) ->
    case strategy(Data) of
        ?DIAL_METHOD_SINGLE ->
            order_group_members(Weight, Member, JObj);
        _ ->
            unordered_group_members(Weight, Member, JObj)
    end.

-spec unordered_group_members(group_weight(), kz_json:object(), kz_json:object()) ->
          kz_json:objects().
unordered_group_members(Weight, Member, JObj) ->
    Endpoints = kz_json:get_json_value(<<"endpoints">>, JObj, kz_json:new()),
    kz_json:foldl(fun(Key, Endpoint, Acc) ->
                          [create_group_member(Key, Endpoint, Weight, Member) | Acc]
                  end
                 ,[]
                 ,Endpoints
                 ).

-spec order_group_members(group_weight(), kz_json:object(), kz_json:object()) -> kz_json:objects().
order_group_members(GroupWeight, Member, JObj) ->
    Endpoints = kz_json:get_json_value(<<"endpoints">>, JObj, kz_json:new()),
    GroupMembers =
        kz_json:foldl(fun(Key, Endpoint, Acc) ->
                              order_group_member_fold(Key, Endpoint, Acc, GroupWeight, Member)
                      end
                     ,orddict:new()
                     ,Endpoints
                     ),
    [V || {_, V} <- orddict:to_list(GroupMembers)].

-spec order_group_member_fold(kz_json:path(), kz_json:object(), orddict:orddict(), group_weight(), kz_json:object()) ->
          orddict:orddict().
order_group_member_fold(Key, Endpoint, Acc, GroupWeight, Member) ->
    case group_weight(Endpoint) of
        'undefined' ->
            lager:debug("endpoint ~s has no weight, removing from ordered group", [Key]),
            Acc;
        Weight ->
            GroupMember = create_group_member(Key, Endpoint, GroupWeight, Member),
            orddict:store(Weight, GroupMember, Acc)
    end.

-spec create_group_member(kz_term:ne_binary(), kz_json:object(), group_weight(), kz_json:object()) ->
          kz_json:object().
create_group_member(Key, Endpoint, GroupWeight, Member) ->
    DefaultDelay = get_member_delay(Member),
    DefaultTimeout = get_member_timeout(Member),
    kz_json:set_values([{<<"endpoint_type">>, kz_json:get_ne_binary_value(<<"type">>, Endpoint)}
                       ,{<<"id">>, Key}
                       ,{<<"delay">>, kz_json:get_integer_value(<<"delay">>, Endpoint, DefaultDelay)}
                       ,{<<"timeout">>, kz_json:get_integer_value(<<"timeout">>, Endpoint, DefaultTimeout)}
                       ,{<<"weight">>, GroupWeight}
                       ]
                      ,Member
                      ).

-spec weighted_random_sort(weighted_endpoints()) -> endpoints().
weighted_random_sort(Endpoints) ->
    _ = rand:seed('exsplus'),
    WeightSortedEndpoints = lists:sort(Endpoints),
    weighted_random_sort(WeightSortedEndpoints, []).

-spec weighted_random_sort(weighted_endpoints(), endpoints()) -> endpoints().
weighted_random_sort([_ | _] = ListWeight, Acc) ->
    [{Sum, _} | _] = ListInterval = set_intervals_on_weight(ListWeight, [], 0),
    Pivot = random_integer(Sum),
    {_W, {_Id, _Endpoint} = Element} = weighted_random_get_element(ListInterval, Pivot),
    ListNew = lists:delete(Element, ListWeight),
    weighted_random_sort(ListNew, [Element | Acc]);
weighted_random_sort([], Acc) ->
    Acc.

-spec set_intervals_on_weight(weighted_endpoints(), weighted_endpoints(), integer()) ->
          weighted_endpoints().
set_intervals_on_weight([{Weight, _}=E | Tail], Acc, Sum) ->
    set_intervals_on_weight(Tail, [{Weight + Sum, E} | Acc], Sum + Weight);
set_intervals_on_weight([], Acc, _Sum) ->
    Acc.

-spec weighted_random_get_element(weighted_endpoints(), integer()) -> weighted_endpoint().
weighted_random_get_element(List, Pivot) ->
    {_, {Weight, _}} = case [E || E={X, _} <- List, X =< Pivot] of
                           [] -> lists:last(List);
                           L -> hd(L)
                       end,
    ListFiltered = [E || E = {_, {W, _}} <- List, W =:= Weight],
    lists:nth(random_integer(length(ListFiltered)), ListFiltered).

-spec random_integer(integer()) -> integer().
random_integer(I) ->
    rand:uniform(I).

-spec repeats(kz_json:object()) -> pos_integer().
repeats(Data) ->
    max(1, kz_json:get_integer_value(<<"repeats">>, Data, 1)).

-spec group_weight(kz_json:object()) -> group_weight() | 'undefined'.
group_weight(Endpoint) ->
    group_weight(Endpoint, 'undefined').

-spec group_weight(kz_json:object(), Default) -> group_weight() | Default.
group_weight(Endpoint, Default) ->
    case kz_json:get_integer_value(<<"weight">>, Endpoint) of
        'undefined' -> Default;
        N when N < 1 -> 1;
        N when N > 100 -> 100;
        N -> N
    end.

-spec strategy(kz_json:object()) -> kz_term:ne_binary().
strategy(Data) ->
    kz_json:get_binary_value(<<"strategy">>, Data, ?DIAL_METHOD_SIMUL).

-spec freeswitch_strategy(kz_json:object()) -> kz_term:ne_binary().
freeswitch_strategy(Data) ->
    case strategy(Data) of
        ?DIAL_METHOD_SIMUL -> ?DIAL_METHOD_SIMUL;
        _ -> ?DIAL_METHOD_SINGLE
    end.

-spec get_member_delay(kz_json:object()) -> delay().
get_member_delay(MemberJObj) ->
    get_member_delay(MemberJObj, 0).

-spec get_member_delay(kz_json:object(), delay()) -> delay().
get_member_delay(MemberJObj, Default) ->
    kz_json:get_integer_value(<<"delay">>, MemberJObj, Default).

-spec get_member_timeout(kz_json:object()) -> timeout_t().
get_member_timeout(MemberJObj) ->
    get_member_timeout(MemberJObj, 20).

-spec get_member_timeout(kz_json:object(), timeout_t()) -> timeout_t().
get_member_timeout(MemberJObj, Default) ->
    kz_json:get_integer_value(<<"timeout">>, MemberJObj, Default).
