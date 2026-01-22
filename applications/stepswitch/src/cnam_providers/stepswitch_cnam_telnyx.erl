%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2018, Voxter Communications
%%% @doc CNAM lookup using Telnyx
%%% @end
%%%-----------------------------------------------------------------------------
-module(stepswitch_cnam_telnyx).

-export([request/2]).

-include("stepswitch.hrl").

-define(DEFAULT_URL, <<"https://api.telnyx.com/v2/number_lookup/{{phone_number}}?type=caller-name">>).

-define(HTTP_ACCEPT_HEADER, "application/json").
-define(HTTP_CONTENT_TYPE, "application/json").

-spec request(kz_term:ne_binary(), kz_json:object()) -> kz_term:api_binary().
request(Number, JObj) ->
    Url = kz_term:to_list(get_http_url(JObj)),
    case kz_http:get(Url, get_http_headers(), get_http_options()) of
        {'ok', 404, _, _} ->
            lager:debug("cnam lookup for ~s returned 404", [Number]),
            'undefined';
        {'ok', Status, _, <<>>} ->
            lager:debug("cnam lookup for ~s returned as ~p and empty body", [Number, Status]),
            'undefined';
        {'ok', 200=Status, _, ResponseBody} ->
            lager:debug("cnam lookup for ~s returned ~p: ~s", [Number, Status, ResponseBody]),
            handle_ok_resp(ResponseBody);
        {'ok', Status, _, ResponseBody} ->
            lager:debug("cnam lookup for ~s returned ~p: ~s", [Number, Status, ResponseBody]),
            'undefined';
        {'error', _R} ->
            lager:debug("cnam lookup for ~s failed: ~p", [Number, _R]),
            'undefined'
    end.

-spec get_http_url(kz_json:object()) -> kz_term:ne_binary().
get_http_url(JObj) ->
    Template = kapps_config:get_binary(?CNAM_CONFIG_CAT, <<"http_url">>, ?DEFAULT_URL),
    {'ok', SrcUrl} = stepswitch_cnam:render(JObj, Template),
    iolist_to_binary(SrcUrl).

-spec get_http_headers() -> [{nonempty_string(), nonempty_string()}].
get_http_headers() ->
    Headers = [{"Accept", ?HTTP_ACCEPT_HEADER}
              ,{"User-Agent", ?HTTP_USER_AGENT}
              ,{"Content-Type", ?HTTP_CONTENT_TYPE}
              ],
    maybe_enable_auth(Headers).

-spec get_http_options() -> kz_term:proplist().
get_http_options() ->
    [{'connect_timeout', ?HTTP_CONNECT_TIMEOUT_MS}
    ,{'timeout', 1500}
    ].

-spec maybe_enable_auth([{nonempty_string(), nonempty_string()}]) ->
                               [{nonempty_string(), nonempty_string()}].
maybe_enable_auth(Props) ->
    Token = kapps_config:get_string(?CNAM_CONFIG_CAT, <<"http_token_auth_token">>, <<>>),
    case kz_term:is_empty(Token) of
        'true' -> Props;
        'false' -> [{"Authorization", lists:flatten(["Bearer ", Token])} | Props]
    end.

-spec handle_ok_resp(kz_term:ne_binary()) -> kz_term:api_binary().
handle_ok_resp(ResponseBody) ->
    JObj = kz_json:decode(ResponseBody),

    %% Pull the nested caller_name in a type-tolerant way
    RawCallerName = kz_json:get_value([<<"data">>, <<"caller_name">>, <<"caller_name">>], JObj),

    %% Normalize "absent" caller names: undefined, null (atom), <<"null">> (binary), or non-binaries
    CallerName =
        case RawCallerName of
            'WIRELESS CALLER'   -> 'undefined';
            'Unknown'           -> 'undefined';
            'undefined'         -> 'undefined';
            null                -> 'undefined';
            <<"null">>          -> 'undefined';
            B when is_binary(B) -> B;            %% actual string value
            _Other              -> 'undefined'
        end,

    case CallerName of
        'undefined' ->
            %% No CNAM available: fall back to the calling number.
            %% Prefer national_format if present; else use E.164 phone_number.
            case kz_json:get_binary_value([<<"data">>, <<"national_format">>], JObj) of
                'undefined' ->
                    case kz_json:get_binary_value([<<"data">>, <<"phone_number">>], JObj) of
                        'undefined' ->
                            lager:debug("cnam lookup: no caller name; no number fields to fall back to"),
                            'undefined';
                        E164 ->
                            lager:debug("cnam lookup: no caller name; falling back to E.164 ~p", [E164]),
                            E164
                    end;
                Nat ->
                    lager:debug("cnam lookup: no caller name; falling back to national format ~p", [Nat]),
                    Nat
            end;
        Name ->
            %% We have a real name; log and truncate to 18 chars (CNAM spec)
            lager:debug("cnam lookup: CallerName(raw)=~p (~p bytes)", [Name, size(Name)]),
            maybe_truncate(Name)
    end.

-spec maybe_truncate(binary()) -> binary().
maybe_truncate(CallerName) when size(CallerName) > 18 ->
    lager:debug("cnam lookup: truncating CallerName from ~p (~p bytes) to 18",
                [CallerName, size(CallerName)]),
    kz_binary:truncate_right(CallerName, 18);
maybe_truncate(CallerName) ->
    lager:debug("cnam lookup: CallerName not truncated (~p bytes)", [size(CallerName)]),
    CallerName.
