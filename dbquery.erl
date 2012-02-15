-module(dbquery).

-export([rquery/2]).

-include("../../support/include/dllog.hrl").
-include("../include/dbschema.hrl").

%%% in epoch conversions, 62167219200 is Jan 1 1970 0:0:0 in seconds
-define(EPOCHV, 62167219200).

-define(SV4(A, B, C, D), ((A-$0)*1000+(B-$0)*100+(C-$0)*10+(D-$0))).
-define(SV2(A, B),       ((A-$0)*10+(B-$0))).

-record(parse_elems, {
          get,
          where,
          sort, 
          range
         }).

% Query object state
-record(qost, {
          oid,
          onum,
          od,
          md,
          olink
         }).

% Query state
-record(qst, {
          clique,
          res = [],
          where_vars = [],
          exceptions = []
         }).

rquery(What, Onumlist) ->
    PE = transform_query(What, #parse_elems{}),
    mnesia:activity(
      async_dirty,
      fun() ->
              run_query(Onumlist, PE)
      end, [], mnesia_frag).





transform_query([], PE) -> PE;
transform_query([{get, G} | T], PE) ->
    transform_query(T, PE#parse_elems{get = G});
transform_query([{where, W} | T], PE) ->
    transform_query(T, PE#parse_elems{where = W});
transform_query([{sort, S} | T], PE) ->
    transform_query(T, PE#parse_elems{sort = S});
transform_query([{range, R} | T], PE) ->
    transform_query(T, PE#parse_elems{range = R});
transform_query([_ | T], PE) ->
    transform_query(T, PE).

run_query(Onumlist, PE) ->
    Clique = clique:my_clique(),
    run_query(Onumlist, PE, #qst{clique = Clique}).

run_query([], _, Qst) ->
    {ok, Qst#qst.res, Qst#qst.exceptions};
run_query([Onum | T], Pe, Qst) ->
    case objdb:q_get_od(Onum) of
        not_found ->
            E = [{Onum, not_found} | Qst#qst.exceptions],
            run_query(T, Pe, Qst#qst{exceptions = E});
        OD when record(OD, od) ->
            case where(
                   #qost{onum = Onum, od = OD, oid = {Qst#qst.clique, Onum}}, 
                   Pe#parse_elems.where,
                       Qst#qst.where_vars) of
                {ok, true, _, Continue, Qost1, NVL} ->
                    case rquery(Qost1, Pe#parse_elems.get, []) of
                        {ok, Query_result, Sysdat} ->
                            Nres = 
                                [{{Qst#qst.clique, Onum}, 
                                  {Query_result, Sysdat}} | Qst#qst.res],
                            Tval = if Continue == true -> T;
                                      true -> []
                                   end,
                            
%                           io:format("*** Query_result ~p~nSysdat ~p~nNres ~p~n",
%                                     [Query_result, Sysdat, Nres]),

                            run_query(
                              Tval, Pe, Qst#qst{res = Nres, 
                                                where_vars = NVL});
                        Err = {error, _} ->
                            Err
                    end;
                {ok, false, _, Continue, _Qost1, NVL} ->
                    Ex = ["Where clause failed for " ++ 
                          integer_to_list(Onum) | Qst#qst.exceptions],
                    Tval = if Continue == true -> T;
                              true -> []
                           end,
                    run_query(
                      Tval, Pe, Qst#qst{exceptions = Ex, where_vars = NVL});
                Err ->
                    Err
            end
    end.

where(Qost, undefined, Vars) ->
    {ok, true, true, true, Qost, Vars};
where(Qost, Clause, Vars) ->
    case catch where_bool(Qost, {Vars, []}, Clause) of
        {Qost1, {val, bool, Bool}, {NVars, _}} ->
            {ok, true, true, true, Qost1, NVars};
        Err ->
            Err
    end.

where_bool(Qost, Vars, Clause) ->
    {Qost1, Val, Nvars} = w(Qost, Vars, Clause),
    w(Qost1, Nvars, Val).


%%%
%%% w - main where clause function
%%%

%%% neg - negate an evaluation
w(Qost, Vars, {op, neg, {val, bool, true}}) ->
    {Qost, {val, bool, false}, Vars};
w(Qost, Vars, {op, neg, {val, bool, false}}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, neg, C1={val, _, _}}) ->
    {Qost1, C2, NVars} = w(Qost, Vars, C1),
    w(Qost1, NVars, {op, neg, C2});

%%% land - logical and - make sure we evaluate first arg before second
w(Qost, Vars, {op, land, C={val, bool, true}, {val, bool, true}}) ->
    {Qost, C, Vars};
w(Qost, Vars, {op, land, {val, bool, _}, {val, bool, _}}) ->
    {Qost, {val, bool, false}, Vars};
w(Qost, Vars, {op, land, C={val, bool, false}, _}) ->
    {Qost, C, Vars};
w(Qost, Vars, {op, land, C1={val, bool, true}, C2}) ->
    {Qost1, C3, NVars} = w(Qost, Vars, C2),
    w(Qost1, NVars, {op, land, C1, C3});
w(Qost, Vars, {op, land, C1, C2}) ->
    {Qost1, C3, NVars} = w(Qost, Vars, C1),
    w(Qost1, NVars, {op, land, C3, C2});

%%% lor - logical or - make sure we evaluate first arg before second
w(Qost, Vars, {op, lor, C={val, bool, false}, {val, bool, false}}) ->
    {Qost, C, Vars};
w(Qost, Vars, {op, lor, {val, bool, _}, {val, bool, _}}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, lor, C={val, bool, true}, _}) ->
    {Qost, C, Vars};
w(Qost, Vars, {op, lor, C1={val, bool, false}, C2}) ->
    {Qost1, C3, NVars} = w(Qost, Vars, C2),
    w(Qost1, NVars, {op, lor, C1, C3});
w(Qost, Vars, {op, lor, C1, C2}) ->
    {Qost1, C3, NVars} = w(Qost, Vars, C1),
    w(Qost1, NVars, {op, lor, C3, C2});

%%% eq - equal
w(Qost, Vars, {op, eq, {val, T, V}, {val, T, V}}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, eq, {val, T, _}, {val, T, _}}) ->
    {Qost, {val, bool, false}, Vars};

%%% gt - greater than
w(Qost, Vars, {op, gt, {val, T, V1}, {val, T, V2}}) when V1 > V2 ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, gt, {val, T, _}, {val, T, _}}) ->
    {Qost, {val, bool, false}, Vars};

%%% gte - greater than or equal
w(Qost, Vars, {op, gte, {val, T, V1}, {val, T, V2}}) when V1 >= V2 ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, gte, {val, T, _}, {val, T, _}}) ->
    {Qost, {val, bool, false}, Vars};

%%% lt - less than
w(Qost, Vars, {op, lt, {val, T, V1}, {val, T, V2}}) when V1 < V2 ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, lt, {val, T, _}, {val, T, _}}) ->
    {Qost,{val, bool, false}, Vars};

%%% lte - less than or equal
w(Qost, Vars, {op, lte, {val, T, V1}, {val, T, V2}}) when V1 =< V2 ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, lte, {val, T, _}, {val, T, _}}) ->
    {Qost, {val, bool, false}, Vars};

%%% catch null for the above comparison ops
w(Qost, Vars, {op, Op, {val, _, _}, {val, null, null}}) 
  when Op == eq; Op == gt; Op == gte; Op == lt; Op == lte ->
    {Qost, {val, bool, false}, Vars};
w(Qost, Vars, {op, Op, {val, null, null}, {val, _, _}}) 
  when Op == eq; Op == gt; Op == gte; Op == lt; Op == lte ->
    {Qost, {val, bool, false}, Vars};

%%% add - addition
w(Qost, Vars, {op, add, {val, str, V1}, {val, str, V2}}) ->
    {Qost, {val, str, string:concat(V1, V2)}, Vars};
w(Qost, Vars, {op, add, {val, int, V1}, {val, int, V2}}) ->
    {Qost, {val, int, V1 + V2}, Vars};
w(Qost, Vars, {op, add, {val, float, V1}, {val, float, V2}}) ->
    {Qost, {val, float, V1 + V2}, Vars};
w(Qost, Vars, {op, add, {val, time_date, V1}, {val, int, V2}}) ->
    G = calendar:datetime_to_gregorian_seconds(V1) + V2,
    {Qost, {val, time_date, calendar:gregorian_seconds_to_datetime(G)}, Vars};
w(Qost, Vars, {op, add, {val, epoch, V1}, {val, int, V2}}) ->
    {Qost, {val, epoch, V1 + V2}, Vars};
w(Qost, Vars, {op, add, V1={val, date, _}, {val, int, V2}}) ->
    G = calendar:datetime_to_gregorian_seconds(to_time_date(V1)) + V2,
    {Qost, 
     {val, date, to_date(calendar:gregorian_seconds_to_datetime(G))}, Vars};

%%% sub - subtraction
w(Qost, Vars, {op, sub, {val, int, V1}, {val, int, V2}}) ->
    {Qost, {val, int, V1 - V2}, Vars};
w(Qost, Vars, {op, sub, {val, float, V1}, {val, float, V2}}) ->
    {Qost, {val, float, V1 - V2}, Vars};
w(Qost, Vars, {op, sub, {val, time_date, V1}, {val, int, V2}}) ->
    G = calendar:datetime_to_gregorian_seconds(V1) - V2,
    {Qost, {val, time_date, calendar:gregorian_seconds_to_datetime(G)}, Vars};
w(Qost, Vars, {op, sub, {val, epoch, V1}, {val, int, V2}}) ->
    {Qost, {val, epoch, V1 - V2}, Vars};
w(Qost, Vars, {op, sub, V1={val, date, _}, {val, int, V2}}) ->
    G = calendar:datetime_to_gregorian_seconds(to_time_date(V1)) - V2,
    {Qost, 
     {val, date, to_date(calendar:gregorian_seconds_to_datetime(G))}, Vars};
w(Qost, Vars, {op, sub, {val, time_date, V1}, {val, time_date, V2}}) ->
    {Qost,
     {val, int, calendar:datetime_to_gregorian_seconds(V1) -
      calendar:datetime_to_gregorian_seconds(V2)}, Vars};

%%% mult - multiply
w(Qost, Vars, {op, mult, {val, int, V1}, {val, int, V2}}) ->
    {Qost, {val, int, V1 * V2}, Vars};
w(Qost, Vars, {op, mult, {val, float, V1}, {val, float, V2}}) ->
    {Qost, {val, float, V1 * V2}, Vars};

%%% divd - divide
w(Qost, Vars, {op, divd, {val, int, V1}, {val, int, V2}}) ->
    {Qost, {val, int, V1 div V2}, Vars};
w(Qost, Vars, {op, divd, {val, float, V1}, {val, float, V2}}) ->
    {Qost, {val, float, V1 / V2}, Vars};

%%% mod - modulus
w(Qost, Vars, {op, mod, {val, int, V1}, {val, int, V2}}) ->
    {Qost, {val, int, V1 rem V2}, Vars};

%%% catch null for the above math ops
w(Qost, Vars, {op, Op, V={val, _, _}, {val, null, null}}) when Op == add;
    Op == sub; Op == mult ->
    {Qost, V, Vars};
w(Qost, Vars, {op, Op, {val, null, null}, V={val, _, _}}) when Op == add;
    Op == mult ->
    {Qost, V, Vars};

%%% biand - bitwise and
w(Qost, Vars, {op, biand, {val, int, V1}, {val, int, V2}}) ->
    {Qost, {val, int, V1 band V2}, Vars};

%%% bior - bitwise or
w(Qost, Vars, {op, bior, {val, int, V1}, {val, int, V2}}) ->
    {Qost, {val, int, V1 bor V2}, Vars};

%%% bixor - bitwise xor
w(Qost, Vars, {op, bixor, {val, int, V1}, {val, int, V2}}) ->
    {Qost, {val, int, V1 bxor V2}, Vars};

%%% catch null for the above bit ops
w(Qost, Vars, {op, Op, V={val, int, _}, {val, null, null}}) when Op == biand;
    Op == bior; Op == bixor ->
    {Qost, V, Vars};
w(Qost, Vars, {op, Op, {val, null, null}, V={val, int, _}}) when Op == biand;
    Op == bior; Op == bixor ->
    {Qost, V, Vars};
w(Qost, Vars, {op, Op, V={val, null, null}, {val, null, null}}) when Op == biand;
    Op == bior; Op == bixor ->
    {Qost, V, Vars};

%%% bicom - bitwise compliment
w(Qost, Vars, {op, bicom, {val, int, V}}) ->
    {Qost, {val, int, bnot V}, Vars};
w(Qost, Vars, {op, biand, V={val, null, null}}) ->
    {Qost, V, Vars};

%%% bisl - bitwise shift left
w(Qost, Vars, {op, bisl, {val, int, V1}, {val, int, V2}}) ->
    {Qost, {val, int, V1 bsl V2}, Vars};

%%% bisr - bitwise shift right
w(Qost, Vars, {op, bisr, {val, int, V1}, {val, int, V2}}) ->
    {Qost, {val, int, V1 bsr V2}, Vars};

%%% eqi - equals string (case insensitive)
w(Qost, Vars, {op, eqi, {val, str, V1}, {val, str, V2}}) ->
    w(Qost, Vars, {op, eq, {val, str, to_upr(V1)}, {val, str, to_upr(V2)}});

%%% cstr - contains string
w(Qost, Vars, {op, cstr, {val, str, V1}, {val, str, V2}}) ->
    V = case string:str(V1, V2) of
            0 -> false;
            _ -> true
        end,
    {Qost, {val, bool, V}, Vars};

%%% cstri - contains string (case insensitive)
w(Qost, Vars, {op, cstri, {val, str, V1}, {val, str, V2}}) ->
    w(Qost, Vars, {op, cstr, {val, str, to_upr(V1)}, {val, str, to_upr(V2)}});

%%% bstr - begins with string
w(Qost, Vars, {op, bstr, {val, str, V1}, {val, str, V2}}) ->
    V = case string:str(V1, V2) of
            1 -> true;
            _ -> false
        end,
    {Qost, {val, bool, V}, Vars};

%%% bstri - begins with string (case insensitive)
w(Qost, Vars, {op, bstri, {val, str, V1}, {val, str, V2}}) ->
    w(Qost, Vars, {op, bstr, {val, str, to_upr(V1)}, {val, str, to_upr(V2)}});

%%% estr - ends with string
w(Qost, Vars, {op, estr, {val, str, V1}, {val, str, V2}}) ->
    P = ((string:len(V1) - string:len(V2)) + 1),
    V = case string:rstr(V1, V2) of
            P when P > 0 -> true;
            _ -> false
        end,
    {Qost, {val, bool, V}, Vars};

%%% estri - ends with string (case insensitive)
w(Qost, Vars, {op, estri, {val, str, V1}, {val, str, V2}}) ->
    w(Qost, Vars, {op, estr, {val, str, to_upr(V1)}, {val, str, to_upr(V2)}});

%%% regex - regular expression matches
w(Qost, Vars, {op, regex, {val, str, V1}, {val, str, V2}}) ->
    V = case regexp:match(V1, V2) of
            {match, Start, Length} ->
                string:substr(V1, Start, Length);
            nomatch ->
                "";
            {error, Err} ->
                throw({error, {invalid_where_regex, Err}})
        end,
    {Qost, {val, str, V}, Vars};

%%% od - get object data
w(Qost, Vars, {op, od, {val, str, Key}}) ->
    {Qost1, Vt, Vv} = 
        case dbquery_get_od(Qost, Key) of
            not_found ->
                {null, null};
            {error, Err} ->
                throw({error, {invalid_od_lookup, Err}});
            {_, string, Val} ->
                {str, Val};
            {_, int64, Val} ->
                {int, Val};
            {_, epoch_time, Val} ->
                {epoch, Val};
            _ ->
                {null, null}
        end,
    {Qost1, {val, Vt, Vv}, Vars};

%%% md - get meta data
w(Qost, Vars, {op, md, {val, str, Key}}) ->
    % finishing this (so that it doesn't drop everything but the first
    % result) is a TODO
    {Qost1, R} = dbquery_get_md(Qost, Key),
    {Vt, Vv} = 
        case R of
            [] ->                          {null, null};
            [{_, string, Val} | _] ->      {str, Val};
            [{_, int32,  Val} | _] ->      {int, Val};
            [{_, int64,  Val} | _] ->      {int, Val};
            [{_, float,  Val} | _] ->      {float, Val};
            [{_, bool,   Val} | _] ->      {bool, Val};
            [{_, time_date, Val} | _] ->   {time_date, Val};
            [{_, epoch_time, Val} | _] ->  {epoch, Val};
            [{_, date, Val} | _] ->        {date, Val};
            [{_, time_period, Val} | _] -> {time_period, Val};
            _ ->                           {null, null}
        end,
    {Qost1, {val, Vt, Vv}, Vars};

%%% acl - get acl entry

%%% bf - get blobfrag entry

%%% is_str - value is string
w(Qost, Vars, {op, is_str, {val, str, _}}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, is_str, {val, _, _}}) ->
    {Qost, {val, bool, false}, Vars};

%%% is_int - value is integer
w(Qost, Vars, {op, is_int, {val, int, _}}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, is_int, {val, _, _}}) ->
    {Qost, {val, bool, false}, Vars};

%%% is_float - value is float
w(Qost, Vars, {op, is_float, {val, float, _}}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, is_float, {val, _, _}}) ->
    {Qost, {val, bool, false}, Vars};

%%% is_bool - value is bool
w(Qost, Vars, {op, is_bool, {val, bool, _}}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, is_bool, {val, _, _}}) ->
    {Qost, {val, bool, false}, Vars};

%%% is_time_date - value is time_date
w(Qost, Vars, {op, is_time_date, {val, time_date, _}}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, is_time_date, {val, _, _}}) ->
    {Qost, {val, bool, false}, Vars};

%%% is_epoch - value is epoch
w(Qost, Vars, {op, is_epoch, {val, epoch, _}}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, is_epoch, {val, _, _}}) ->
    {Qost, {val, bool, false}, Vars};

%%% is_date - value is date
w(Qost, Vars, {op, is_date, {val, date, _}}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, is_date, {val, _, _}}) ->
    {Qost, {val, bool, false}, Vars};

%%% is_time_period - value is time_period
w(Qost, Vars, {op, is_time_period, {val, time_period, _}}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, is_time_period, {val, _, _}}) ->
    {Qost, {val, bool, false}, Vars};

%%% is_null - value is null
w(Qost, Vars, {op, is_null, {val, null, null}}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {op, is_null, {val, _, _}}) ->
    {Qost, {val, bool, false}, Vars};

%%% to_time_date - convert to time_date
w(Qost, Vars, 
  {op, to_time_date, 
   {val, str, 
    [Y1, Y2, Y3, Y4, MO1, MO2, D1, D2, H1, H2, MI1, MI2, S1, S2]}}) ->
    Year =    ?SV4(Y1, Y2, Y3, Y4),
    Month =   ?SV2(MO1, MO2),
    Day =     ?SV2(D1, D2),
    Hour =    ?SV2(H1, H2),
    Minute =  ?SV2(MI1, MI2),
    Second =  ?SV2(S1, S2),
    {Qost, {val, time_date, 
            {{Year, Month, Day}, {Hour, Minute, Second}}}, Vars};

w(Qost, Vars, {op, to_time_date, V={val, time_date, _}}) ->
    {Qost, V, Vars};
w(Qost, Vars, {op, to_time_date, V={val, T, _}})
    when T == epoch; T == date; T == int ->
    {Qost, to_time_date(V), Vars};

%%% to_epoch - convert to epoch
w(Qost, Vars, {op, to_epoch, {val, int, V}}) ->
    {Qost, {val, epoch, V}, Vars};
w(Qost, Vars, {op, to_epoch, V={val, epoch, _}}) ->
    {Qost, V, Vars};
w(Qost, Vars, {op, to_epoch, V={val, T, _}}) when T == time_date; T == date ->
    {Qost, to_epoch(V), Vars};

%%% to_date - convert to date
w(Qost, Vars, {op, to_date, {val, str, [Y1, Y2, Y3, Y4, M1, M2, D1, D2]}}) ->
    Year =   ?SV4(Y1, Y2, Y3, Y4),
    Month =  ?SV2(M1, M2),
    Day =    ?SV2(D1, D2),
    {Qost, {val, date, {Year, Month, Day}}, Vars};
w(Qost, Vars, {op, to_date, V={val, date, _}}) ->
    {Qost, V, Vars};
w(Qost, Vars, {op, to_date, V={val, T, _}})
    when T == time_date; T == epoch; T == int ->
    {Qost, to_date(V), Vars};

%%% to_time_period - convert to time_period
w(Qost, Vars, {op, to_time_period, {val, str, V}}) when length(V) > 14 ->
    [Y1, Y2, Y3, Y4, MO1, MO2, D1, D2, H1, H2, MI1, MI2, S1, S2 | Dur] = V,
    Year =   ?SV4(Y1, Y2, Y3, Y4),
    Month =  ?SV2(MO1, MO2),
    Day =    ?SV2(D1, D2),
    Hour =   ?SV2(H1, H2),
    Min =    ?SV2(MI1, MI2),
    Sec =    ?SV2(S1, S2),
    Durval = list_to_integer(Dur),
    {Qost, 
     {val, time_date, {{Year, Month, Day}, {Hour, Min, Sec}, Durval}}, Vars};
w(Qost, Vars, {op, to_time_period, {val, time_date, {D, T}}, {val, int, S}}) ->
    {Qost, {val, time_date, {D, T, S}}, Vars};
w(Qost, Vars, {op, to_time_period, V={val, epoch, _}, {val, int, S}}) ->
    {_, _, {D, T}} = to_time_date(V),
    {Qost, {val, time_date, {D, T, S}}, Vars};
w(Qost, Vars, {op, to_time_period, {val, date, V}, {val, int, S}}) ->
    {Qost, {val, time_date, {V, {0, 0, 0}, S}}, Vars};

%%% now - get current time
w(Qost, Vars, {op, now}) ->
    {Qost, {val, time_date, calendar:universal_time()}, Vars};

%%% *var - get variable
w(Qost, Vars={_, LVars}, {op, var, Key={val, _, _}}) ->
    case var_find(LVars, Key) of
        false ->
            w(Qost, Vars, {op, rvar, Key});
        Val ->
            {Qost, Val, Vars}
    end;
w(Qost, Vars={_, LVars}, {op, lvar, Key={val, _, _}}) ->
    case var_find(LVars, Key) of
        false ->
            {Qost, {val, null, null}, Vars};
        Val ->
            {Qost, Val, Vars}
    end;
w(Qost, Vars={RVars, _}, {op, rvar, Key={val, _, _}}) ->
    case var_find(RVars, Key) of
        false ->
            {Qost, {val, null, null}, Vars};
        Val ->
            {Qost, Val, Vars}
    end;

%%% set_*var - set a variable for later use
w(Qost, {RVars, LVars}, {op, set_lvar, Key={val, _, _}, Val={val, _, _}}) ->
    {Qost, {val, bool, true}, {RVars, [{Key, Val} | LVars]}};
w(Qost, {RVars, LVars}, {op, set_rvar, Key={val, _, _}, Val={val, _, _}}) ->
    {Qost, {val, bool, true}, {[{Key, Val} | RVars], LVars}};

%%% stop_rec - stop recursion
w(_Qost, Vars, {op, stop_rec, {val, bool, B}}) ->
    throw({ok, B, false, true, Vars});
w(Qost, Vars, {op, stop_rec, C1}) ->
    {Qost1, C2, NVars} = w(Qost, Vars, C1),
    w(Qost1, NVars, {op, stop_rec, C2});

%%% stop_query - stop query
w(_Qost, Vars, {op, stop_query, {val, bool, B}}) ->
    throw({ok, B, false, false, Vars});
w(Qost, Vars, {op, stop_query, C1}) ->
    {Qost1, C2, NVars} = w(Qost, Vars, C1),
    w(Qost1, NVars, {op, stop_query, C2});

%%% date conversions for operations - must be before op errors
w(Qost, Vars, {op, Op, TD={val, time_date, _}, E={val, epoch, _}})
    when Op == eq; Op == gt; Op == gte; Op == lt; Op == lte; Op == sub ->
    w(Qost, Vars, {op, Op, TD, to_time_date(E)});
w(Qost, Vars, {op, Op, E={val, epoch, _}, TD={val, time_date, _}})
    when Op == eq; Op == gt; Op == gte; Op == lt; Op == lte; Op == sub ->
    w(Qost, Vars, {op, Op, to_time_date(E), TD});
w(Qost, Vars, {op, Op, TD={val, time_date, _}, D={val, date, _}})
    when Op == eq; Op == gt; Op == gte; Op == lt; Op == lte ->
    w(Qost, Vars, {op, Op, to_date(TD), D});
w(Qost, Vars, {op, Op, D={val, date, _}, TD={val, time_date, _}})
    when Op == eq; Op == gt; Op == gte; Op == lt; Op == lte ->
    w(Qost, Vars, {op, Op, D, to_date(TD)});
w(Qost, Vars, {op, Op, E={val, epoch, _}, D={val, date, _}})
    when Op == eq; Op == gt; Op == gte; Op == lt; Op == lte ->
    w(Qost, Vars, {op, Op, to_date(E), D});
w(Qost, Vars, {op, Op, D={val, date, _}, E={val, epoch, _}})
    when Op == eq; Op == gt; Op == gte; Op == lt; Op == lte ->
    w(Qost, Vars, {op, Op, D, to_date(E)});

w(Qost, Vars, {op, sub, TD={val, time_date, _}, D={val, date, _}}) ->
    w(Qost, Vars, {op, sub, TD, to_time_date(D)});
w(Qost, Vars, {op, sub, D={val, date, _}, TD={val, time_date, _}}) ->
    w(Qost, Vars, {op, sub, to_time_date(D), TD});
w(Qost, Vars, {op, sub, E={val, epoch, _}, D={val, date, _}}) ->
    w(Qost, Vars, {op, sub, to_time_date(E), to_time_date(D)});
w(Qost, Vars, {op, sub, D={val, date, _}, E={val, epoch, _}}) ->
    w(Qost, Vars, {op, sub, to_time_date(D), to_time_date(E)});

%%% op errors - must before op evaluation
w(_, _, {op, Op, C={val, _, _}}) ->
    throw({error, invalid_where_operation, {op, Op, C}});
w(_, _, {op, Op, C1={val, _, _}, C2={val, _, _}}) ->
    throw({error, invalid_where_operation, {op, Op, C1, C2}});

%%% default operation evaluation
w(Qost, Vars, {op, Op, C1}) ->
    {Qost1, C2, NVars} = w(Qost, Vars, C1),
    w(Qost1, NVars, {op, Op, C2});
w(Qost, Vars, {op, Op, C1={val, _, _}, C2}) ->
    {Qost1, C3, NVars} = w(Qost, Vars, C2),
    w(Qost1, NVars, {op, Op, C1, C3});
w(Qost, Vars, {op, Op, C1, C2={val, _, _}}) ->
    {Qost1, C3, NVars} = w(Qost, Vars, C1),
    w(Qost1, NVars, {op, Op, C3, C2});
w(Qost, Vars, {op, Op, C1, C2}) ->
    {Qost1, C3, NVars1} = w(Qost, Vars, C1),
    {Qost2, C4, NVars2} = w(Qost1, NVars1, C2),
    w(Qost2, NVars2, {op, Op, C3, C4});

%%% values - this forces a value into type bool
w(Qost, Vars, {val, str, ""}) ->
    {Qost, {val, bool, false}, Vars};
w(Qost, Vars, {val, str, _}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {val, int, 0}) ->
    {Qost, {val, bool, false}, Vars};
w(Qost, Vars, {val, int, _}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {val, float, 0}) ->
    {Qost, {val, bool, false}, Vars};
w(Qost, Vars, {val, float, 0.0}) ->
    {Qost, {val, bool, false}, Vars};
w(Qost, Vars, {val, float, _}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, C={val, bool, _}) ->
    {Qost, C, Vars};
w(Qost, Vars, {val, time_date, _}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {val, epoch, 0}) ->
    {Qost, {val, bool, false}, Vars};
w(Qost, Vars, {val, epoch, _}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {val, date, _}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {val, time_period, _}) ->
    {Qost, {val, bool, true}, Vars};
w(Qost, Vars, {val, null, _}) ->
    {Qost, {val, bool, false}, Vars};

%%% error on anything else
w(_, _, Err) ->
    throw({error, invalid_where_evaluation, Err}).



rquery(Qost, [], Res) ->
    Other_data = gen_other_data(Qost),
    %{ok, Other_data, Res};
    {ok, Res, Other_data};
rquery(Qost, [oid | T], Res) ->
    %rquery(Qost, T, [{oid, objservgen:build_oid(Qost#qost.onum)} | Res]);
    rquery(Qost, T, [{oid, Qost#qost.oid} | Res]);
rquery(Qost, [childlist | T], Res) ->
    {Qost1, CL} = dbquery_get_childlist(Qost),
    rquery(Qost1, T, [{childlist, CL} | Res]);
rquery(Qost, [parentlist | T], Res) ->
    {Qost1, PL} = dbquery_get_parentlist(Qost),
    rquery(Qost1, T, [{parentlist, PL} | Res]);
rquery(Qost, [blob | T], Res) ->
    {Qost1, Blob} = dbquery_get_blob(Qost),
    rquery(Qost1, T, [{blob, Blob}  | Res]);
rquery(Qost, [{blobchunk, Pos, Nbytes} | T], Res) ->
    {Qost1, BC} = dbquery_get_blobchunk(Qost, Pos, Nbytes),
    rquery(Qost1, T, [{blobchunk, BC} | Res]);
rquery(Qost, [all_md | T], Res) ->
    {Qost1, MD} = dbquery_get_allmd(Qost),
    rquery(Qost1, T, [{all_md, MD} | Res]);
rquery(Qost, [all_od | T], Res) ->
    {Qost1, OD} = dbquery_get_allod(Qost),
    rquery(Qost1, T, [{all_od, OD} | Res]);
rquery(Qost, [all_bf | T], Res) ->
    {Qost1, BL} = dbquery_get_bloblist(Qost),
    rquery(Qost1, T, [{bloblist, BL} | Res]);
rquery(Qost, [{md, Key} | T], Res) ->
    case dbquery_get_md(Qost, Key) of
        {Qost1, []} -> rquery(Qost1, T, Res);
        {Qost1, L} when is_list(L) -> rquery(Qost1, T, [{md, L} | Res])
    end;
rquery(Qost, [{od, Key} | T], Res) ->
    case dbquery_get_od(Qost, Key) of
        Err = {error, _} ->
            Err;
        {Qost1, Nada} when Nada == undefined; Nada == not_found ->
            rquery(Qost1, T, Res);
        {Qost1, OD} ->
            rquery(Qost1, T, [{od, OD} | Res])
    end;
rquery(Qost, [all | T], Res) ->
    {Qost1, All} = dbquery_get_all(Qost),
    rquery(Qost1, T, [{all, All} | Res]);
rquery(Qost, [_ | T], Res) ->
    rquery(Qost, T, Res).



dbquery_get_childlist(Qost) when Qost#qost.olink == undefined ->
    CL = objdb:q_get_childlist(Qost#qost.onum),
    {Qost#qost{olink = CL}, CL};
dbquery_get_childlist(Qost) ->
    {Qost, Qost#qost.olink}.



dbquery_get_parentlist(Qost) ->
    OD = Qost#qost.od,
    Inptr = case OD#od.inptr of
                undefined ->
                    [];
                L when is_list(L) ->
                    L;
                T = {_, _} ->
                    [T]
            end,
    {Qost, Inptr}.


dbquery_get_bloblist(Qost) ->
    % just one blob for an object, for now
    OD = Qost#qost.od,
    case OD#od.blob of
        undefined ->
            {Qost, []};
        B when record(B, blob) ->
            Frags = if is_binary(B#blob.frags) -> 
                            binary_to_term(B#blob.frags);
                       true ->
                            B#blob.frags
                    end,
            {Qost, {B#blob.sz, Frags}}
    end.

dbquery_get_blob(Qost) ->
    OD = Qost#qost.od,
    case OD#od.blob of
        undefined ->
            {Qost, {}};
        B when record(B, blob) ->
            {Qost, {B#blob.sz, B#blob.loc}}
    end.


dbquery_get_blobchunk(Qost, Pos, Nbytes) ->
    OD = Qost#qost.od,
    case OD#od.blob of
        undefined ->
            {Qost, {}};
        B when record(B, blob) ->
            {Qost, {B#blob.sz, B#blob.loc, Pos, Nbytes}}
    end.


dbquery_get_allmd(Qost) when Qost#qost.md == undefined ->
    MD = objdb:q_get_md(Qost#qost.onum),
    dbquery_get_allmd(Qost#qost{md = MD});
dbquery_get_allmd(Qost) ->
    {Qost, Qost#qost.md}.


dbquery_get_md(Qost, Key) when Qost#qost.md == undefined ->
    MD = objdb:q_get_md(Qost#qost.onum),
    dbquery_get_md(Qost#qost{md = MD}, Key);
dbquery_get_md(Qost, Key) when is_list(Key) ->
    dbquery_get_md(Qost, list_to_binary(Key));
dbquery_get_md(Qost, BKey) when is_binary(BKey) ->
    MD = Qost#qost.md,
    {Qost, [KTV || KTV = {K, _, _} <- MD, K == BKey]}.
                          

dbquery_get_allod(Qost) ->                        
    OD = Qost#qost.od,
    Blobsz = case OD#od.blob of
                 undefined -> 0;
                 B when record(B, blob) ->
                     B#blob.sz
             end,
    %%Owner = objdb:q_get_ownername(Qost#qost.onum),
    OID = Qost#qost.oid,
    Owner = objdb:q_get_ownername(Qost#qost.od),
    {Qost, 
     [{"name",      string,     objdb:unpack_str(OD#od.name)},
      %{"oid",       oid,        objservgen:build_oid(Qost#qost.onum)},
      {"oid",       oid,        Qost#qost.oid},
      {"type",      string,     objdb:unpack_str(OD#od.type)},
      {"owner",     string,     objdb:unpack_str(Owner)},
      {"size",      int64,      Blobsz},
      {"created",   epoch_time, OD#od.crts},
      {"modified",  epoch_time, OD#od.modts}]}.
     

dbquery_get_all(Qost) ->
    dbquery_get_all(
      [childlist, parentlist, all_bf, blob, all_md, all_od], Qost, []).

dbquery_get_all([], Qost, Res) ->
    {Qost, Res};
dbquery_get_all([N | T], Qost, Res) ->
    {Qost1, V} = 
        case N of
            childlist ->  dbquery_get_childlist(Qost);
            parentlist -> dbquery_get_parentlist(Qost);
            all_bf ->     dbquery_get_bloblist(Qost);
            blob ->       dbquery_get_blob(Qost);
            all_md ->     dbquery_get_allmd(Qost);
            all_od ->     dbquery_get_allod(Qost)
        end,
    dbquery_get_all(T, Qost1, [{N, V} | Res]).


dbquery_get_od(Qost, Key) ->
    get_od_elem(Qost, Key, Qost#qost.od).

get_od_elem(Q, "oid", OD) ->
    %{Q, {"oid", oid, objservgen:build_oid(Q#qost.onum)}};
    {Q, {"oid", oid, Q#qost.oid}};
get_od_elem(Q, "onum", OD) ->
    {Q, {"onum", int64, Q#qost.onum}};
get_od_elem(Q, "name", OD) ->
    Name = objdb:unpack_str(OD#od.name),
    {Q, {"name", string, Name}};
get_od_elem(Q, "owner", OD) ->
    %%Owner = objdb:q_get_ownername(Q#qost.onum),
    Owner = objdb:q_get_ownername(OD),
    {Q, {"owner", string, Owner}};
get_od_elem(Q, "type", OD) ->
    {Q, {"type", string, objdb:unpack_str(OD#od.type)}};
get_od_elem(Q, "size", OD) ->
    {Qost1, V} = dbquery_get_blob(Q),
    SZ = case V of
             {} -> 0;
             {N, _} -> N
         end,
    {Q, {"size", int64, SZ}};
get_od_elem(Q, "created", OD) ->
    {Q, {"created", epoch_time, OD#od.crts}};
get_od_elem(Q, "modified", OD) ->
    {Q, {"modified", epoch_time, OD#od.modts}};

get_od_elem(_, Field, _) -> {error, {unknown_field, Field}}.

gen_other_data(Qost) ->
    OD = Qost#qost.od,
    [{name, objdb:unpack_str(OD#od.name)},
     {sort, {OD#od.name, OD#od.crts}}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%
%%% utility functions
%%%

%%% convert to uppercase for case insensitive conversions
to_upr(Str) ->
    string:to_upper(Str).

%%% time/date/epoch conversions
%%% in epoch conversions, 62167219200 is Jan 1 1970 0:0:0 in seconds
to_time_date({val, int, V}) ->
    {val, time_date, calendar:gregorian_seconds_to_datetime(V + ?EPOCHV)};
to_time_date({val, epoch, V}) ->
    {val, time_date, calendar:gregorian_seconds_to_datetime(V + ?EPOCHV)};
to_time_date({val, date, V}) ->
    {val, time_date, {V, {0, 0, 0}}}.

to_epoch({val, time_date, V}) ->
    {val, epoch, calendar:datetime_to_gregorian_seconds(V) - ?EPOCHV};

to_epoch({val, date, V}) ->
    {val, epoch, calendar:datetime_to_gregorian_seconds({V, {0, 0, 0}}) -
     ?EPOCHV}.

to_date({val, int, V}) ->
    {D, _} = calendar:gregorian_seconds_to_datetime(V + ?EPOCHV),
    {val, date, D};
to_date({val, time_date, {V, _}}) ->
    {val, date, V};
to_date({val, epoch, V}) ->
    {D, _} = calendar:gregorian_seconds_to_datetime(V + ?EPOCHV),
    {val, date, D}.

%%% find value in list from key
var_find(List, Key) ->
    case lists:keysearch(Key, 1, List) of
        {value, {_, Val}} ->
            Val;
        false ->
            false
    end.



