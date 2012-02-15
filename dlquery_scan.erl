-module(dlquery_scan).

-export([scan/1]).

% [ {category, linenumber, symbol}, ...
%   {symbol, linenumber}, ...
%   {'$end', linenumber}, ...
%

scan(S) when is_list(S) ->
    scan(list_to_binary(S));
scan(Statement) when is_binary(Statement) ->
    top_scan(1,Statement,[]).


top_scan(Ln,<<>>,Acc) ->
    {ok,lists:reverse([{'$end',Ln} | Acc])};
top_scan(Ln,<<Sp:8, Rest/binary>>,Acc) when Sp == $\t; Sp == $ ->
    top_scan(Ln,Rest,Acc);
top_scan(Ln,<<Nl:8, Rest/binary>>,Acc) when Nl == $\n ; Nl == $\r ->
    top_scan(Ln + 1,Rest,Acc);
top_scan(Ln,<<Os:8,Rest/binary>>,Acc) when Os == $' ; Os == $" ->
    case scan_string(Os,Rest) of
        {Type,Symbol,Rest2} ->
            top_scan(Ln,Rest2,[{Type,Ln,Symbol} | Acc]);
        Err ->
            Err
    end;
top_scan(Ln,B = <<N:8, _/binary>>,Acc) when N >= $0 , N =< $9 ->
    case scan_number(B) of
        {Type,Symbol,Rest2} ->
            top_scan(Ln,Rest2,[{Type,Ln,Symbol} | Acc]);
        Err  ->
            Err
    end;
top_scan(Ln,B,Acc) ->
    case scan_atom(B) of
        {_,Symbol,Rest2} ->
            top_scan(Ln,Rest2,[{Symbol,Ln}|Acc]);
        Err ->
            Err
    end.


scan_string(Os,B) ->
    scan_string(Os,B,[]).
scan_string(_,<<>>,Acc) ->
    {error,{unterminated_string,lists:reverse(Acc)}};
scan_string(Os,<<Os:8,Rest/binary>>,Str) ->
    {str,lists:reverse(Str),Rest};
scan_string(Os,<<$\\:8,Os:8,Rest/binary>>,Str) ->
    scan_string(Os,Rest,[Os | Str]);
scan_string(Os,<<Char:8,Rest/binary>>,Str) ->
    scan_string(Os,Rest,[Char | Str]).


% a number can be terminated with a space nl ) , EOB
scan_number(B) ->
    scan_number(B,int,[]).
scan_number(<<>>,Type,Acc) ->
    {Type,revacc_to_number(Type,Acc),<<>>};
scan_number(<<N:8,Rest/binary>>,Type,Acc) when N >= $0 , N =< $9 ->
    scan_number(Rest,Type,[N | Acc]);
scan_number(<<Atom:8,Rest/binary>>,int,Acc) when Atom == $. ->
    scan_number(Rest,float,[Atom | Acc]);
scan_number(<<Atom:8,_/binary>>,float,Acc) when Atom == $. ->
    {error,{invalid_float,lists:reverse(Acc)}};
scan_number(Bin = <<Atom:8,_/binary>>,Type,Acc) 
  when Atom == $) ; Atom == $\t ; Atom == $ ; Atom == $, ;
       Atom == $= ; Atom == $< ; Atom == $>; Atom == $: ; 
       Atom == $/ ; Atom == $! ->
    {Type,revacc_to_number(Type,Acc),Bin};
scan_number(<<Sym:8,_/binary>>,int,Acc) ->
    {error,{invalid_integer,Sym,lists:reverse(Acc)}};
scan_number(<<Sym:8,_/binary>>,float,Acc) ->
    {error,{invalid_float,Sym,lists:reverse(Acc)}}.

revacc_to_number(_,[]) -> throw(null_racc_to_int);
revacc_to_number(Type,Acc) when Type == int ->
    list_to_integer(lists:reverse(Acc));
revacc_to_number(Type,Acc) when Type == float ->
    list_to_float(lists:reverse(Acc)).


% trap the various atom symbols to make parsing a little easier.
scan_atom(<<$(:8,Rest/binary>>) -> {atom,atom_result([$(]),Rest};
scan_atom(<<$):8,Rest/binary>>) -> {atom,atom_result([$)]),Rest};
scan_atom(<<$,:8,Rest/binary>>) -> {atom,atom_result([$,]),Rest};
scan_atom(<<$::8,Rest/binary>>) -> {atom,atom_result([$:]),Rest};
scan_atom(<<$-:8,$>:8,Rest/binary>>) ->
    {atom,list_to_atom("->"),Rest};
scan_atom(<<$!:8,$=:8,Rest/binary>>) ->
    {atom,list_to_atom("!="),Rest};
scan_atom(<<$!:8,Rest/binary>>) -> {atom,atom_result([$!]),Rest};
scan_atom(<<$=:8,Rest/binary>>) -> {atom,atom_result([$=]),Rest};
scan_atom(<<$>:8,$=:8,Rest/binary>>) ->
    {atom,list_to_atom(">="),Rest};
scan_atom(<<$>:8,Rest/binary>>) -> {atom,atom_result([$>]),Rest};
scan_atom(<<$<:8,$=:8,Rest/binary>>) ->
    {atom,list_to_atom("<="),Rest};
scan_atom(<<$<:8,Rest/binary>>) -> {atom,atom_result([$<]),Rest};
scan_atom(<<$+:8,Rest/binary>>) -> {atom,atom_result([$+]),Rest};
scan_atom(<<$-:8,Rest/binary>>) -> {atom,atom_result([$-]),Rest};
scan_atom(<<$*:8,Rest/binary>>) -> {atom,atom_result([$*]),Rest};
scan_atom(<<$/:8,Rest/binary>>) -> {atom,atom_result([$/]),Rest};
scan_atom(<<$%:8,Rest/binary>>) -> {atom,atom_result([$%]),Rest};
scan_atom(<<$&:8,Rest/binary>>) -> {atom,atom_result([$&]),Rest};
scan_atom(<<$|:8,Rest/binary>>) -> {atom,atom_result([$|]),Rest};
scan_atom(<<$^:8,Rest/binary>>) -> {atom,atom_result([$^]),Rest};
scan_atom(<<$~:8,Rest/binary>>) -> {atom,atom_result([$~]),Rest};
scan_atom(<<$<:8,$<:8,Rest/binary>>) ->
    {atom,list_to_atom("<<"),Rest};
scan_atom(<<$>:8,$>:8,Rest/binary>>) ->
    {atom,list_to_atom(">>"),Rest};
scan_atom(Bin = <<First_char:8,_/binary>>) ->
    % the known atom symbols should be trapped in the above
    % binary comprehensions, now only let atoms pass through if
    % they're at least one alpha character.
    case is_atom_char(First_char) of
        true ->
            scan_atom(Bin,[]);
        false ->
            {error,{unrecognized_atom,First_char}}
    end.

scan_atom(<<>>,Acc) ->
    {atom,atom_result(Acc),<<>>};
scan_atom(Bin = <<B:8,Rest/binary>>,Acc) ->
    case is_atom_char(B) of
        true -> 
            scan_atom(Rest,[B | Acc]);
        false ->
            {atom,atom_result(Acc),Bin}
    end.

atom_result(Acc) when is_list(Acc) ->   
    list_to_atom(string:to_lower(lists:reverse(Acc))).

is_atom_char(B) when B >= $a , B =< $z -> true;
is_atom_char(B) when B >= $A , B =< $Z -> true;
is_atom_char(B) when B == $_ -> true;
is_atom_char(_) -> false.
