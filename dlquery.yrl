Nonterminals
    root_query
    full_x opt_x
    get_x get_list get_item
    from_x from_list from_rec from_item obj_path op_list op_item
        rn_list rn_set oid_list oid_set oid_val
    where_x where_group where_eval where_meval where_func where_set where_val
        where_pre where_op where_mop where_fop
    sort_x sort_list
    range_x range_list range_item
    field_x bf_field_x str_val int_val float_val bool_val.

Terminals
    '(' ')' ',' ':'
    'get' 'oid' 'childlist' 'parentlist' 'md' 'od' 'acl' 'bf' 'blob' 'all'
    'from' 'recurse' 'at' 'oids' 'rootname' 'rootnames' 'objpath' 'name' '->'
    'where' '!' '~' 'and' 'or' '!='
    '=' '>' '>=' '<' '<=' '+' '-' '*' '/' '%' '&' '|' '^' '<<' '>>'
    'eq' 'eqi' 'cstr' 'cstri' 'bstr' 'bstri' 'estr' 'estri' 'regex' 'is_str'
    'is_int' 'is_float' 'is_bool' 'is_time_date' 'is_epoch' 'is_date'
    'is_time_period' 'is_null' 'to_time_date' 'to_epoch' 'to_date'
    'to_time_period' 'now' 'var' 'lvar' 'rvar' 'set_lvar' 'set_rvar'
    'stop_rec' 'stop_query'
    'sort' 'asc' 'des'
    'range'
    'true' 'TRUE' 't' 'T' 'false' 'FALSE' 'f' 'F'
    str int float.

Rootsymbol root_query.

Endsymbol '$end'.

%% ====================================================================

root_query -> full_x : '$1'.

%% ====================================================================

full_x -> get_x from_x : {'$1','$2'}.
full_x -> get_x from_x opt_x: {'$1','$2','$3'}.
full_x -> get_x from_x opt_x opt_x: {'$1','$2','$3','$4'}.
full_x -> get_x from_x where_x : {'$1','$2','$3'}.
full_x -> get_x from_x where_x opt_x: {'$1','$2','$3','$4'}.
full_x -> get_x from_x where_x opt_x opt_x: {'$1','$2','$3','$4','$5'}.
full_x -> where_x : '$1'.

opt_x -> sort_x : '$1'.
opt_x -> range_x : '$1'.

%% ====================================================================

get_x -> 'get' get_list : {get, lists:flatten('$2')}.
get_x -> 'get' '(' get_list ')' : {get, lists:flatten('$3')}.
get_x -> 'get' : {get,[]}.

get_list -> get_item : ['$1'].
get_list -> get_item ',' get_list : ['$1','$3'].

get_item -> 'oid' : oid.
get_item -> 'childlist' : childlist.
get_item -> 'parentlist' : parentlist.
get_item -> 'all' 'od' : all_od.
get_item -> 'all' 'md' : all_md.
get_item -> 'all' 'acl' : all_acl.
get_item -> 'all' 'bf' : all_bf.
get_item -> field_x : '$1'.
get_item -> bf_field_x : '$1'.
get_item -> 'blob' : blob.
get_item -> 'blob' int_val int_val : {blobchunk,'$2','$3'}.
get_item -> 'blob' bf_field_x : {blobchunk,'$2'}.
get_item -> 'all' : all.

%% ====================================================================

from_x -> 'from' from_list : {from, lists:flatten('$2')}.
from_x -> 'from' '(' from_list ')' : {from, lists:flatten('$3')}.
from_list -> from_rec : ['$1'].
from_list -> from_rec ',' from_list : ['$1','$3'].

from_rec -> 'recurse' int_val 'at' from_item  : {recurse,'$2','$4'}.
from_rec -> 'recurse' 'all' 'at' from_item : {recurse,all,'$4'}.
from_rec -> from_item : '$1'.

from_item -> obj_path : {objpath, '$1'}.
from_item -> rn_list : {rnlist, '$1'}.
from_item -> oid_list : {oidlist, '$1'}.

obj_path -> 'objpath' '(' op_list ')' : lists:flatten('$3').
op_list -> op_item : ['$1'].
op_list -> op_item '->' op_list : [ '$1', '$3' ].
op_item -> 'rootname' ':' str_val : {rootname, '$3'}.
op_item -> 'name' ':' str_val : {name, '$3'}.
op_item -> 'oid' oid_val : '$2'.

rn_list -> 'rootname' str_val : [{rootname,'$2'}].
rn_list -> 'rootname' '(' str_val ')' : [{rootname,'$3'}].
rn_list -> 'rootnames' '(' rn_set ')' : lists:flatten('$3').
rn_set -> str_val : [{rootname,'$1'}].
rn_set -> str_val ',' rn_set : [{rootname,'$1'}, '$3'].

oid_list -> 'oid' oid_val : '$2'.
oid_list -> 'oids' '(' oid_set ')' : lists:flatten('$3').
oid_set -> oid_val : ['$1'].
oid_set -> oid_val ',' oid_set : ['$1','$3'].

oid_val -> int_val ':' int_val : {oid,{'$1','$3'}}.
oid_val -> '(' int_val ':' int_val ')' : {oid, {'$2','$4'}}.
oid_val -> int_val : {oid,'$1'}.

%% ====================================================================

where_x -> 'where' where_group : {where, '$2'}.

where_group -> where_group 'and' where_group : {op,land,'$1','$3'}.
where_group -> where_group 'or' where_group : {op,lor,'$1','$3'}.
where_group -> where_eval '!=' where_eval : {op,neg,{op,eq,'$1','$3'}}.
where_group -> where_eval where_op where_eval : {op,'$2','$1','$3'}.
where_group -> where_meval where_mop where_meval : {op,'$2','$1','$3'}.
where_group -> where_eval : '$1'.

where_eval -> '(' where_group ')' : '$2'.
where_eval -> where_pre where_eval : {op,'$1','$2'}.
where_eval -> where_func : list_to_tuple(lists:flatten('$1')).
where_eval -> where_val : '$1'.

where_meval -> '(' where_group ')' : '$2'.
where_meval -> where_meval where_mop where_meval : {op,'$2','$1','$3'}.
where_meval -> where_pre where_meval : {op,'$1','$2'}.
where_meval -> where_func : list_to_tuple(lists:flatten('$1')).
where_meval -> where_val : '$1'.

where_func -> where_fop '(' ')' : [op,'$1'].
where_func -> where_fop '(' where_set ')' : [op,'$1','$3'].

where_set -> where_group : ['$1'].
where_set -> where_group ',' where_set : ['$1','$3'].

where_val -> str_val : {val,str,'$1'}.
where_val -> int_val : {val,int,'$1'}.
where_val -> float_val : {val,float,'$1'}.
where_val -> bool_val : {val,bool,'$1'}.

where_pre -> '!' : neg.
where_pre -> '~' : bicom.

where_op -> '=' : eq.
where_op -> '>' : gt.
where_op -> '>=' : gte.
where_op -> '<' : lt.
where_op -> '<=' : lte.

where_mop -> '+' : add.
where_mop -> '-' : sub.
where_mop -> '*' : mult.
where_mop -> '/' : divd.
where_mop -> '%' : mod.
where_mop -> '&' : biand.
where_mop -> '|' : bior.
where_mop -> '^' : bixor.
where_mop -> '<<' : bisl.
where_mop -> '>>' : bisr.

where_fop -> 'eq' : eq.
where_fop -> 'eqi' : eqi.
where_fop -> 'cstr' : cstr.
where_fop -> 'cstri' : cstri.
where_fop -> 'bstr' : bstr.
where_fop -> 'bstri' : bstri.
where_fop -> 'estr' : estr.
where_fop -> 'estri' : estri.
where_fop -> 'regex' : regex.
where_fop -> 'od' : od.
where_fop -> 'md' : md.
where_fop -> 'acl' : acl.
where_fop -> 'bf' : bf.
where_fop -> 'is_str' : is_str.
where_fop -> 'is_int' : is_int.
where_fop -> 'is_float' : is_float.
where_fop -> 'is_bool' : is_bool.
where_fop -> 'is_time_date' : is_time_date.
where_fop -> 'is_epoch' : is_epoch.
where_fop -> 'is_date' : is_date.
where_fop -> 'is_time_period' : is_time_period.
where_fop -> 'is_null' : is_null.
where_fop -> 'to_time_date' : to_time_date.
where_fop -> 'to_epoch' : to_epoch.
where_fop -> 'to_date' : to_date.
where_fop -> 'to_time_period' : to_time_period.
where_fop -> 'now' : now.
where_fop -> 'var' : var.
where_fop -> 'lvar' : lvar.
where_fop -> 'rvar' : rvar.
where_fop -> 'set_lvar' : set_lvar.
where_fop -> 'set_rvar' : set_rvar.
where_fop -> 'stop_rec' : stop_rec.
where_fop -> 'stop_query' : stop_query.

%% ====================================================================

sort_x -> 'sort' sort_list : {sort, '$2'}.
sort_x -> 'sort' '(' sort_list ')' : {sort, '$3'}.

sort_list -> field_x : {asc,'$1'}.
sort_list -> field_x ':' 'asc' : {asc,'$1'}.
sort_list -> field_x ':' 'des' : {des,'$1'}.

%% ====================================================================

range_x -> 'range' range_list : {range,lists:flatten('$2')}.
range_x -> 'range' '(' range_list ')' : {range,lists:flatten('$3')}.

range_list -> range_item : ['$1'].
range_list -> range_item ',' range_list : ['$1','$3'].

range_item -> int_val : {'$1',1}.
range_item -> int_val ':' int_val : {'$1','$3'}.

%% ====================================================================

field_x -> 'od' str_val : {od,'$2'}.
field_x -> 'od' '(' str_val ')' : {od,'$3'}.
field_x -> 'md' str_val : {md,'$2'}.
field_x -> 'md' '(' str_val ')' : {md,'$3'}.
field_x -> 'acl' str_val : {acl,'$2'}.
field_x -> 'acl' '(' str_val ')' : {acl,'$3'}.

bf_field_x -> 'bf' str_val : {bf, '$2'}.
bf_field_x -> 'bf' '(' str_val ')' : {bf, '$3'}.

str_val -> str : value_of('$1').

int_val -> int : value_of('$1').

float_val -> float : value_of('$1').

bool_val -> 'true' : 'true'.
bool_val -> 'TRUE' : 'true'.
bool_val -> 't' : 'true'.
bool_val -> 'T' : 'true'.
bool_val -> 'false' : 'false'.
bool_val -> 'FALSE' : 'false'.
bool_val -> 'f' : 'false'.
bool_val -> 'F' : 'false'.

Erlang code.

value_of({_Category,_Line_num,Value}) -> Value;
value_of({Value,_Line_num}) -> Value.
