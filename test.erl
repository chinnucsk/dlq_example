#!/usr/bin/env escript
%% -*- erlang -*-

main([]) ->
    F = fun(S) -> 
                {ok, Toks} = dlquery_scan:scan(S),
                io:format("~n~p~n",[dlquery:parse(Toks)])
        end,
    [F(S) || S <- ["get oid, md('subject'), md('from') from
                    recurse 1 at objpath(
                        rootname:'john_doe'->name:'mail'->name:'inbox')
                    where md('flagged') = true or md('read') = false",


                   "get oid, od 'size', md 'book_title' from 
                    recurse 1 at 
                    objpath(rootname:'john_doe'->name:'bookshelf'->name:'books')
                    where eqi(md('book_author'),'stephenson')
                    and od('timestamp') < to_date('20120214')",


                   "get all od, all md, childlist, parentlist from
                    recurse 2 at rootname('testing1234') where
                    (((acl('test:12345:accountname') & 293 and
                       acl('test:12345:accountname') & 1048576) or
                       stop_rec(false))
                     and (set_lvar('type', od('type'))
                     and (var('type') = 'dirrecMembers' 
                          or var('type') = 'contactInfo' 
                          or var('type') = 'contactAddressInfo' 
                          or ((var('type') = 'contract') or stop_rec(false)))))"]].
