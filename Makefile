all: dlquery.beam dlquery_scan.beam
	./test.erl

dlquery.beam: dlquery.yrl
	erl -eval 'yecc:file(dlquery)' -s erlang halt
	erlc dlquery.erl

dlquery_scan.beam: dlquery_scan.erl
	erlc dlquery_scan.erl

clean:
	rm -f dlquery.beam dlquery.erl dlquery_scan.beam

