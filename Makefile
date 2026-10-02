.PHONY: all compile test smoke clean

ERLC = erlc
ERLCFLAGS = +debug_info -Werror

all: compile

compile:
	mkdir -p ebin
	$(ERLC) $(ERLCFLAGS) -o ebin src/*.erl

test: compile
	erl -noshell -pa ebin -eval 'ok = topology:self_check(), halt(0).'

smoke: compile
	./gossip 16 full gossip
	./gossip 9 line gossip
	./gossip 16 2D push-sum
	./gossip 9 imp2D push-sum

clean:
	rm -rf ebin erl_crash.dump
