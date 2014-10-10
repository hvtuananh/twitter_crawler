ROOT=$(shell pwd)
LIBS=twitter4j-core-3.0.3.jar twitter4j-stream-3.0.3.jar 

comma:= :
empty:=
space:= $(empty) $(empty)

LIBS2=$(addprefix $(ROOT)/lib/, $(LIBS))
LIBS3=$(subst $(space),$(comma),$(LIBS2))

all:
	find src | egrep "[.]java$$" > file.list
	javac -classpath $(LIBS3):. @file.list

run:
	nohup java -classpath $(LIBS3):src/. Stream > /dev/null

debug:
	nohup java -classpath $(LIBS3):src/. Stream
