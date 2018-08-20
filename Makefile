################################
### FreeSwitch headers files found in libfreeswitch-dev ###
FS_INCLUDES=/usr/local/freeswitch/include/freeswitch
FS_LIB=/usr/local/freeswitch/lib
FS_MODULES=/usr/local/freeswitch/mod
################################
KF_INCLUDES=/usr/local/Cellar/librdkafka/0.11.5/include/librdkafka


CC=gcc 
CFLAGS=-fPIC -O3 -fomit-frame-pointer -fno-exceptions -Wall -std=c99 -pedantic

INCLUDES=-I/usr/local/include -I$(FS_INCLUDES) -I$(KF_INCLUDES)
LDFLAGS=-lm -L/usr/local/lib -lrdkafka -L$(FS_LIB) -lfreeswitch

all: mod_event_kafka.o
	$(CC) $(CFLAGS) $(INCLUDES) -shared -Xlinker -x -o mod_event_kafka.so mod_event_kafka.o $(LDFLAGS)

mod_event_kafka.o: mod_event_kafka.c
	$(CC) $(CFLAGS) $(INCLUDES) -c mod_event_kafka.c

clean:
	rm -f *.o *.so *.a *.la

install: all
	/usr/bin/install -c mod_event_kafka.so $(FS_MODULES)/mod_event_kafka.so