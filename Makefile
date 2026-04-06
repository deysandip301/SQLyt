CC := gcc
CFLAGS := -std=c11 -pthread
TARGET := db

.PHONY: all build test clean

all: build

build: $(TARGET)

$(TARGET): main.c btree.c pager.c parser.c cli.c
	$(CC) $(CFLAGS) main.c -o $(TARGET)

test: $(TARGET)
	python3 -m unittest -v test_main.py

clean:
	rm -f $(TARGET)
