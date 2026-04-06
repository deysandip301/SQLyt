CC := gcc
CFLAGS := -std=c11 -pthread -Iinclude
TARGET := db
SRC_DIR := src
INC_DIR := include
TEST_DIR := tests
OBJ_DIR := .build/obj
OBJS := $(OBJ_DIR)/app.o $(OBJ_DIR)/readline_runtime.o $(OBJ_DIR)/btree.o $(OBJ_DIR)/pager.o $(OBJ_DIR)/parser.o $(OBJ_DIR)/cli.o

.PHONY: all build test clean

all: build

build: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) -o $(TARGET)

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

$(OBJ_DIR)/app.o: $(SRC_DIR)/app.c $(INC_DIR)/sqlyt.h | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $(SRC_DIR)/app.c -o $(OBJ_DIR)/app.o

$(OBJ_DIR)/readline_runtime.o: $(SRC_DIR)/readline_runtime.c $(INC_DIR)/sqlyt.h | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $(SRC_DIR)/readline_runtime.c -o $(OBJ_DIR)/readline_runtime.o

$(OBJ_DIR)/btree.o: $(SRC_DIR)/btree.c $(INC_DIR)/sqlyt.h | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $(SRC_DIR)/btree.c -o $(OBJ_DIR)/btree.o

$(OBJ_DIR)/pager.o: $(SRC_DIR)/pager.c $(INC_DIR)/sqlyt.h | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $(SRC_DIR)/pager.c -o $(OBJ_DIR)/pager.o

$(OBJ_DIR)/parser.o: $(SRC_DIR)/parser.c $(INC_DIR)/sqlyt.h | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $(SRC_DIR)/parser.c -o $(OBJ_DIR)/parser.o

$(OBJ_DIR)/cli.o: $(SRC_DIR)/cli.c $(INC_DIR)/sqlyt.h | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $(SRC_DIR)/cli.c -o $(OBJ_DIR)/cli.o

test: $(TARGET)
	python3 -m unittest -v $(TEST_DIR)/test_main.py

clean:
	rm -f $(TARGET) $(OBJS)
	rm -rf .build
