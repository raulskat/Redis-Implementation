CC = gcc
CFLAGS = -Wall -Wextra -pedantic -std=c11 -g -pthread -D_POSIX_C_SOURCE=200809L
LDFLAGS = -pthread
INCLUDES = -Iinclude
TARGET = redis-server
SRC = $(wildcard src/*.c)
OBJ = $(SRC:.c=.o)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $(OBJ) $(LDFLAGS)

src/%.o: src/%.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

.PHONY: clean
clean:
	rm -f $(OBJ) $(TARGET)
