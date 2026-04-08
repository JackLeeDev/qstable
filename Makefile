CC = gcc
CFLAGS = -std=gnu99 -Wall -Wextra -O2 -fPIC
LDFLAGS = -shared

LUA_PATH = ./3rd/lua # replace with your lua path
LUA_INC = -I$(LUA_PATH)

# Library name
TARGET = qstable.so

# Source files
SRCS = qstable.c qbuf.c qbarray.c
OBJS = $(SRCS:.c=.o)

# Default target
all: $(TARGET)

# Link shared library
$(TARGET): $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ -lm -ldl

# Compile C files in root directory
%.o: %.c
	$(CC) $(CFLAGS) $(LUA_INC) -c -o $@ $<

# Clean build files
clean:
	rm -f $(OBJS) $(TARGET)
