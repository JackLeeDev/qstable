//use big endian
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "qbuf.h"

#define QBUF_INT_MAX_SIZE 10
#define QBUF_INT_MAX_INDEX QBUF_INT_MAX_SIZE-1
#define QBUF_UINT_MAX_SIZE 9
#define QBUF_UINT_MAX_INDEX QBUF_UINT_MAX_SIZE-1

void qbuf_reserve(char** buffer, int32_t* cap, int32_t* wptr, int32_t add_size) {
	if (*wptr + add_size > *cap) {
		while (*wptr + add_size > *cap) {
			if (*cap > 0)
				*cap <<= 1;
			else
				*cap = 64;
		}
		*buffer = realloc(*buffer, *cap);
	}
}

void qbuf_write_byte(char** buffer, int32_t* cap, int32_t* wptr, uint8_t b) {
	qbuf_reserve(buffer, cap, wptr, 1);
	*(*buffer+*wptr) = b;
	*wptr += 1;
}

int32_t qbuf_write_integer(char** buffer, int32_t* cap, int32_t* wptr, int64_t value) {
	bool negative = false;
	if (value < 0) {
		negative = true;
		value = -value;
	}
	uint8_t buf[QBUF_INT_MAX_SIZE];
	int32_t nbyte = 0;
	int32_t i;
	for (i=0; i<QBUF_INT_MAX_SIZE; ++i) {
		++nbyte;
		int32_t index = QBUF_INT_MAX_INDEX - i;
		if (i != 0) {
			buf[index] = (value>>(7*i-1))&0xFE;
		}
		else {
			buf[index] = ((value<<1)&((value<=0x3F)?0x7E:0xFE)) | 1; // set end tag
		}
		if (value<((int64_t)1<<(i*7+6)) || i>=QBUF_INT_MAX_INDEX) {
			if (negative) {
				buf[index] |= 0x80; //mark negative
			}
			qbuf_write_buf(buffer, cap, wptr, &buf[QBUF_INT_MAX_INDEX-nbyte+1], nbyte);
			break;
		}
	}
	return nbyte;
}

bool qbuf_read_integer(char* buffer, int32_t size, int32_t* rptr, int64_t* value) {
	*value = 0;
	bool negative = false;
	int32_t nbyte = 0;
	int32_t i;
	for (i=0; i<QBUF_INT_MAX_SIZE; ++i) {
		if (*rptr >= size) {
			return false;
		}
		uint8_t b = buffer[*rptr];
		if (i != 0) {
			*value = (((int64_t)*value)<<7) + (b>>1);
		}
		else {
			//check negative tag
			if ((b&0x80) != 0) {
				negative = true;
			}
			*value = (b&0x7F) >> 1;
		}
		++*rptr;
		++nbyte;
		//check end tag
		if ((b&0x01) != 0) {
			break;
		}
		if (i == QBUF_INT_MAX_INDEX) {
			return false;
		}
	}
	if (negative) {
		*value = -*value;
	}
	return true;
}

int32_t qbuf_write_uinteger(char** buffer, int32_t* cap, int32_t* wptr, int64_t value) {
	uint8_t buf[QBUF_UINT_MAX_SIZE];
	int32_t nbyte = 0;
	int32_t i;
	for (i=0; i<QBUF_UINT_MAX_SIZE; ++i) {
		++nbyte;
		int32_t index = QBUF_UINT_MAX_INDEX - i;
		if (i != 0) {
			buf[index] = (value>>(7*i-1))&0xFE;
		}
		else {
			buf[index] = ((value<<1)&0xFE) | 1; // add end tag
		}
		if (value<((int64_t)1<<((i+1)*7)) || i>=QBUF_UINT_MAX_INDEX) {
			qbuf_write_buf(buffer, cap, wptr, &buf[QBUF_UINT_MAX_INDEX-nbyte+1], nbyte);
			break;
		}
	}
	return nbyte;
}

bool qbuf_read_uinteger(char* buffer, int32_t size, int32_t* rptr, int64_t* value) {
	*value = 0;
	int32_t nbyte = 0;
	int32_t i;
	for (i=0; i<QBUF_UINT_MAX_SIZE; ++i) {
		if (*rptr >= size) {
			return false;
		}
		uint8_t b = buffer[*rptr];
		*value = (((int64_t)*value)<<7) + (b>>1);
		++*rptr;
		++nbyte;
		//check end tag
		if ((b&0x01) != 0) {
			break;
		}
		if (i == QBUF_UINT_MAX_INDEX) {
			return false;
		}
	}
	return true;
}

void qbuf_write_buf(char** buffer, int32_t* cap, int32_t* wptr, const void* buf, int32_t buf_size) {
	qbuf_reserve(buffer, cap, wptr, buf_size);
	assert(*wptr + buf_size <= *cap);
	if (buf_size == 1) {
		*(*buffer+(*wptr)++) = *(const char*)buf;
	}
	else {
		memcpy(*buffer + *wptr, buf, buf_size);
		*wptr += buf_size;
	}
}

char* qbuf_read_buf(char* buffer, int32_t size, int32_t* rptr, int32_t buf_size) {
	if (*rptr + buf_size <= size) {
		char* buf = buffer + *rptr;
		*rptr += buf_size;
		return buf;
	}
	return NULL;
}

void qbuf_write_skip(char** buffer, int32_t* cap, int32_t* wptr, int32_t skip_size) {
	qbuf_reserve(buffer, cap, wptr, skip_size);
	*wptr += skip_size;
}

void qbuf_write_rewind(char** buffer, int32_t* wptr, int32_t rewind_size) {
	(void)buffer;
	assert(rewind_size >= *wptr);
	*wptr -= rewind_size;
}