#ifndef _qbuf_h_
#define _qbuf_h_

#include <stdint.h>
#include <stdbool.h>

#define buf_reserve(encoder, add_size) qbuf_reserve(&(encoder)->buffer, &(encoder)->cap, &(encoder)->write_ptr, add_size)
#define write_byte(encoder, b) qbuf_write_byte(&(encoder)->buffer, &(encoder)->cap, &(encoder)->write_ptr, (b))
#define write_integer(encoder, value) qbuf_write_integer(&(encoder)->buffer, &(encoder)->cap, &(encoder)->write_ptr, (value))
#define write_uinteger(encoder, value) qbuf_write_uinteger(&(encoder)->buffer, &(encoder)->cap, &(encoder)->write_ptr, (value))
#define write_buf(encoder, _buffer, size) qbuf_write_buf(&(encoder)->buffer, &(encoder)->cap, &(encoder)->write_ptr, _buffer, (size))
#define read_integer(decoder, value) qbuf_read_integer((decoder)->buffer, (decoder)->size, &(decoder)->read_ptr, (value))
#define read_uinteger(decoder, value) qbuf_read_uinteger((decoder)->buffer, (decoder)->size, &(decoder)->read_ptr, (value))
#define read_buf(decoder, buf_size) qbuf_read_buf((decoder)->buffer, (decoder)->size, &(decoder)->read_ptr, (buf_size))
#define write_skip(encoder, skip_size) qbuf_write_skip(&(encoder)->buffer, &(encoder)->cap, &(encoder)->write_ptr, (skip_size))

void qbuf_reserve(char** buffer, int32_t* cap, int32_t* wptr, int32_t add_size);

void qbuf_write_byte(char** buffer, int32_t* cap, int32_t* wptr, uint8_t b);

int32_t qbuf_write_integer(char** buffer, int32_t* cap, int32_t* wptr, int64_t value);

bool qbuf_read_integer(char* buffer, int32_t size, int32_t* rptr, int64_t* value);

int32_t qbuf_write_uinteger(char** buffer, int32_t* cap, int32_t* wptr, int64_t value);

bool qbuf_read_uinteger(char* buffer, int32_t size, int32_t* rptr, int64_t* value);

void qbuf_write_buf(char** buffer, int32_t* cap, int32_t* wptr, const void* buf, int32_t buf_size);

char* qbuf_read_buf(char* buffer, int32_t size, int32_t* rptr, int32_t buf_size);

void qbuf_write_skip(char** buffer, int32_t* cap, int32_t* wptr, int32_t skip_size);

void qbuf_write_rewind(char** buffer, int32_t* wptr, int32_t rewind_size);

#endif