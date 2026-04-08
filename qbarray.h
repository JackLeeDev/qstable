#ifndef _qbarray_h_
#define _qbarray_h_

#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>

typedef int32_t (*qbarray_compare_func)(const void*, const void*);

typedef struct qbarray {
	void* data;
	int32_t elem_size;
	int32_t size;
	int32_t cap;
	int32_t expand_size; // 0 double current cap, >0 fixed size increment
	qbarray_compare_func compare;
} qbarray;

void qbarray_init(qbarray* array, int32_t elem_size, int32_t expand_size, qbarray_compare_func compare);

void qbarray_release(qbarray* array);

void* qbarray_get(qbarray* array, int32_t index);

int32_t qbarray_indexof(qbarray* array, void* elem);

void* qbarray_find(qbarray* array, void* elem);

void* qbarray_find_value(qbarray* array, void* value); //value equals *elem

void qbarray_push_back(qbarray* array, void* elem); //without reordering, should call qbarray_sort later

void qbarray_append(qbarray* array, void* elem, int32_t num); //call qbarray_sort later

bool qbarray_insert(qbarray* array, void* elem);

bool qbarray_remove(qbarray* array, void* elem);

void qbarray_clear(qbarray* array);

void qbarray_pop_front(qbarray* array, int32_t n);

void qbarray_pop_back(qbarray* array, int32_t n);

void qbarray_sort(qbarray* array);

#endif

