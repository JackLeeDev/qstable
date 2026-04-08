#include <assert.h>
#include <stdint.h>
#include <string.h>
#include "qdef.h"
#include "qbarray.h"

#define BARRAY_INIT_SIZE 4

#define get_elem_ptr(array, index) ((char*)array->data + (index)*array->elem_size)

static inline bool find_index(qbarray* array, void* elem, int32_t* index) {
	int32_t left = 0;
	int32_t right = array->size-1;
	while (left <= right) {
		int32_t middle = (left + right) / 2;
		int32_t r = array->compare(elem, get_elem_ptr(array, middle));
		if (r < 0) {
			right = middle - 1;
		}
		else if (r == 0) {
			*index = middle;
			return true;
		}
		else {
			left = middle + 1;
		}
	}
	*index = left;
	return false;
}

void qbarray_init(qbarray* array, int32_t elem_size, int32_t expand_size, qbarray_compare_func compare) {
	assert(elem_size > 0);
	// assert(compare);
	memset(array, 0, sizeof(*array));
	array->elem_size = elem_size;
	array->expand_size = expand_size,
	array->compare = compare;
}

void qbarray_release(qbarray* array) {
	if (array) {
		safe_free(array->data);
		array->size = 0;
	}
}

void* qbarray_get(qbarray* array, int32_t index) {
	if (index >= 0 && index < array->size) {
		return get_elem_ptr(array, index);
	}
	return NULL;
}

int32_t qbarray_indexof(qbarray* array, void* elem) {
	assert(array);
	assert(elem);
	if (array->size <= 4) {
		int32_t i;
		for (i=0; i<array->size; ++i) {
			if (array->compare(elem, get_elem_ptr(array, i)) == 0) {
				return i;
			}
		}
	}
	else {
		int32_t index = -1;
		if (find_index(array, elem, &index)) {
			assert(index >= 0);
			return index;
		}
	}
	return -1;
}

void* qbarray_find(qbarray* array, void* elem) {
	int32_t index = qbarray_indexof(array, elem);
	if (index >= 0 && index < array->size) {
		return get_elem_ptr(array, index);
	}
	return NULL;
}

void* qbarray_find_value(qbarray* array, void* value) {
	return qbarray_find(array, &value);
}

static inline void array_reserve(qbarray* array, int32_t num) {
	assert(num >= 0);
	if (array->size + num > array->cap) {
		while (array->size + num > array->cap) {
			if (array->expand_size > 0) {
				array->cap += array->expand_size;
			}
			else {
				if (array->cap > 0) 
					array->cap <<= 1;
				else
					array->cap = BARRAY_INIT_SIZE;
			}
		}
		array->data = realloc(array->data, array->cap*array->elem_size);
	}
}

void qbarray_push_back(qbarray* array, void* elem) {
	array_reserve(array, 1);
	memcpy(get_elem_ptr(array, array->size++), elem, array->elem_size);
}

void qbarray_append(qbarray* array, void* elem, int32_t num) {
	assert(num >= 0);
	if (num > 0) {
		array_reserve(array, num);
		memcpy(get_elem_ptr(array, array->size), elem, num*array->elem_size);
		array->size += num;
	}
}

bool qbarray_insert(qbarray* array, void* elem) {
	assert(array);
	assert(elem);
	int32_t index = -1;
	if (find_index(array, elem, &index)) {
		assert(index >= 0);
		return false;
	}
	array_reserve(array, 1);
	assert(array->size < array->cap);
	int32_t move_count = array->size - index;
	if (move_count > 0) {
		memmove(get_elem_ptr(array, index+1), get_elem_ptr(array, index), move_count*array->elem_size);
	}
	memcpy(get_elem_ptr(array, index), elem, array->elem_size);
	++array->size;
	return true;
}

bool qbarray_remove(qbarray* array, void* elem) {
	assert(array);
	assert(elem);
	int32_t index = -1;
	if (find_index(array, elem, &index)) {
		assert(index >= 0);
		int32_t move_count = array->size - index - 1;
		if (move_count > 0) {
			memmove(get_elem_ptr(array, index), get_elem_ptr(array, index+1), move_count*array->elem_size);
		}
		--array->size;
		return true;
	}
	return false;
}

void qbarray_clear(qbarray* array) {
	array->size = 0;
}

void qbarray_pop_front(qbarray* array, int32_t n) {
	assert(n <= array->size);
	if (n > 0) {
		int32_t move_count = array->size - n;
		if (move_count > 0) {
			memmove(get_elem_ptr(array, 0), get_elem_ptr(array, n), move_count*array->elem_size);
		}
		array->size -= n;
		assert(array->size >= 0);
	}
}

void qbarray_pop_back(qbarray* array, int32_t n) {
	assert(n <= array->size);
	if (n > 0) {
		array->size -= n;
	}
}

void qbarray_sort(qbarray* array) {
	if (array->size > 1) {
		qsort(array->data, array->size, array->elem_size, array->compare);
	}
}