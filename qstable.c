#define LUA_LIB

#include <assert.h>
#include <stdbool.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <lua.h>
#include <lauxlib.h>
#include "qlock.h"
#include "qbarray.h"

#define QST_TNIL 0
#define QST_TINTEGER 1
#define QST_TSTRING 2
#define QST_TNUMBER 3
#define QST_TBOOLEAN 4
#define QST_TTABLE 5

#define QST_TT_MAP 0
#define QST_TT_ARRAY 1

#define MAX_HASH_SIZE 48
#define MAX_NAME_SIZE 64
#define TABLE_HNODE_SIZE 4096
#define QSTABLE_MAGIC 0x5F7D9B1D3F5F7D9BULL //FNV-1a(com.qstable.mylib)

#define get_table_iarray_element(shmcfg, idx) &(((int64_t*)((char*)(shmcfg)+(shmcfg)->iarray_pos))[idx])
#define get_table_darray_element(shmcfg, idx) &(((double*)((char*)(shmcfg)+(shmcfg)->darray_pos))[idx])
#define get_table_sarray_element(shmcfg, idx) ((const char*)((char*)(shmcfg)+(((int32_t*)((char*)(shmcfg)+(shmcfg)->sarray_pos))[idx])))
#define get_table_by_index(shmcfg, idx) ((qxtable*)((char*)(shmcfg)+(((int32_t*)((char*)(shmcfg)+(shmcfg)->tarray_pos))[idx])))

typedef struct qstable_value {
	uint8_t tt : 8;
	uint32_t idx : 24;
} qstable_value;

typedef struct qxtable {
	uint8_t tt : 1;
	uint32_t size : 31;
	qstable_value fields[];
} qxtable;

typedef struct qstable_hnode {
	int32_t idx;
	uint64_t hash;
} qstable_hnode;

typedef struct shm_config {
	uint64_t magic;
	int32_t iarray_pos;
	int32_t iarray_size;
	int32_t darray_pos;
	int32_t darray_size;
	int32_t sarray_pos;
	int32_t sarray_size;
	int32_t str_data_pos;
	int32_t tarray_pos;
	int32_t tarray_size;
	int32_t table_data_pos;
	int32_t mem_size;
	uint32_t pid;
	int64_t time;
	char hash[MAX_HASH_SIZE];
	char name[MAX_NAME_SIZE];
} shm_config;

typedef struct config {
	char name[MAX_NAME_SIZE];
	shm_config* shmcfg;
	int64_t saddr;
	int64_t eaddr;
} config;

typedef struct config_loader {
	qbarray iarray;
	qbarray darray;
	qbarray sarray;
	qbarray tarray[TABLE_HNODE_SIZE];
	int32_t str_data_size;
	int32_t tb_size;
	int32_t tb_write_idx;
	int32_t tb_data_size;
	int32_t write_pos;
} config_loader;

typedef struct config_context {
	qbarray cfg;
	qbarray sorted_cfg;
	rwlock_t lock;
	bool inited;
} config_context;

static config_context g_ctx;

static int32_t int64_index(config_loader* loader, int64_t value) {
	int32_t idx = qbarray_indexof(&loader->iarray, &value);
	if (idx == -1) {
		qbarray_insert(&loader->iarray, &value);
		idx = qbarray_indexof(&loader->iarray, &value);
		assert(idx >= 0);
	}
	return idx;
}

static int32_t double_index(config_loader* loader, double value) {
	int32_t idx = qbarray_indexof(&loader->darray, &value);
	if (idx == -1) {
		qbarray_insert(&loader->darray, &value);
		idx = qbarray_indexof(&loader->darray, &value);
		assert(idx >= 0);
	}
	return idx;
}

static int32_t string_index(config_loader* loader, const char* value) {
	int32_t idx = qbarray_indexof(&loader->sarray, &value);
	if (idx == -1) {
		qbarray_insert(&loader->sarray, &value);
		idx = qbarray_indexof(&loader->sarray, &value);
		loader->str_data_size += strlen(value) + 1;
		assert(idx >= 0);
	}
	return idx;
}

static void table_preload(lua_State *L, int32_t stack, config_loader* loader) {
	loader->tb_size++;
	loader->tb_data_size += sizeof(qxtable);

	bool is_array = true;
	int32_t array_size = lua_rawlen(L, stack);
	int32_t size = 0;

	lua_pushnil(L);
	while (lua_next(L, stack)) {
		size = size + 1;
		int32_t tk = lua_type(L, -2);
		switch (tk) {
			case LUA_TNUMBER: {
				if (lua_isinteger(L, -2)) {
					int64_t key = lua_tointeger(L, -2);
					if (key <= 0 || key > array_size) {
						is_array = false;
					}
					int64_index(loader, key);
				}
				else {
					lua_pop(L, 2);
					luaL_error(L, "Invalid type key(%s), expect integer got double", luaL_typename(L, -2));
				}
				break;
			}
			case LUA_TSTRING: {
				is_array = false;
				string_index(loader, lua_tostring(L, -2));
				break;
			}
			default: {
				lua_pop(L, 2);
				luaL_error(L, "Invalid type key(%s)", luaL_typename(L, -2));
				break;
			}
		}

		int32_t tv = lua_type(L, -1);
		switch (tv) {
			case LUA_TNUMBER: {
				if (lua_isinteger(L, -1)) {
					int64_index(loader, lua_tointeger(L, -1));
				}
				else {
					double_index(loader, lua_tonumber(L, -1));
				}
				break;
			}
			case LUA_TSTRING: {
				string_index(loader, lua_tostring(L, -1));
				break;
			}
			case LUA_TTABLE: {
				table_preload(L, lua_gettop(L), loader);
				break;
			}
			case LUA_TBOOLEAN: {
				break;
			}
			default: {
				lua_pop(L, 2);
				luaL_error(L, "Invalid type value(%s)", luaL_typename(L, tv));
				break;
			}
		}

		lua_pop(L, 1);
	}

	if (is_array) {
		is_array = array_size == size;
	}
	loader->tb_data_size += size*sizeof(qstable_value) * (is_array?1:2);
}

static int32_t table_info(lua_State *L, int32_t stack, bool* is_array, bool* is_iarray, uint64_t* hash) {
	int32_t array_size = lua_rawlen(L, stack);
	int32_t size = 0;
	*is_array = true;
	*is_iarray = true;

	lua_pushnil(L);
	while (lua_next(L, stack)) {
		size = size + 1;
		int32_t tk = lua_type(L, -2);
		switch (tk) {
			case LUA_TNUMBER: {
				if (lua_isinteger(L, -2)) {
					int64_t key = lua_tointeger(L, -2);
					if (key <= 0 || key > array_size) {
						*is_array = false;
					}
					*hash += lua_tointeger(L, -1);
				}
				else {
					lua_pop(L, 2);
					luaL_error(L, "Invalid type key(%s), expect integer got double", luaL_typename(L, -2));
				}
				break;
			}
			case LUA_TSTRING: {
				*is_array = false;
				break;
			}
			default: {
				lua_pop(L, 2);
				luaL_error(L, "Invalid type key(%s)", luaL_typename(L, -2));
				break;
			}
		}
		if (lua_isinteger(L, -1)){
			*hash += lua_tointeger(L, -1);
		}
		else {
			*is_iarray = false;
		}
		lua_pop(L, 1);
	}

	if (*is_array) {
		*is_array = array_size == size;
	}
	
	if (!*is_array){
		*is_iarray = false;
	}

	return size;
}

static
int32_t field_compare(const void* a, const void* b) {
	const qstable_value* fa = (const qstable_value*)a;
	const qstable_value* fb = (const qstable_value*)b;
	if (fa->tt != fb->tt) {
		return fa->tt == QST_TINTEGER ? -1 : 1;
	}
	return fa->idx < fb->idx ? -1 : 1;
}

static inline int32_t convert_table(lua_State *L, int32_t stack, shm_config* shmcfg, config_loader* loader);

static void write_value(lua_State *L, shm_config* shmcfg, config_loader* loader, qstable_value* value) {
	int32_t vt = lua_type(L, -1);
	switch (vt) {
		case LUA_TNUMBER: {
			if (lua_isinteger(L, -1)) {
				value->tt = QST_TINTEGER;
				value->idx = int64_index(loader, lua_tointeger(L, -1));
			}
			else {
				value->tt = QST_TNUMBER;
				value->idx = double_index(loader, lua_tonumber(L, -1));
			}
			break;
		}
		case LUA_TSTRING: {
			value->tt = QST_TSTRING;
			value->idx = string_index(loader, lua_tostring(L, -1));
				break;
			}
		case LUA_TBOOLEAN: {
			value->tt = QST_TBOOLEAN;
			value->idx = lua_toboolean(L, -1) ? 1 : 0;
			break;
		}
		case LUA_TTABLE: {
			value->tt = QST_TTABLE;
			value->idx = convert_table(L, lua_gettop(L), shmcfg, loader);
			break;
		}
		default: {
			assert(0);
			break;
		}
	}
}

static int32_t convert_table(lua_State *L, int32_t stack, shm_config* shmcfg, config_loader* loader) {
	bool is_array = false;
	bool is_iarray = false;
	uint64_t hash = 0;
	
	int32_t size = table_info(L, stack, &is_array, &is_iarray, &hash);
	int32_t mem_size = sizeof(qxtable) + sizeof(qstable_value)*size*(is_array?1:2);
	int32_t idx = loader->tb_write_idx++;
	((int32_t*)((char*)shmcfg + shmcfg->tarray_pos))[idx] = loader->write_pos;
	loader->write_pos += mem_size;

	qxtable* tb = get_table_by_index(shmcfg, idx);
	tb->tt = is_array ? QST_TT_ARRAY : QST_TT_MAP;
	tb->size = size;

	if (size <= 0) {
		return idx;
	}

	if (is_array) {
		int32_t i;
		for (i=0; i<size; i++) {
			lua_rawgeti(L, stack, i+1);
			write_value(L, shmcfg, loader, &tb->fields[i]);
			lua_pop(L, 1);
		}
		if (is_iarray) {
			qbarray* array = &loader->tarray[hash%TABLE_HNODE_SIZE];
			int32_t i;
			for (i=0; i<array->size; ++i) {
				qstable_hnode* node = (qstable_hnode*)qbarray_get(array, i);
				int32_t tbindex = node->idx;
				qxtable* cache = get_table_by_index(shmcfg, tbindex);
				if (node->hash == hash && tb->size == size && memcmp(tb, cache, mem_size) == 0) {
					loader->tb_write_idx--;
					loader->write_pos -= mem_size;
					return tbindex;
				}
			}
			qstable_hnode node;
			node.idx = idx;
			node.hash = hash;
			qbarray_push_back(array, &node);
		}
	}
	else {
		int32_t fidx = 0;
		lua_pushnil(L);
		while (lua_next(L, stack)) {
			qstable_value* value = &tb->fields[fidx++*2];
			int32_t tk = lua_type(L, -2);
			switch (tk) {
				case LUA_TNUMBER: {
					if (lua_isinteger(L, -2)) {
						int64_t key = lua_tointeger(L, -2);
						value->tt = QST_TINTEGER;
						value->idx = int64_index(loader, key);
					}
					else {
						lua_pop(L, 2);
						luaL_error(L, "Invalid type key(%d), not a integer", tk);
					}
					break;
				}
				case LUA_TSTRING: {
					value->tt = QST_TSTRING;
					value->idx = string_index(loader, lua_tostring(L, -2));
					break;
				}
				default: {
					lua_pop(L, 2);
					luaL_error(L, "Invalid type key(%d)", tk);
					break;
				}
			}
			lua_pop(L, 1);
		}

		assert(fidx == size);
		qsort(tb->fields, size, sizeof(qstable_value) * 2, field_compare);

		int32_t i;
		for (i=0; i<size; i++) {
			qstable_value* key = &tb->fields[i*2];
			qstable_value* value = &tb->fields[i*2+1];
			if (key->tt == QST_TINTEGER) {
				lua_pushinteger(L, *get_table_iarray_element(shmcfg, key->idx));
			}
			else if (key->tt == QST_TSTRING) {
				lua_pushstring(L, get_table_sarray_element(shmcfg, key->idx));
			}
			else {
				assert(0);
			}
			lua_rawget(L, -2);
			write_value(L, shmcfg, loader, value);
			lua_pop(L, 1);
		}
	}

	return idx;
}

static inline int32_t int64_compare(const void* a, const void* b) {
	return *(int64_t*)a - *(int64_t*)b;
}

static inline int32_t double_compare(const void* a, const void* b) {
	double ret = *(double*)a - *(double*)b;
	return (ret==0) ? 0 : (ret>0?1:-1);
}

static inline int32_t string_compare(const void* a, const void* b) {
	return strcmp(*(const char**)a, *(const char**)b);
}

static int32_t lnew(lua_State *L) {
	const char* name = luaL_checkstring(L, 1);
	if (!name) {
		luaL_error(L, "Invalid type name(nil)");
	}

	if (strlen(name) <= 0 || strlen(name) >= MAX_NAME_SIZE) {
		luaL_error(L, "Invalid name size(%d)", strlen(name));
	}

	uint32_t pid = luaL_checkinteger(L, 2);
	int64_t time = luaL_checkinteger(L, 3);
	luaL_checktype(L, 4, LUA_TTABLE);
	lua_settop(L, 4);

	config_loader loader[1];
	memset(loader, 0, sizeof(loader));
	
	qbarray_init(&loader->iarray, sizeof(int64_t), 0, int64_compare);
	qbarray_init(&loader->darray, sizeof(double), 0, double_compare);
	qbarray_init(&loader->sarray, sizeof(double), 0, string_compare);
	int32_t i;
	for (i=0; i<TABLE_HNODE_SIZE; i++) {
		qbarray_init(&loader->tarray[i], sizeof(qstable_hnode), 0, NULL);
	}
	table_preload(L, 4, loader);

	int32_t write_pos = sizeof(shm_config);
	int32_t idata_size = loader->iarray.size * sizeof(int64_t);
	write_pos += idata_size;
	int32_t ddata_size = loader->darray.size * sizeof(double);
	write_pos += ddata_size;
	int32_t sdata_size = loader->sarray.size * sizeof(int32_t);
	write_pos += sdata_size;
	write_pos += loader->str_data_size;
	int32_t tdata_size = loader->tb_size * sizeof(int32_t);
	write_pos += tdata_size;
	int32_t mem_size = write_pos + loader->tb_data_size;

	shm_config* shmcfg = (shm_config*)malloc(mem_size); 
	memset(shmcfg, 0, mem_size);
	shmcfg->magic = QSTABLE_MAGIC;
	snprintf(shmcfg->name, sizeof(shmcfg->name), name);
	shmcfg->pid = pid;
	shmcfg->time = time;
	shmcfg->iarray_size = loader->iarray.size;
	shmcfg->darray_size = loader->darray.size;
	shmcfg->sarray_size = loader->sarray.size;
	shmcfg->tarray_size = loader->tb_size;

	int32_t offset = sizeof(shm_config);
	shmcfg->iarray_pos = offset;
	offset += idata_size;
	shmcfg->darray_pos = offset;
	offset += ddata_size;
	shmcfg->sarray_pos = offset;
	offset += sdata_size;
	shmcfg->str_data_pos = offset;
	offset += loader->str_data_size;
	shmcfg->tarray_pos = offset;
	offset += tdata_size;
	shmcfg->table_data_pos = offset;
	loader->write_pos = write_pos;
	shmcfg->mem_size = mem_size;

	for (i=0; i<loader->iarray.size; i++) {
		*get_table_iarray_element(shmcfg, i) = *(int64_t*)qbarray_get(&loader->iarray, i);
	}

	for (i=0; i<loader->darray.size; i++) {
		*get_table_darray_element(shmcfg, i) = *(double*)qbarray_get(&loader->darray, i);
	}

	for (i=0; i<loader->sarray.size; i++) {
		const char* str = *(const char**)qbarray_get(&loader->sarray, i);
		int32_t len = strlen(str) + 1;
		memcpy((char*)shmcfg + shmcfg->str_data_pos, str, len);
		*&((int32_t*)((char*)shmcfg + shmcfg->sarray_pos))[i] = shmcfg->str_data_pos;
		shmcfg->str_data_pos += len;
		assert(shmcfg->str_data_pos <= shmcfg->tarray_pos);
	}

	convert_table(L, 4, shmcfg, loader);
	assert((loader->write_pos <= shmcfg->mem_size));

	if (shmcfg->mem_size != loader->write_pos) {
		shmcfg->mem_size = loader->write_pos;
		shmcfg = (shm_config*)realloc(shmcfg, shmcfg->mem_size);
	}

	qbarray_release(&loader->iarray);
	qbarray_release(&loader->darray);
	qbarray_release(&loader->sarray);
	for (i=0; i<TABLE_HNODE_SIZE; i++) {
		qbarray_release(&loader->tarray[i]);
	}
	
	lua_pushlightuserdata(L, shmcfg);

	return 1;
}

static int32_t lset_hash(lua_State *L) {
	shm_config* shmcfg = (shm_config*)lua_touserdata(L, 1);
	if (!shmcfg) {
		luaL_error(L, "Invalid config");
	}
	const char* hash = luaL_checkstring(L, 2);
	snprintf(shmcfg->hash, sizeof(shmcfg->hash), hash);
	return 0;
}

static int32_t lget_info(lua_State *L) {
	shm_config* shmcfg = (shm_config*)lua_touserdata(L, 1);
	if (!shmcfg) {
		luaL_error(L, "Invalid config");
	}
	lua_newtable(L);
	lua_pushstring(L, shmcfg->name);
	lua_setfield(L, -2, "name");
	lua_pushstring(L, shmcfg->hash);
	lua_setfield(L, -2, "hash");
	lua_pushinteger(L, shmcfg->pid);
	lua_setfield(L, -2, "pid");
	lua_pushinteger(L, shmcfg->time);
	lua_setfield(L, -2, "time");
	lua_pushinteger(L, shmcfg->mem_size);
	lua_setfield(L, -2, "size");
	return 1;
}

static qstable_value* find_map_value(lua_State *L, shm_config* shmcfg, qxtable* tb, const char* sk, int64_t ik, int32_t* ret_idx) {
	int32_t left = 0;
	int32_t right = tb->size-1;
	int32_t tk = sk ? QST_TSTRING : QST_TINTEGER;
	while (left <= right) {
		int32_t middle = (left + right) / 2;
		qstable_value* key = &tb->fields[middle*2];
		if (key->tt == tk) {
			int64_t ret;
			if (tk == QST_TSTRING)
				ret = strcmp(sk, get_table_sarray_element(shmcfg, key->idx));
			else
				ret = ik - *get_table_iarray_element(shmcfg, key->idx);

			if (ret < 0) {
				right = middle - 1;
			}
			else if (ret == 0) {
				if (ret_idx) {
					*ret_idx = middle;
				}
				return &tb->fields[middle*2+1];
			}
			else {
				left = middle + 1;
			}
		}
		else {
			if (key->tt < tk)
				left = middle + 1;
			else
				right = middle - 1;
		}
	}
	return NULL;
}

static int32_t push_value(lua_State *L, shm_config* shmcfg, qstable_value* value) {
	switch (value->tt) {
		case QST_TINTEGER: {
			lua_pushinteger(L, *get_table_iarray_element(shmcfg, value->idx));
			return 1;
		}
		case QST_TNUMBER: {
			lua_pushnumber(L, *get_table_darray_element(shmcfg, value->idx));
			return 1;
		}
		case QST_TBOOLEAN: {
			lua_pushboolean(L, value->idx);
			return 1;
		}
		case QST_TSTRING: {
			lua_pushstring(L, get_table_sarray_element(shmcfg, value->idx));
			return 1;
		}
		case QST_TTABLE: {
			qxtable* tb = get_table_by_index(shmcfg, value->idx);
			lua_pushnil(L);
			lua_pushlightuserdata(L, tb);
			return 2;
		}
		default:
			break;
	}
	return 0;
}

static inline int32_t try_push_value(lua_State *L, shm_config* shmcfg, qxtable* tb, const char* sk, int64_t ik) {
	if (tb->tt == QST_TT_ARRAY) {
		if (ik >= 1 && ik <= tb->size) {
			return push_value(L, shmcfg, &tb->fields[ik - 1]);
		}
		return 0;
	}
	else {
		qstable_value* value = find_map_value(L, shmcfg, tb, sk, ik, NULL);
		if (value) {
			return push_value(L, shmcfg, value);
		}
		return 0;
	}
}

shm_config* get_conf(qxtable* tb) {
	assert(tb);
	shm_config* shmcfg = NULL;
	rwlock_rlock(&g_ctx.lock);
	config tmp;
	tmp.saddr = (int64_t)tb;
	tmp.eaddr = tmp.saddr;
	config* cfg = (config*)qbarray_find(&g_ctx.sorted_cfg, &tmp);
	if (cfg) {
		shmcfg = cfg->shmcfg;
	}
	rwlock_runlock(&g_ctx.lock);
	return shmcfg;
}


static int32_t lindex(lua_State *L) {
	qxtable* tb = (qxtable*)lua_touserdata(L, 1);
	if (!tb) {
		luaL_error(L, "Invalid table");
	}

	shm_config* shmcfg = get_conf(tb);
	if (!shmcfg) {
		luaL_error(L, "Invalid config");
	}

	int32_t tk = lua_type(L, 2);
	switch (tk) {
		case LUA_TNUMBER: {
			return try_push_value(L, shmcfg, tb, NULL, lua_tointeger(L, 2));
		}
		case LUA_TSTRING: {
			return try_push_value(L, shmcfg, tb, lua_tostring(L, 2), -1);
		}
		case LUA_TNIL: {
			return 0;
		}
		default: {
			luaL_error(L, "Invalid type key(%s)", luaL_typename(L, 2));
			break;
		}
	}

	return 0;
}

static inline int32_t config_compare(const void* a, const void* b) {
	const config* cfga = (const config*)a;
	const config* cfgb = (const config*)b;
	return strcmp(cfga->name, cfgb->name);
}

static inline int32_t config_mem_compare(const void* a, const void* b) {
	const config* cfga = (const config*)a;
	const config* cfgb = (const config*)b;
	if ((cfga->saddr >= cfgb->saddr && cfga->saddr <= cfgb->eaddr)
		|| (cfgb->saddr >= cfga->saddr && cfgb->saddr <= cfga->eaddr)) {
		return 0;
	}
	return cfga->saddr > cfgb->saddr ? -1 : 1;
}

static void g_ctx_init() {
	assert(!g_ctx.inited);
	g_ctx.inited = true;
	qbarray_init(&g_ctx.cfg, sizeof(config), 0, config_compare);
	qbarray_init(&g_ctx.sorted_cfg, sizeof(config), 0, config_mem_compare);
}

static int32_t lupdate(lua_State *L) {
	if (!g_ctx.inited) {
		g_ctx_init();
	}

	luaL_checktype(L, 1, LUA_TTABLE);

	rwlock_wlock(&g_ctx.lock);
	lua_pushnil(L);
	while (lua_next(L, 1)) {
		shm_config* shmcfg = (shm_config*)lua_touserdata(L, -1);
		if (!shmcfg) {
			lua_pop(L, 2);
			luaL_error(L, "Invalid config");
		}

		config cfg;
		snprintf(cfg.name, sizeof(cfg.name), shmcfg->name);
		cfg.shmcfg = shmcfg;
		cfg.saddr = (int64_t)shmcfg;
		cfg.eaddr = cfg.saddr + shmcfg->mem_size;
		config* cache = qbarray_find(&g_ctx.cfg, &cfg);
		if (cache) {
			*cache = cfg;
		}
		else {
			qbarray_insert(&g_ctx.cfg, &cfg);
		}
		qbarray_insert(&g_ctx.sorted_cfg, &cfg);

		lua_pop(L, 1);
	}
	rwlock_wunlock(&g_ctx.lock);

	return 0;
}

static int32_t lreload(lua_State *L) {
	lua_newtable(L);

	rwlock_rlock(&g_ctx.lock);
	int32_t i;
	for (i = 0; i < g_ctx.cfg.size; ++i) {
		config* cfg = qbarray_get(&g_ctx.cfg, i);
		lua_pushstring(L, cfg->name);
		lua_pushlightuserdata(L, get_table_by_index(cfg->shmcfg, 0));
		lua_rawset(L, -3);
	}
	rwlock_runlock(&g_ctx.lock);

	return 1;
}

static int32_t lmemory(lua_State *L) {
	int32_t mem_size = 0;

	rwlock_rlock(&g_ctx.lock);
	int32_t i;
	for (i = 0; i < g_ctx.cfg.size; ++i) {
		config* cfg = qbarray_get(&g_ctx.cfg, i);
		mem_size += cfg->shmcfg->mem_size;
	}
	rwlock_runlock(&g_ctx.lock);
	lua_pushinteger(L, mem_size);

	return 1;
}

static int32_t llen(lua_State *L) {
	qxtable* tb = (qxtable*)lua_touserdata(L, 1);
	if (!tb) {
		luaL_error(L, "Invalid table");
	}

	shm_config* shmcfg = get_conf(tb);
	if (!shmcfg) {
		luaL_error(L, "Invalid config");
	}

	if (tb->tt == QST_TT_ARRAY) {
		lua_pushinteger(L, tb->size);
		return 1;
	}

	int32_t size = 0;
	int32_t i;
	for (i=1; i<=tb->size; ++i) {
		if (find_map_value(L, shmcfg, tb, NULL, i, NULL))
			++size;
		else
			break;
	}
	lua_pushinteger(L, size);

	return 1;
}

static int32_t lnext(lua_State *L) {
	qxtable* tb = (qxtable*)lua_touserdata(L, 1);
	if (!tb) {
		luaL_error(L, "Invalid tbable");
	}

	shm_config* shmcfg = get_conf(tb);
	if (!shmcfg) {
		luaL_error(L, "Invalid config");
	}

	if (tb->size <= 0) {
		return 0;
	}

	int64_t ik = 0;
	const char* sk = NULL;
	int32_t next_idx = -1;
	int32_t tk = lua_type(L, 2);

	switch (tk) {
		case LUA_TNUMBER: {
			ik = lua_tointeger(L, 2);
			break;
		}
		case LUA_TSTRING: {
			sk = lua_tostring(L, 2);
			break;
		}
		case LUA_TNIL: {
			next_idx = 0;
			break;
		}
		default: {
			luaL_error(L, "Invalid type key(%s)", luaL_typename(L, 2));
			break;
		}
	}

	if (next_idx < 0) {
		if (tb->tt == QST_TT_ARRAY) {
			if (ik >= 1 && ik < tb->size) {
				next_idx = ik;
			}
		}
		else {
			int32_t ret_idx = -1;
			find_map_value(L, shmcfg, tb, sk, ik, &ret_idx);
			if (ret_idx >= 0 && ret_idx < tb->size - 1) {
				next_idx = ret_idx + 1;
			}
		}
	}

	if (next_idx >= 0 && next_idx < tb->size) {
		qstable_value* key = NULL;
		qstable_value* value = NULL;
		if (tb->tt == QST_TT_ARRAY) {
			value = &tb->fields[next_idx];
			lua_pushinteger(L, next_idx + 1);
		}
		else {
			key = &tb->fields[next_idx*2];
			value = &tb->fields[next_idx*2+1];
			if (key->tt == QST_TSTRING) {
				lua_pushstring(L, get_table_sarray_element(shmcfg, key->idx));
			}
			else if (key->tt == QST_TINTEGER) {
				lua_pushinteger(L, *get_table_iarray_element(shmcfg, key->idx));
			}
			else {
				luaL_error(L, "Invalid next key");
			}
		}
		return push_value(L, shmcfg, value) + 1;
	}

	return 0;
}

static int32_t ltostring(lua_State *L) {
	shm_config* shmcfg = (shm_config*)lua_touserdata(L, 1);
	if (!shmcfg) {
		luaL_error(L, "Invalid config");
	}
	lua_pushlstring(L, (char*)shmcfg->name, shmcfg->mem_size - ((char*)shmcfg->name - (char*)shmcfg));
	return 1;
}

static int32_t ldelete(lua_State *L) {
	shm_config* shmcfg = (shm_config*)lua_touserdata(L, 1);
	if (!shmcfg) {
		luaL_error(L, "Invalid config");
	}
	free(shmcfg);
	return 0;
}

static int32_t lshmat(lua_State *L) {
	key_t key = luaL_checkinteger(L, 1);

	int32_t shm_id = shmget(key, 0, 0);
	if (shm_id == -1) {
		return 0;
	}

	shm_config* shconf = (shm_config*)shmat(shm_id, NULL, 0);
	if (shconf == (shm_config*)-1) {
		return 0;
	}

	struct shmid_ds shm_info;
	if (shmctl(shm_id, IPC_STAT, &shm_info) == -1) {
		return 0;
	}

	//check if valid
	int32_t size = shm_info.shm_segsz;
	if (size < sizeof(shm_config) || size != shconf->mem_size || shconf->magic != QSTABLE_MAGIC) {
		shmdt(shconf);
		return 0;
	}

	lua_pushlightuserdata(L, shconf);

	return 1;
}

static int32_t lshmdt(lua_State *L) {
	shm_config* shmcfg = (shm_config*)lua_touserdata(L, 1);
	if (!shmcfg) {
		luaL_error(L, "Invalid config");
	}
	shmdt(shmcfg);
	return 0;
}

static int32_t lshmrmid(lua_State *L) {
	key_t key = luaL_checkinteger(L, 1);
	int32_t shm_id = shmget(key, 0, 0);
	if (shm_id == -1) {
		return 0;
	}
	shmctl(shm_id, IPC_RMID, NULL);
	return 0;
}

static int32_t lshminfo(lua_State *L) {
	key_t key = luaL_checkinteger(L, 1);
	int32_t shm_id = shmget(key, 0, 0);
	if (shm_id != -1) {
		struct shmid_ds shm_info;
		if (shmctl(shm_id, IPC_STAT, &shm_info) != -1) {
			lua_newtable(L);
			lua_pushinteger(L, shm_id);
			lua_setfield(L, -2, "shm_id");
			lua_pushinteger(L, shm_info.shm_segsz);
			lua_setfield(L, -2, "shm_segsz");
			lua_pushinteger(L, shm_info.shm_nattch);
			lua_setfield(L, -2, "shm_nattch");
			return 1;
		}
	}
	return 0;
}

static int32_t lshmsave(lua_State *L) {
	shm_config* shmcfg = (shm_config*)lua_touserdata(L, 1);
	if (!shmcfg) {
		luaL_error(L, "Invalid config");
	}
	key_t key = luaL_checkinteger(L, 2);

	int32_t shm_id = shmget(key, shmcfg->mem_size, IPC_CREAT | IPC_EXCL | 0644);
	if (shm_id == -1) {
		if (errno == 17) {
			return 0;
		}
		else {
			luaL_error(L, "Shmget error(%d)", errno);
		}
	}

	shm_config* shconf = (shm_config*)shmat(shm_id, NULL, 0);
	if (shconf == (shm_config*)-1) {
		return 0;
	}
	memcpy(shconf, shmcfg, shmcfg->mem_size);
	
	lua_pushlightuserdata(L, shconf);
	lua_pushinteger(L, shmcfg->mem_size);
	return 2;
}

static int32_t lget_conf(lua_State *L) {
	qxtable* tb = (qxtable*)lua_touserdata(L, 1);
	if (!tb) {
		luaL_error(L, "Invalid tbable");
	}
	shm_config* shmcfg = get_conf(tb);
	if (!shmcfg) {
		luaL_error(L, "Invalid config");
	}
	lua_pushlightuserdata(L, shmcfg);
	return 1;
}

static int32_t ltimemillis(lua_State *L) {
	struct timeval tv;
    gettimeofday(&tv, NULL);
    uint64_t timestamp = (uint64_t)tv.tv_sec*1000 + tv.tv_usec/1000;
    lua_pushinteger(L, timestamp);
	return 1;
}

int32_t  luaopen_qstable_core(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "index",			lindex },
		{ "len",			llen },
		{ "next",			lnext },
		{ "new",			lnew },
		{ "set_hash",		lset_hash },
		{ "get_info",		lget_info },
		{ "update",			lupdate },
		{ "reload",			lreload },
		{ "tostring",		ltostring },
		{ "delete",			ldelete },
		{ "shmat",			lshmat },
		{ "shmdt",			lshmdt },
		{ "shmrmid",		lshmrmid },
		{ "shminfo",		lshminfo },
		{ "shmsave",		lshmsave },
		{ "get_conf",		lget_conf },
		{ "memory",			lmemory },
		{ "timemillis",		ltimemillis },
		{ NULL, NULL },
	};
	luaL_newlib(L, l);
	return 1;
}