local c = require "qstable.core"
local md5 = require "md5"

local setmetatable = setmetatable
local rawset = rawset
local pairs = pairs
local select = select
local assert = assert
local tonumber = tonumber

local c_index = c.index
local c_len = c.len
local c_next = c.next

local cache_config = {}
local config_hash = {}
local meta = {}
local weak_meta = {__mode = "v"}
local pid
local shm_key_serial = 1

local _M = {}

local function wrap_root(name, ud)
    return setmetatable({__ud = assert(ud), __name = name}, meta)
end

local function wrap_table(ud)
    return setmetatable({__ud = assert(ud)}, meta)
end

local function qstable_next(t, k)
    local nextk,nextv,nextud = c_next(t.__ud, k)
    if nextud then
        return nextk,t[nextk]
    else
        return nextk,nextv
    end
end

meta.__index = function(t, k)
    local v,udata= c_index(t.__ud, k)
    if udata then
        local wrap_tablevalue = wrap_table(udata)
        rawset(t, k, wrap_tablevalue)
        return wrap_tablevalue
    else
        return v
    end
end

meta.__newindex = function(t, k, v)
    error("Cannot modify a read-only table")
end

meta.__len = function(t)
    return c_len(t.__ud)
end

meta.__pairs = function(t)
    return qstable_next,t,nil
end

meta.__ipairs = function(t)
    return qstable_next,t,nil
end

for k,v in pairs(meta) do
    weak_meta[k] = v
end

local function calc_hash(conf)
    assert(conf)
    local str = c.tostring(conf)
    local h = md5.sumhexa(str)
    return h
end

function _M.reload()
    local ud_list = c.reload()
    for name,ud in pairs(ud_list) do
        local cache = cache_config[name]
        if not cache or cache.__ud ~= ud then
            cache_config[name] = wrap_root(name, ud)
            local info = c.get_info(c.get_conf(ud))
            config_hash[name] = info.hash
        end
    end
end

function _M.find(name, ...)
    local conf = cache_config[name]
    if not conf then
        return
    end
    local val = conf
    local n = select("#", ...)
    for i =1,n do
        val = val[select(i, ...)]
        if val == nil then
            return nil
        end
    end
    return val
end

local function gen_shm_key()
    local shm_key = math.floor(c.timemillis()/1000) + shm_key_serial
    shm_key_serial = shm_key_serial + 1
    return shm_key
end

local function get_shm_nattch(shm_key)
    local info = c.shminfo(shm_key)
    return info and info.shm_nattch or -1
end

local function get_pid()
    if not pid then
        local handle = io.popen("echo $$")
        pid = tonumber(handle:read("*a"))
        handle:close()
    end
    return pid
end

local function get_shm_infos(cache_infos)
    local f = io.open("/proc/sysvipc/shm", "r")
    local text = f:read("*a")
    local shm_infos = {}
    f:close()
    
    local function get_name_infos(name)
        local infos = shm_infos[name]
        if not infos then
            infos = {}
            shm_infos[name] = infos
        end
        return infos
    end

    for line in text:gmatch("[^\r\n]+") do
        local shm_key = line:match("%s*(%d+)%s+.*")
        shm_key = tonumber(shm_key)
        if shm_key and shm_key > 0 then
            local info = cache_infos[shm_key]
            if not info then
                local shminfo = c.shminfo(shm_key)
                if shminfo then
                    local shm_conf = c.shmat(shm_key)
                    if shm_conf then
                        info = c.get_info(shm_conf)
                        if info.size == shminfo.shm_segsz then
                            info.shm_key = shm_key
                            table.insert(get_name_infos(info.name), info)
                            cache_infos[shm_key] = info
                        end
                        c.shmdt(shm_conf)
                    end
                end
            else
                table.insert(get_name_infos(info.name), info)
            end
        end
    end

    for name,infos in pairs(shm_infos) do
        table.sort(infos, function(a, b)
            if a.time ~= b.time then
                return a.time < b.time
            else
                return a.pid < b.pid
            end
        end)
    end

    return shm_infos
end

function _M.update(confs)
    _M.reload()

    local random_confs = {}
    for name,data in pairs(confs) do
        table.insert(random_confs, {name = name, data = data})
    end

    local count = #random_confs
    for i=1,count-1 do
        local index = math.random(i, count)
        if i ~= index then
            local tmp = random_confs[i]
            random_confs[i] = random_confs[index]
            random_confs[index] = tmp
        end
    end

    local pid = get_pid()
    local shm_confs = {}
    local cache_infos = {}
    local shm_infos = get_shm_infos(cache_infos)
    local new_shm_infos = {}

    for index,v in pairs(random_confs) do
        local name = v.name
        local data = v.data
        local conf = c.new(name, pid, c.timemillis(), data)
        local hash = calc_hash(conf)
        c.set_hash(conf, hash)
        
        --query form local
        if config_hash[name] == hash then
            c.delete(conf)
        else
            --query from shared memory
            local found
            config_hash[name] = hash
            local infos = shm_infos[name]
            if infos then
                for _,info in pairs(infos) do
                    if info.hash == hash then
                        local shm_conf = c.shmat(info.shm_key)
                        if shm_conf then
                            if c.get_info(shm_conf).hash == hash then
                                shm_confs[name] = shm_conf
                                c.delete(conf)
                                found = true
                                break
                            else
                                c.shmdt(shm_conf)
                            end
                        end
                    end
                end
            end
            --new config
            if not found then
                local new_info,shm_conf
                for i=1,65535 do
                    local shm_key = gen_shm_key()
                    shm_conf = c.shmsave(conf, shm_key)
                    if shm_conf then
                        shm_key = shm_key
                        new_info = {
                            hash = hash,
                            shm_key = shm_key,
                            shm_conf = shm_conf,
                            pid = pid,
                        }
                        new_shm_infos[name] = new_info
                        break
                    end
                end
                c.delete(conf)
                if not shm_conf then
                    error("shmsave fail " .. name)
                end
                new_info.time = c.timemillis()
            end
        end
    end

    --concurrent config loading processing
    shm_infos = get_shm_infos(cache_infos)
    for name,new_info in pairs(new_shm_infos) do
        local found
        local infos = shm_infos[name]
        if infos then
            for _,info in pairs(infos) do
                if info.hash == new_info.hash then
                    local shm_conf = c.shmat(info.shm_key)
                    if shm_conf then
                        if c.get_info(shm_conf).hash == new_info.hash then
                            if info.time <= new_info.time then
                                if info.time < new_info.time or info.pid < new_info.pid then
                                    found = true
                                    shm_confs[name] = shm_conf
                                    break
                                else
                                    c.shmdt(shm_conf)
                                end
                            else
                                c.shmdt(shm_conf)
                            end
                        else
                            c.shmdt(shm_conf)
                        end
                    end
                end
            end
        end

        if found then
            c.shmdt(new_info.shm_conf)
        else
            shm_confs[name] = new_info.shm_conf
        end
    end

    if next(shm_confs) then
        c.update(shm_confs)
    end

    --release shared memory when reference equals 0
    shm_infos = get_shm_infos(cache_infos)
    for name,infos in pairs(shm_infos) do
         for _,info in pairs(infos) do
            if get_shm_nattch(info.shm_key) == 0 then
                c.shmrmid(info.shm_key)
            end
        end
    end
end

function _M.md5(tb)
    if type(tb) == "string" then
        return md5.sumhexa(tb)
    elseif type(tb) == "table" then
        local conf = c.new("tmp", get_pid(), c.timemillis(), tb)
        local hash = calc_hash(conf)
        c.delete(conf)
        return hash
    else
        error("invalid type table " .. type(tb))
    end
end

function _M.memory()
    return c.memory()
end

function _M.gc()
    for _,cache in pairs(cache_config) do
        setmetatable(cache, weak_meta)
    end
    collectgarbage("collect")
    for _,cache in pairs(cache_config) do
        setmetatable(cache, meta)
    end
end

return _M
