-- Copyright (C) yuewenhu
-- Copyright (C) yunfengmeng

local redis        = require "resty.redis"
local hashring     = require "resty.hashring"
local concat       = table.concat
local insert       = table.insert
local type         = type
local pairs        = pairs
local pcall        = pcall
local ipairs       = ipairs
local unpack       = unpack
local time         = ngx.time
local null         = ngx.null
local ngx_log      = ngx.log
local ERR          = ngx.ERR
local re_match     = ngx.re.match
local setmetatable = setmetatable


local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end


local commands = {
    "append",               --[["auth",]]           --[["bgrewriteaof",]]
    --[["bgsave",]]         "bitcount",             --[["bitop",]]
    "blpop",                "brpop",
    --[["brpoplpush",]]     --[["client",]]         --[["config",]]
    --[["dbsize",]]
    --[["debug",]]          "decr",                 "decrby",
    --[["del",]]            --[["discard",]]        --[["dump",]]
    --[["echo",]]
    --[["eval",]]           --[["exec",]]           "exists",
    "expire",               "expireat",             --[["flushall",]]
    --[["flushdb",]]        "get",                  "getbit",
    "getrange",             "getset",               "hdel",
    "hexists",              "hget",                 "hgetall",
    "hincrby",              "hincrbyfloat",         "hkeys",
    "hlen",
    "hmget",                "hmset",                --[["hscan",]]
    "hset",
    "hsetnx",               "hvals",                "incr",
    "incrby",               "incrbyfloat",          --[["info",]]
    --[["keys",]]
    --[["lastsave",]]       "lindex",               "linsert",
    "llen",                 "lpop",                 "lpush",
    "lpushx",               "lrange",               "lrem",
    "lset",                 "ltrim",                --[["mget",]]
    --[["migrate",]]
    --[["monitor",]]        --[["move",]]           --[["mset",]]
    --[["msetnx",]]         --[["multi",]]          --[["object",]]
    "persist",              "pexpire",              "pexpireat",
    --[["ping",]]           "psetex",               --[["psubscribe",]]
    "pttl",
    --[["publish",]]        --[["punsubscribe",]]   --[["pubsub",]]
    --[["quit",]]
    --[["randomkey",]]      --[["rename",]]         --[["renamenx",]]
    --[["restore",]]
    "rpop",                 --[["rpoplpush",]]      "rpush",
    "rpushx",               "sadd",                 --[["save",]]
    "scan",                 "scard",                --[["script",]]
    --[["sdiff",]]          --[["sdiffstore",]]
    --[["select",]]         "set",                  "setbit",
    "setex",                "setnx",                "setrange",
    --[["shutdown",]]       --[["sinter",]]         --[["sinterstore",]]
    --[["sismember",]]      --[["slaveof",]]        --[["slowlog",]]
    "smembers",             --[["smove",]]          "sort",
    "spop",                 --[["srandmember",]]    "srem",
    "sscan",
    --[["strlen",]]         --[["subscribe",]]      --[["sunion",]]
    --[["sunionstore",]]    --[["sync",]]           --[["time",]]
    "ttl",
    --[["type",]]           --[["unsubscribe",]]    --[["unwatch",]]
    --[["watch",]]          "zadd",                 "zcard",
    "zcount",               "zincrby",              --[["zinterstore",]]
    "zrange",               "zrangebyscore",        "zrank",
    "zrem",                 "zremrangebyrank",      "zremrangebyscore",
    "zrevrange",            "zrevrangebyscore",     "zrevrank",
    --[["zscan",]]
    "zscore",               --[["zunionstore",]]    --[["evalsha"]]
}


local _M = {}

local cache_nodes = {}


-- server like: "127.0.0.1:6379"
local function _connect(self, server)

    local red = redis:new()

    if self.config.timeout > 0 then
        red:set_timeout(self.config.timeout)
    end

    local m, err = re_match(server, "([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})\\s*:\\s*([0-9]{1,5})", "jo")
    if not m then
        return nil
    end

    local host, port = m[1], m[2]
    local ok, err = red:connect(host, port)
    if not ok then
        ngx_log(ERR, server, ", connect err: ", err)

        local node = cache_nodes[self.config.name]["_servers"][server]
        if not node._fail_time or (time() - node._fail_time) > node.fail_timeout then
            node._max_fails = 0
            node._fail_time = time()
        end

        node._max_fails = node._max_fails + 1

        if (time() - node._fail_time) <= node.fail_timeout and node._max_fails >= node.max_fails then
            node._down_time = time()
            
            ngx_log(ERR, server, ", is down, _max_fails: ", node._max_fails, ", fail_timeout: ", node.fail_timeout, "s")
        end

        return nil, err
    end

    if self.config.password ~= "" then
        local ok, err = red:auth(self.config.password)
        if not ok then
            ngx_log(ERR, server, ", auth err: ", err)
        end
    end

    return red
end


-- pools connection pool
-- return server, red, err
local function _fetch_server(self, key, pools)

    local pools = pools or {}

    local servers = cache_nodes[self.config.name]["_servers"]
    local rings   = cache_nodes[self.config.name]["_rings"]
    local changed = {}
    for _, s in pairs(servers) do
        
        local i = s.backup and 2 or 1 -- primary servers first
        if not rings[i] then
            rings[i] = hashring:new()
        end
        
        -- 1. Have not failed
        -- 2. The number of failures is not reached
        -- 3. Failed timeout is not reached
        if not s._max_fails or (s._max_fails < s.max_fails) or (s._down_time and (time() - s._down_time) > s.fail_timeout) then
            if rings[i]:add_node(s.server, s.weight, true) > 0 then
                changed[i] = true
            end
        else
            rings[i]:remove_node(s.server)
        end
    end
    
    -- if ring changes, re-sorting
    for i, ring in pairs(rings) do
        if changed[i] then
            ring:sort_pos()
        end
    end
    
    for _, ring in pairs(rings) do
        while true do
            local node = ring:get_node(key)
            if not node then
                break
            end
            if not pools[node] then
                local red, err = _connect(self, node)
                if red then
                    pools[node] = red
                end
            end

            if pools[node] then
                return node, pools[node]
            else
                ring:remove_node(node)
            end
        end
    end
    return "0.0.0.0:0", nil, "no server available"
end


-- return res, err
local function _do_cmd(self, cmd, key, ...)

    if self._reqs then -- use pipeline
        if not self[cmd] then
            return false, "cmd not found"
        end

        insert(self._group, { size = 1 })
        insert(self._reqs, { cmd = cmd, key = key, args = { ... } })
        return true
    end

    local _, red, err = _fetch_server(self, key)
    if err then
        return false, err
    end

    local res, err = red[cmd](red, key, ...)

    red:set_keepalive(self.config.idle_timeout, self.config.pool_size)
    return res, err
end


for i = 1, #commands do
    local cmd = commands[i]
    _M[cmd] = function (self, ...)
        return _do_cmd(self, cmd, ...)
    end
end


function _M.init_pipeline(self)
    self._reqs  = {}
    self._group = {}
end


function _M.cancel_pipeline(self)
    self._reqs  = nil
    self._group = nil
end


local mcommands = {
    mset = {
        cmd  = "set",
        func = function(i, item, res) -- return ok
            local data = "OK"
            for j = i + 1, i + item.size do
                if type(res[j]) == "table" and res[j][1] == false then
                    data = res[j]
                    break
                end
            end
            return data
        end
    },
    msetnx = {
        cmd  = "setnx",
        func = function(i, item, res) -- return 1 or 0
            local data = 1
            for j = i + 1, i + item.size do
                if (type(res[j]) == "number" and res[j] < 1) or (type(res[j]) == "table" and res[j][1] == false) then
                    data = res[j]
                    break
                end
            end
            return data
        end
    },
    mget = {
        cmd  = "get",
        func = function(i, item, res) -- return list
            local data = {}
            for j = i + 1, i + item.size do
                data[#data + 1] = res[j]
            end
            return data
        end
    },
    del = {
        cmd  = "del",
        func = function(i, item, res) -- reutrn affected
            local data = 0
            for j = i + 1, i + item.size do
                if type(res[j]) == "number" and res[j] > 0 then
                    data = data + res[j]
                elseif type(res[j]) == "table" and res[j][1] == false then
                    data = res[j]
                    break
                end
            end
            return data
        end
    }
}


local function _merge(group, res)
    local ret = {}
    local i   = 0
    for _, item in ipairs(group) do
    
        local data
        
        if item.mcmd and mcommands[item.mcmd] then
            data = mcommands[item.mcmd].func(i, item, res)
        else
            data = res[i + 1]
        end

        i = i + item.size
        ret[#ret + 1] = data
    end

    return ret
end


-- red redis connecting object
-- return res, err
local function _commit(self, red, reqs)

    if not red then
        return nil, "no redis"
    end

    red:init_pipeline()
    for _, req in pairs(reqs) do
        if req.args and #req.args > 0 then
            red[req.cmd](red, req.key, unpack(req.args))
        else
            red[req.cmd](red, req.key)
        end
    end

    local result, err = red:commit_pipeline()
    
    if err then
        return nil, err
    end

    red:set_keepalive(self.config.idle_timeout, self.config.pool_size)

    return result, nil
end


function _M.commit_pipeline(self)
    local reqs  = self._reqs
    local group = self._group
    if not reqs then
        return nil, "no pipeline"
    end

    self._reqs  = nil
    self._group = nil

    local servers = {}
    local pools   = {}
    for idx, req in ipairs(reqs) do

        local node = _fetch_server(self, req.key, pools)
        if not servers[node] then
            servers[node] = { reqs = {}, idxs = {} }
        end

        local mreqs = servers[node].reqs
        local midxs = servers[node].idxs
        mreqs[#mreqs + 1] = req
        midxs[#midxs + 1] = idx
    end

    local ret = new_tab(0, #reqs)

    for node, item in pairs(servers) do
    
        local red = pools[node] or nil
        local res, err = _commit(self, red, item.reqs) -- res "nil" only "no redis" or "commit error"

        for i, idx in ipairs(item.idxs) do
            ret[idx] = res and res[i] or { false, err } -- see: https://github.com/openresty/lua-resty-redis
        end
    end

    -- merge results
    if #group > 0 then
        ret = _merge(group, ret)
    end

    return ret
end


-- like: mget mset msetnx del
local function _do_mcmd(self, mcmd, cmd, ...)

    local pipeline = self._reqs and true or nil
    if not pipeline then
        _M.init_pipeline(self)
    end

    local args = { ... }
    if mcmd == "mset" or mcmd == "msetnx" then -- k1, v1, k2, v2
        insert(self._group, { size = #args / 2, mcmd = mcmd })
        for i = 1, #args, 2 do
            insert(self._reqs, { cmd = cmd, key = args[i], args = { args[i + 1] } })
        end
    else
        insert(self._group, { size = #args, mcmd = mcmd })
        for _, key in ipairs(args) do
            insert(self._reqs, { cmd = cmd, key = key })
        end
    end

    if not pipeline then
        local res, err = _M.commit_pipeline(self)
        if not res then
            return res, err
        end

        if type(res[1]) == "table" and res[1][1] == false then
            return unpack(res[1])
        else
            return res[1]
        end
    end
end


for mcmd, item in pairs(mcommands) do
    local cmd = item.cmd
    _M[mcmd] = function (self, ...)
        return _do_mcmd(self, mcmd, cmd, ...)
    end
end


function _M.new(self, conf)
    local config = {
        name         = conf.name or "redis_hash",
        servers      = conf.servers,
        password     = conf.password,
        timeout      = conf.timeout,
        idle_timeout = conf.idle_timeout or 1000,
        pool_size    = conf.pool_size or 100,
    }

    local mt = setmetatable({ config = config }, { __index = _M })

    if not cache_nodes[config.name] then
        cache_nodes[config.name] = { _servers = {}, _rings = {}}

        for _, s in pairs(config.servers) do
            cache_nodes[config.name]["_servers"][s.server] = s
        end
    end

    return mt
end


return _M

