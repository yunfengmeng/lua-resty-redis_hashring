# Name

lua-resty-redis_hashring - lua redis hash consistency client driver implement for the ngx_lua based on the cosocket API
# Status

This library is considered production ready.
# Description

This Lua library is a redis hash consistency client driver implement for the ngx_lua nginx module:
# Synopsis

```
local conf = {
    name    = "test",
    servers = {
        -- like nginx upstream
        { server = "127.0.0.1:6378", weight = 1, max_fails = 5, fail_timeout = 60 }, -- fail_timeout(s)
        { server = "127.0.0.1:6379", weight = 1, max_fails = 5, fail_timeout = 60 },
        { server = "127.0.0.1:6380", weight = 1, max_fails = 1, fail_timeout = 60, backup = true},
    },
    password        = "",
    timeout         = 3000,
    idle_timeout    = 1000,
    pool_size       = 200,
}

local cjson = require "cjson"
local redis_hashring = require "resty.redis_hashring"

local hredis = redis_hashring:new(conf)

hredis:init_pipeline()
hredis:mset('a', 1, 'b', 2, 'c', 3)
hredis:msetnx('a1', 1, 'b2', 2, 'c3', 3)
hredis:mget('a','b','c')
hredis:del('a','b','c')
local res, err = hredis:commit_pipeline()

ngx.say(cjson.encode(res))
```
# Requires

1. lua-resty-redis: [https://github.com/openresty/lua-resty-redis](https://github.com/openresty/lua-resty-redis "https://github.com/openresty/lua-resty-redis")
# TODO

# See Also

