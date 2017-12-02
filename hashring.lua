-- Copyright (C) yunfengmeng
-- A Small LUA Library Which Implements Consistent Hashing

local crc32        = ngx.crc32_short
local tbl_sort     = table.sort
local tbl_insert   = table.insert
local tbl_remove   = table.remove
local str_sub      = string.sub
local lower        = string.lower
local pairs        = pairs
local ipairs       = ipairs
local next         = next
local type         = type
local tonumber     = tonumber
local re_match     = ngx.re.match
local md5          = ngx.md5
local setmetatable = setmetatable


local _M = {}


-- Uses CRC32/MD5 to hash a value into a 32bit int
local function _hash(self, str)
    if lower(self.hasher) == "crc32" then
        return crc32(str)
    else
        return tonumber(str_sub(md5(str), 1, 8), 16) --md5 to int32
    end
end


-- Sorts the ring position
function _M.sort_pos(self)
    return tbl_sort(self._pos, function(a, b) return a < b end)
end


-- Add node
-- weight number suggest range: 0 - 100
-- return The number of nodes that were added
function _M.add_node(self, node, weight, lazy_sort)

    if self._nodes[node] then
        return 0
    end
    
    weight = tonumber(weight) or 1

    self._nodes[node] = {}
    for i = 1, (self.vnode * weight) do
        local pos = _hash(self, node .. "-" .. i) -- like: 127.0.0.1-1
        if not self._ring[pos] then
            self._ring[pos] = node
            tbl_insert(self._pos, pos)
            tbl_insert(self._nodes[node], pos)
        end
    end

    if not lazy_sort then -- lazy sort ?
        self:sort_pos()
    end

    return 1
end


-- Remove node
-- return The number of nodes that were removed
function _M.remove_node(self, node)

    if not self._nodes[node] then
        return 0
    end

    for _, pos in pairs(self._nodes[node]) do -- delete virtual nodes

        self._ring[pos] = nil

        for k, v in pairs(self._pos) do
            if pos == v then
                tbl_remove(self._pos, k)
                break
            end
        end
    end
    
    self._nodes[node] = nil

    return 1
end


-- Get node
-- Support Hash Tag
function _M.get_node(self, key)

    if #self._pos < 1 then
        return nil
    end
    
    local m, err = re_match(key, "\\{(.+?)\\}", "jo") -- Match Hash Tag
    if m then
        key = m[1]
    end

    local pos  = _hash(self, key)
    local _, i = next(self._pos)
    
    for _, j in ipairs(self._pos) do
        if pos <= j then
            i = j
            break
        end
    end
    
    return self._ring[i]
end


-- return all nodes
function _M.get_all_nodes(self)
    
    local nodes = {}
    for node, _ in pairs(self._nodes) do
        tbl_insert(nodes, node)
    end
    
    return nodes
end


-- nodes   table like: { '127.0.0.1:6379', '127.0.0.1:6380' }
-- options table like: { vnode = 64, k = v ... }
function _M.new(self, nodes, options)
    local nodes   = nodes or {}
    local options = options or {}

    local _t = {
        vnode  = 64, -- Number of virtual nodes
        hasher = 'md5',
        _nodes = {},
        _ring  = {},
        _pos   = {},
    }

    for k, v in pairs(options) do
        _t[k] = v
    end

    local mt = setmetatable(_t, { __index = _M })

    if #nodes > 0 then
        for _, node in pairs(nodes) do
            mt:add_node(node, 1, true)
        end
        mt:sort_pos()
    end

    return mt
end


return _M

