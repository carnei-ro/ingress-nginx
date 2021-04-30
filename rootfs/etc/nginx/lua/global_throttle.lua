local resty_global_throttle = require("resty.global_throttle")
local util = require("util")

local ngx = ngx
local ngx_exit = ngx.exit
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_INFO = ngx.INFO

local _M = {}

local DECISION_CACHE = ngx.shared.global_throttle_cache

-- it does not make sense to cache decision for too little time
-- the benefit of caching likely is negated if we cache for too little time
-- Lua Shared Dict's time resolution for expiry is 0.001.
local CACHE_THRESHOLD = 0.001

local DEFAULT_RAW_KEY = "remote_addr"

local function should_ignore_request(ignored_header)
  -- { "header-name", "header-value01", "header-value02", "header-valueN" }
  if not ignored_header or #ignored_header < 2 then
    -- we expect the header name then header values
    return false
  end

  local header_value = ngx.req.get_headers()[ignored_header[1]]
  -- if header value does not exists perform rate limit evaluation
  if not header_value then
    return false
  end
  
  for i = 2, #ignored_header do
    -- if match, ignore this request for rate limit eval
    if ignored_header[i] == header_value then
      return true
    end
  end

  return false
end

local function is_enabled(config, location_config)
  if config.memcached.host == "" or config.memcached.port == 0 then
    return false
  end
  if location_config.limit == 0 or
    location_config.window_size == 0 then
    return false
  end

  -- Hack:
  --  ignore rate limiting based on cidrs would not work for me
  --  because all instances belong to the same network.
  --  The hack consists in evaluate a Header and possible values
  --
  -- ignored_header will be a comma separated list, but
  --  the first value it is the Header Name and the other values
  --  are the possible match values
  if should_ignore_request(location_config.ignored_header) then
    return false
  end

  return true
end

local function get_namespaced_key_value(namespace, key_value)
  return namespace .. key_value
end

function _M.throttle(config, location_config)
  if not is_enabled(config, location_config) then
    return
  end

  local key_value = util.generate_var_value(location_config.key)
  if not key_value or key_value == "" then
    key_value = ngx.var[DEFAULT_RAW_KEY]
  end

  local namespaced_key_value =
    get_namespaced_key_value(location_config.namespace, key_value)

  local is_limit_exceeding = DECISION_CACHE:get(namespaced_key_value)
  if is_limit_exceeding then
    ngx.var.global_rate_limit_exceeding = "c"
    return ngx_exit(config.status_code)
  end

  local my_throttle, err = resty_global_throttle.new(
    location_config.namespace,
    location_config.limit,
    location_config.window_size,
    {
      provider = "memcached",
      host = config.memcached.host,
      port = config.memcached.port,
      connect_timeout = config.memcached.connect_timeout,
      max_idle_timeout = config.memcached.max_idle_timeout,
      pool_size = config.memcached.pool_size,
    }
  )
  if err then
    ngx.log(ngx.ERR, "faled to initialize resty_global_throttle: ", err)
    -- fail open
    return
  end

  local desired_delay, estimated_final_count
  estimated_final_count, desired_delay, err = my_throttle:process(key_value)
  if err then
    ngx.log(ngx.ERR, "error while processing key: ", err)
    -- fail open
    return
  end

  if desired_delay then
    if desired_delay > CACHE_THRESHOLD then
      local ok
      ok, err =
        DECISION_CACHE:safe_add(namespaced_key_value, true, desired_delay)
      if not ok then
        if err ~= "exists" then
          ngx_log(ngx_ERR, "failed to cache decision: ", err)
        end
      end
    end

    ngx.var.global_rate_limit_exceeding = "y"
    ngx_log(ngx_INFO, "limit is exceeding for ",
      location_config.namespace, "/", key_value,
      " with estimated_final_count: ", estimated_final_count)

    return ngx_exit(config.status_code)
  end
end

return _M
