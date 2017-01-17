--[[

  Copyright (C) 2017 Masatoshi Teruya

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.

  nsqpp.lua
  lua-nsqpp
  Created by Masatoshi Teruya on 17/01/12.

--]]

--- file scope variables
local util = require('nsqpp.util');
local htonl = util.htonl;
local decodeframe = util.decodeframe;
local floor = math.floor;
local huge = math.huge;
local concat = table.concat;
--- constants
local FRAME_INVAL = util.FRAME_INVAL;
local FRAME_PARTIAL = util.FRAME_PARTIAL;
local FRAME_RESPONSE = util.FRAME_RESPONSE;
local FRAME_ERROR = util.FRAME_ERROR;
local FRAME_MESSAGE = util.FRAME_MESSAGE;
local EAGAIN = -2;
local EINVAL = -1;
local RESPONSE = 0;
local ERROR = 1;
local MESSAGE = 2;


--- helper functions

--- verifyInt
-- @param num
-- @return ok
local function verifyInt( num )
    return type( num ) == 'number' and floor( num ) == num;
end


--- verifyUInt
-- @param num
-- @return ok
local function verifyUInt( num )
    return verifyInt( num ) and num >= 0 and num < huge;
end


--- verifyName
-- @param name
-- @return ok
local function verifyName( name )
    if type( name ) == 'string' and #name > 0 and #name <= 64 then
        local head, tail = name:find('^[a-zA-Z0-9_.-]+');

        if head then
            return tail == #name or name:find('^#ephemeral$', tail + 1 ) ~= nil;
        end
    end

    return false;
end


--- defaults
local DEFAULT_IDENT = {
    --  an identifier used to disambiguate this client
    --  (ie. something specific to the consumer)
    client_id = {
        isa = 'boolean',
        def = false
    },

    --  the hostname where the client is deployed
    hostname = {
        isa = 'string',
        def = 'localhost'
    },

    --  (nsqd v0.2.19+)
    --  bool used to indicate that the client supports feature negotiation.
    --  If the server is capable, it will send back a JSON payload of supported
    --  features and metadata.
    feature_negotiation = {
        isa = 'boolean',
        def = false
    },

    --  (nsqd v0.2.19+)
    --  milliseconds between heartbeats.
    --  Valid range:
    --      1000 <= heartbeat_interval <= configured_max
    --      (-1 disables heartbeats)
    --  Defaults to --client-timeout / 2
    heartbeat_interval = {
        isa = 'number',
        def = 0,
        chk = function( val )
            return verifyInt( val ) and val == 0 or val == -1 or val >= 1000;
        end
    },

    --  (nsqd v0.2.21+)
    --  the size in bytes of the buffer nsqd will use when writing to this
    --  client.
    --  Valid range:
    --      64 <= output_buffer_size <= configured_max
    --      (-1 disables output buffering)
    --  --max-output-buffer-size (nsqd flag) controls the max
    --  Defaults to 16kb
    output_buffer_size = {
        isa = 'number',
        def = 0,
        chk = function( val )
            return verifyInt( val ) and val == 0 or val == -1 or val >= 64;
        end
    },

    --  (nsqd v0.2.21+)
    --  the timeout after which any data that nsqd has buffered will be flushed
    --  to this client.
    --  Valid range:
    --      1ms <= output_buffer_timeout <= configured_max
    --      (-1 disables timeouts)
    --  --max-output-buffer-timeout (nsqd flag) controls the max
    --  Defaults to 250ms
    --  Warning:
    --      configuring clients with an extremely low (< 25ms)
    --      output_buffer_timeout has a significant effect on nsqd CPU usage
    --      (particularly with > 50 clients connected).
    --      This is due to the current implementation relying on Go timers
    --      which are maintained by the Go runtime in a priority queue.
    output_buffer_timeout = {
        isa = 'number',
        def = 0,
        chk = function( val )
            return verifyInt( val ) and val == 0 or val == -1 or val >= 1;
        end
    },

    --  (nsqd v0.2.22+)
    --  enable TLS for this connection.
    --  --tls-cert and --tls-key (nsqd flags) enable TLS and configure the
    --  server certificate
    --  If the server supports TLS it will reply "tls_v1": true
    --  The client should begin the TLS handshake immediately after reading
    --  the IDENTIFY response
    --  The server will respond OK after completing the TLS handshake
    tls_v1 = {
        isa = 'boolean',
        def = false
    },

    --  (nsqd v0.2.23+)
    --  enable snappy compression for this connection.
    --  --snappy (nsqd flag) enables support for this server side
    --  The client should expect an additional, snappy compressed OK response
    --  immediately after the IDENTIFY response.
    snappy = {
        isa = 'boolean',
        def = false
    },

    --  (nsqd v0.2.23+)
    --  enable deflate compression for this connection.
    --  --deflate (nsqd flag) enables support for this server side
    --  The client should expect an additional, deflate compressed OK response
    --  immediately after the IDENTIFY response.
    --  A client cannot enable both snappy and deflate.
    deflate = {
        isa = 'boolean',
        def = false
    },

    --  (nsqd v0.2.23+)
    --  configure the deflate compression level for this connection.
    --  --max-deflate-level (nsqd flag) configures the maximum allowed value
    --  Valid range:
    --      1 <= deflate_level <= configured_max
    --  Higher values mean better compression but more CPU usage for nsqd.
    deflate_level = {
        isa = 'number',
        def = 1,
        chk = function( val )
            return verifyInt( val ) and val >= 1;
        end
    },

    --  (nsqd v0.2.25+)
    --  deliver a percentage of all messages received to this connection.
    --  Valid range:
    --      0 <= sample_rate <= 99
    --      (0 disables sampling)
    --  Defaults to 0
    sample_rate = {
        isa = 'number',
        def = 0,
        chk = function( val )
            return verifyInt( val ) and val >= 0 and val <= 99;
        end
    },

    --  (nsqd v0.2.25+)
    --  a string identifying the agent for this client in the spirit of HTTP
    --  Default: <client_library_name>/<version>
    user_agent = {
        isa = 'string',
        def = 'lua-nsqp/scm'
    },

    --  (nsqd v0.2.28+)
    --  configure the server-side message timeout in milliseconds for messages
    --  delivered to this client.
    --  Valid range:
    --      1000 <= msg_timeout <= configured_max
    --      (0 to use default)
    msg_timeout = {
        isa = 'number',
        def = 0,
        chk = function( val )
            return verifyInt( val ) and val == 0 or val >= 1000;
        end
    },
};


--- identify Update client metadata on the server and negotiate features
-- @param msg
--  .client_id
--      an identifier used to disambiguate this client
--      (ie. something specific to the consumer)
--  .hostname
--      the hostname where the client is deployed
--  .feature_negotiation (nsqd v0.2.19+)
--      bool used to indicate that the client supports feature negotiation.
--      If the server is capable, it will send back a JSON payload of supported
--      features and metadata.
--  .heartbeat_interval (nsqd v0.2.19+)
--      milliseconds between heartbeats.
--      Valid range:
--          1000 <= heartbeat_interval <= configured_max
--          (-1 disables heartbeats)
--      Defaults to --client-timeout / 2
--  .output_buffer_size (nsqd v0.2.21+)
--      the size in bytes of the buffer nsqd will use when writing to this
--      client.
--      Valid range:
--          64 <= output_buffer_size <= configured_max
--          (-1 disables output buffering)
--      --max-output-buffer-size (nsqd flag) controls the max
--      Defaults to 16kb
--  .output_buffer_timeout (nsqd v0.2.21+)
--      the timeout after which any data that nsqd has buffered will be flushed
--      to this client.
--      Valid range:
--          1ms <= output_buffer_timeout <= configured_max
--          (-1 disables timeouts)
--      --max-output-buffer-timeout (nsqd flag) controls the max
--      Defaults to 250ms
--      Warning:
--          configuring clients with an extremely low (< 25ms)
--          output_buffer_timeout has a significant effect on nsqd CPU usage
--          (particularly with > 50 clients connected).
--          This is due to the current implementation relying on Go timers
--          which are maintained by the Go runtime in a priority queue.
--  .tls_v1 (nsqd v0.2.22+)
--      enable TLS for this connection.
--      --tls-cert and --tls-key (nsqd flags) enable TLS and configure the
--      server certificate
--      If the server supports TLS it will reply "tls_v1": true
--      The client should begin the TLS handshake immediately after reading
--      the IDENTIFY response
--      The server will respond OK after completing the TLS handshake
--  .snappy (nsqd v0.2.23+)
--      enable snappy compression for this connection.
--      --snappy (nsqd flag) enables support for this server side
--      The client should expect an additional, snappy compressed OK response
--      immediately after the IDENTIFY response.
--  .deflate (nsqd v0.2.23+)
--      enable deflate compression for this connection.
--      --deflate (nsqd flag) enables support for this server side
--      The client should expect an additional, deflate compressed OK response
--      immediately after the IDENTIFY response.
--      A client cannot enable both snappy and deflate.
--  .deflate_level (nsqd v0.2.23+)
--      configure the deflate compression level for this connection.
--      --max-deflate-level (nsqd flag) configures the maximum allowed value
--      Valid range:
--          1 <= deflate_level <= configured_max
--      Higher values mean better compression but more CPU usage for nsqd.
--  .sample_rate (nsqd v0.2.25+)
--      deliver a percentage of all messages received to this connection.
--      Valid range:
--          0 <= sample_rate <= 99
--          (0 disables sampling)
--      Defaults to 0
--  .user_agent (nsqd v0.2.25+)
--      a string identifying the agent for this client in the spirit of HTTP
--      Default: <client_library_name>/<version>
--  .msg_timeout (nsqd v0.2.28+)
--      configure the server-side message timeout in milliseconds for messages
--      delivered to this client.
-- @param len
-- @return data
-- @return expect
local function identify( ident )
    local idx = 1;
    local json = {};

    assert( type( ident ) == 'table', 'ident must be table' );

    for k, v in pairs( DEFAULT_IDENT ) do
        local val = ident[k];
        local t = type( val );

        -- use default value
        if t == 'nil' then
            val = v.def;
        elseif t ~= v.isa then
            error( 'ident.' .. k .. ' must be ' .. v.isa );
        elseif v.chk and not v.chk( val ) then
            error( 'ident.' .. k .. ' invalid range of value' );
        end

        -- add
        if v.isa == 'string' then
            json[idx] = k .. ':' .. val;
        else
            json[idx] = k .. ':' .. tostring( val );
        end

        idx = idx + 1;
    end

    -- create json string
    json = '{' .. concat( json, ',' ) .. '}';

    return 'IDENTIFY\n' .. htonl( #json ) .. json,
           ident.feature_negotiation and 'IDENT_JSON' or 'OK';
end


--- sub Subscribe to a topic/channel
-- @param topic
-- @param channel
-- @return data
-- @return expect
local function sub( topic, channel )
    assert( verifyName( topic ), 'invalid topic string' );
    assert( verifyName( channel ), 'invalid channel string' );

    return 'SUB ' .. topic .. ' ' .. channel .. '\n', 'OK';
end


--- pub Publish a message to a topic
-- @param topic
-- @param msg
-- @return data
-- @return expect
local function pub( topic, msg )
    assert( verifyName( topic ), 'invalid topic string' );
    assert( type( msg ) == 'string', 'msg must be string' );

    return 'PUB ' .. topic .. '\n' .. htonl( #msg ) .. msg, 'OK';
end


--- mpub Publish multiple messages to a topic
-- @param topic
-- @param msg
-- @param ...
-- @return data
-- @return expect
local function mpub( topic, msg, ... )
    local nmsg = 0;
    local nitem = 1;
    local items = {};
    local body, arr;

    assert( verifyName( topic ), 'invalid topic string' );
    if type( msg ) == 'string' then
        arr = { msg, ... };
        nmsg = select( '#', ... ) + 1;
    elseif type( msg ) == 'table' then
        arr = msg;
        nmsg = #msg;
    else
        error( 'msg must be string or table' );
    end

    for i = 1, nmsg do
        msg = arr[i];
        if type( msg ) ~= 'string' then
            error( 'msg#' .. i .. ' must be string' );
        end

        items[nitem] = htonl( #msg );
        items[nitem + 1] = msg;
        nitem = nitem + 2;
    end

    msg = concat( items );
    body = ( 4 * nmsg ) + #msg;

    return 'MPUB ' .. topic .. '\n' .. htonl( body ) .. htonl( nmsg ) .. msg,
           'OK';
end


--- rdy Update RDY state (indicate you are ready to receive cnt messages)
-- @param cnt
-- @return data
local function rdy( cnt )
    assert( verifyUInt( cnt ), 'cnt must be uint' );

    return 'RDY ' .. tostring( cnt ) .. '\n';
end


--- fin Finish a message (indicate successful processing)
-- @param msgid
-- @return data
local function fin( msgid )
    assert(
        type( msgid ) == 'string' and
        #msgid == 16 and
        msgid:find('^[0-9a-fA-F]+$') ~= nil,
        'msgid must be 16-byte hex string'
    );

    return 'FIN ' .. msgid .. '\n';
end


--- req Re-queue a message (indicate failure to process)
-- @param msgid
-- @param timeout
-- @return data
local function req( msgid, timeout )
    assert(
        type( msgid ) == 'string' and
        #msgid == 16 and
        msgid:find('^[0-9a-fA-F]+$') ~= nil,
        'msgid must be 16-byte hex string'
    );
    assert( verifyUInt( timeout ), 'timeout must be uint' );

    return 'REQ ' .. msgid .. ' ' .. tostring( timeout ) .. '\n';
end


--- touch Reset the timeout for an in-flight message
-- @param msgid
-- @return data
local function touch( msgid )
    assert(
        type( msgid ) == 'string' and
        #msgid == 16 and
        msgid:find('^[0-9a-fA-F]+$') ~= nil,
        'msgid must be 16-byte hex string'
    );

    return 'TOUCH ' .. msgid .. '\n';
end


--- cls Cleanly close your connection (no more messages are sent)
-- @param
-- @return data
-- @return expect
local function cls()
    return 'CLS\n', 'CLOSE_WAIT';
end


--- nop No-op
-- @param
-- @return data
local function nop()
    return 'NOP\n';
end


--- auth
-- @param auth
-- @param secret
-- @param len
-- @return data
-- @return expect
local function auth( secret )
    assert( type( secret ) == 'string', 'secret must be string' );

    return 'AUTH\n' .. htonl( #secret ) .. secret, 'AUTH_JSON';
end


--
-- Frame format:
--
-- |  (int32) ||  (int32) || (binary)
-- |  4-byte  ||  4-byte  || N-byte
-- ------------------------------------
--     size     frame type     data
-- size: frame type + data
--
-- Data format:
--
-- |     (int64)    ||  (uint16)  ||  (hex string) ||  (binary)
-- |     8-byte     ||   2-byte   ||    16-byte    ||  N-byte
-- -------------------------------------------------------------
--   nsec timestamp    attempts       message ID       message
--
--- decode
-- @param data
-- @return typ FRAME_<INVAL, PARTIAL, RESPONSE, ERROR or MESSAGE>
-- @return bytes if typ is FRAME_PARTIAL then bytes needed, else consumed bytes
-- @return data
-- @return msgid
-- @return nsec
local function decode( data )
    return decodeframe( data );
end


-- exports
return {
    --- constants
    EAGAIN = EAGAIN,
    EINVAL = EINVAL,
    RESPONSE = RESPONSE,
    ERROR = ERROR,
    MESSAGE = MESSAGE,
    --- frame-types
    FRAME_INVAL = FRAME_INVAL,
    FRAME_PARTIAL = FRAME_PARTIAL,
    FRAME_RESPONSE = FRAME_RESPONSE,
    FRAME_ERROR = FRAME_ERROR,
    FRAME_MESSAGE = FRAME_MESSAGE,
    --- commands
    identify = identify,
    sub = sub,
    pub = pub,
    mpub = mpub,
    rdy = rdy,
    fin = fin,
    req = req,
    touch = touch,
    cls = cls,
    nop = nop,
    auth = auth,
    --- response decoder
    decode = decode,
};
