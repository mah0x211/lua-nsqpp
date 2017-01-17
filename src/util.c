/*
 *  Copyright 2017 Masatoshi Teruya. All rights reserved.
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to
 *  deal in the Software without restriction, including without limitation the
 *  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 *  sell copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 *  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *  IN THE SOFTWARE.
 *
 *  util.c
 *  lua-nsqpp
 *
 *  Created by Masatoshi Teruya on 17/01/12.
 *
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <lua.h>
#include <lauxlib.h>


#define nsqpp_pushinteger2tbl( L, k, v ) do{ \
    lua_pushstring( L, k );                 \
    lua_pushinteger( L, v );                \
    lua_rawset( L, -3 );                    \
}while(0)


// frame types
enum {
    NSQPP_FRAME_INVAL = -2,
    NSQPP_FRAME_PARTIAL = -1,
    NSQPP_FRAME_RESPONSE = 0,
    NSQPP_FRAME_ERROR,
    NSQPP_FRAME_MESSAGE
};


typedef union {
    int16_t num;
    uint8_t ptr[2];
} nsqpp_bswap16_t;


typedef union {
    int32_t num;
    uint8_t ptr[4];
} nsqpp_bswap32_t;


typedef union {
    int64_t num;
    uint8_t ptr[8];
} nsqpp_bswap64_t;


static inline uint16_t nsqpp_bswap16( uint8_t *bytes )
{
    nsqpp_bswap16_t tmp = { 0 };

    tmp.ptr[0] = bytes[1];
    tmp.ptr[1] = bytes[0];

    return tmp.num;
}

static inline uint32_t nsqpp_bswap32( uint8_t *bytes )
{
    nsqpp_bswap32_t tmp = { 0 };

    tmp.ptr[0] = bytes[3];
    tmp.ptr[1] = bytes[2];
    tmp.ptr[2] = bytes[1];
    tmp.ptr[3] = bytes[0];

    return tmp.num;
}


static inline uint64_t nsqpp_bswap64( uint8_t *bytes )
{
    nsqpp_bswap64_t tmp = { 0 };

    tmp.ptr[0] = bytes[7];
    tmp.ptr[1] = bytes[6];
    tmp.ptr[2] = bytes[5];
    tmp.ptr[3] = bytes[4];
    tmp.ptr[4] = bytes[3];
    tmp.ptr[5] = bytes[2];
    tmp.ptr[6] = bytes[1];
    tmp.ptr[7] = bytes[0];

    return tmp.num;
}


static int decodeframe_lua( lua_State *L )
{
    size_t len = 0;
    const char *data = luaL_checklstring( L, 1, &len );
    uint8_t *frame = (uint8_t*)data;
    int32_t payload = 0;
    int32_t type = 0;

    // need more bytes
    if( len < 4 ){
        lua_pushinteger( L, NSQPP_FRAME_PARTIAL );
        lua_pushinteger( L, 4 - len );
        return 2;
    }

    // decode size
    payload = nsqpp_bswap32( frame );

    // need more bytes
    if( ( len - 4 ) < payload ){
        lua_pushinteger( L, NSQPP_FRAME_PARTIAL );
        lua_pushinteger( L, payload - ( len - 4 ) );
        return 2;
    }

    // decode frame type
    type = nsqpp_bswap32( frame + 4 );
    // push frame type and consumed bytes
    lua_pushinteger( L, type );
    lua_pushinteger( L, payload + 4 );

    // check frame type
    switch( type )
    {
        // push response data
        case NSQPP_FRAME_RESPONSE:
        case NSQPP_FRAME_ERROR:
            lua_pushlstring( L, data + 8, payload - 4 );
            return 3;

        // push message, message-id and nanoseconds
        case NSQPP_FRAME_MESSAGE:
            // message
            lua_pushlstring( L, data + 34, payload - 30 );
            // message-id
            lua_pushlstring( L, data + 18, 16 );
            // nanoseconds
            lua_pushinteger( L, nsqpp_bswap64( (uint8_t*)data + 8 ) );
            // attempts
            lua_pushinteger( L, nsqpp_bswap16( (uint8_t*)data + 10 ) );
            return 6;

        // invalid frame data
        default:
            lua_pushinteger( L, NSQPP_FRAME_INVAL );
            lua_replace( L, -3 );
            return 2;
    }
}


static int htonl_lua( lua_State *L )
{
    lua_Integer len = luaL_checkinteger( L, 1 );
    nsqpp_bswap32_t data = { htonl( len ) };

    lua_pushlstring( L, (const char*)data.ptr, 4 );

    return 1;
}


LUALIB_API int luaopen_nsqpp_util( lua_State *L )
{
    struct luaL_Reg method[] = {
        { "htonl", htonl_lua },
        { "decodeframe", decodeframe_lua },
        { NULL, NULL }
    };
    struct luaL_Reg *ptr = method;

    lua_newtable( L );
    while( ptr->name ){
        lua_pushstring( L, ptr->name );
        lua_pushcfunction( L, ptr->func );
        lua_rawset( L, -3 );
        ptr++;
    }

    // export constants
    nsqpp_pushinteger2tbl( L, "FRAME_INVAL", NSQPP_FRAME_INVAL );
    nsqpp_pushinteger2tbl( L, "FRAME_PARTIAL", NSQPP_FRAME_PARTIAL );
    nsqpp_pushinteger2tbl( L, "FRAME_RESPONSE", NSQPP_FRAME_RESPONSE );
    nsqpp_pushinteger2tbl( L, "FRAME_ERROR", NSQPP_FRAME_ERROR );
    nsqpp_pushinteger2tbl( L, "FRAME_MESSAGE", NSQPP_FRAME_MESSAGE );

    return 1;
}

