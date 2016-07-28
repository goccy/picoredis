#ifndef __PICOREDIS_H__
#define __PICOREDIS_H__

#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <math.h>
#include <netdb.h>
#include <signal.h>
#include <poll.h>
#include <stdarg.h>
#include <assert.h>

typedef struct {
    const char *host;
    int port;
    const char *error;
    int sock;
    char *receive_buf;
} picoredis_t;

typedef struct {
    int num;
    const char **values;
} picoredis_array_t;

typedef enum {
    PICOREDIS_REPLY_SINGLE_LINE,
    PICOREDIS_REPLY_ERROR,
    PICOREDIS_REPLY_NUM,
    PICOREDIS_REPLY_BULK,
    PICOREDIS_REPLY_MULTI_BULK,
} picoredis_reply_type;

typedef struct {
    picoredis_reply_type type;
    int length;
    union {
        char *svalue;
        int ivalue;
        picoredis_array_t *avalue;
    } v;
} picoredis_reply_t;

#define COMMAND_TYPE_DEF(type) PICOREDIS_ ## type

typedef enum {
    COMMAND_TYPE_DEF(QUIT),
    COMMAND_TYPE_DEF(AUTH),

    COMMAND_TYPE_DEF(EXISTS),
    COMMAND_TYPE_DEF(DEL),
    COMMAND_TYPE_DEF(TYPE),
    COMMAND_TYPE_DEF(KEYS),
    COMMAND_TYPE_DEF(RANDOMKEY),
    COMMAND_TYPE_DEF(RENAME),
    COMMAND_TYPE_DEF(RENAMENX),
    COMMAND_TYPE_DEF(DBSIZE),
    COMMAND_TYPE_DEF(EXPIRE),
    COMMAND_TYPE_DEF(EXPIREAT),
    COMMAND_TYPE_DEF(PERSIST),
    COMMAND_TYPE_DEF(TTL),
    COMMAND_TYPE_DEF(SELECT),
    COMMAND_TYPE_DEF(MOVE),
    COMMAND_TYPE_DEF(FLUSHDB),
    COMMAND_TYPE_DEF(FLUSHALL),

    COMMAND_TYPE_DEF(SET),
    COMMAND_TYPE_DEF(GET),
    COMMAND_TYPE_DEF(GETSET),
    COMMAND_TYPE_DEF(MGET),
    COMMAND_TYPE_DEF(SETNX),
    COMMAND_TYPE_DEF(SETEX),
    COMMAND_TYPE_DEF(MSET),
    COMMAND_TYPE_DEF(MSETNX),
    COMMAND_TYPE_DEF(INCR),
    COMMAND_TYPE_DEF(INCRBY),
    COMMAND_TYPE_DEF(DECR),
    COMMAND_TYPE_DEF(DECRBY),
    COMMAND_TYPE_DEF(APPEND),
    COMMAND_TYPE_DEF(SUBSTR),

    COMMAND_TYPE_DEF(RPUSH),
    COMMAND_TYPE_DEF(LPUSH),
    COMMAND_TYPE_DEF(LLEN),
    COMMAND_TYPE_DEF(LRANGE),
    COMMAND_TYPE_DEF(LTRIM),
    COMMAND_TYPE_DEF(LINDEX),
    COMMAND_TYPE_DEF(LSET),
    COMMAND_TYPE_DEF(LREM),
    COMMAND_TYPE_DEF(LPOP),
    COMMAND_TYPE_DEF(RPOP),
    COMMAND_TYPE_DEF(BLPOP),
    COMMAND_TYPE_DEF(BRPOP),
    COMMAND_TYPE_DEF(RPOPLPUSH),

    COMMAND_TYPE_DEF(SADD),
    COMMAND_TYPE_DEF(SREM),
    COMMAND_TYPE_DEF(SPOP),
    COMMAND_TYPE_DEF(SMOVE),
    COMMAND_TYPE_DEF(SCARD),
    COMMAND_TYPE_DEF(SISMEMBER),
    COMMAND_TYPE_DEF(SINTER),
    COMMAND_TYPE_DEF(SINTERSTORE),
    COMMAND_TYPE_DEF(SUNION),
    COMMAND_TYPE_DEF(SUNIONSTORE),
    COMMAND_TYPE_DEF(SDIFF),
    COMMAND_TYPE_DEF(SDIFFSTORE),
    COMMAND_TYPE_DEF(SMEMBERS),
    COMMAND_TYPE_DEF(SRANDMEMBER),

    COMMAND_TYPE_DEF(ZADD),
    COMMAND_TYPE_DEF(ZREM),
    COMMAND_TYPE_DEF(ZINCRBY),
    COMMAND_TYPE_DEF(ZRANK),
    COMMAND_TYPE_DEF(ZREVRANK),
    COMMAND_TYPE_DEF(ZRANGE),
    COMMAND_TYPE_DEF(ZREVRANGE),
    COMMAND_TYPE_DEF(ZRANGEBYSCORE),
    COMMAND_TYPE_DEF(ZCOUNT),
    COMMAND_TYPE_DEF(ZCARD),
    COMMAND_TYPE_DEF(ZSCORE),
    COMMAND_TYPE_DEF(ZREMRANGEBYRANK),
    COMMAND_TYPE_DEF(ZREMRANGEBYSCORE),
    COMMAND_TYPE_DEF(ZUNIONSTORE),
    COMMAND_TYPE_DEF(ZINTERSTORE),

    COMMAND_TYPE_DEF(HSET),
    COMMAND_TYPE_DEF(HGET),
    COMMAND_TYPE_DEF(HMGET),
    COMMAND_TYPE_DEF(HMSET),
    COMMAND_TYPE_DEF(HINCRBY),
    COMMAND_TYPE_DEF(HEXISTS),
    COMMAND_TYPE_DEF(HDEL),
    COMMAND_TYPE_DEF(HLEN),
    COMMAND_TYPE_DEF(HKEYS),
    COMMAND_TYPE_DEF(HVALS),
    COMMAND_TYPE_DEF(HGETALL),

    COMMAND_TYPE_DEF(SORT),

    COMMAND_TYPE_DEF(MULTI),
    COMMAND_TYPE_DEF(EXEC),
    COMMAND_TYPE_DEF(DISCARD),
    COMMAND_TYPE_DEF(WATCH),
    COMMAND_TYPE_DEF(UNWATCH),

    COMMAND_TYPE_DEF(SUBSCRIBE),
    COMMAND_TYPE_DEF(UNSUBSCRIBE),
    COMMAND_TYPE_DEF(PUBLISH),

    COMMAND_TYPE_DEF(SAVE),
    COMMAND_TYPE_DEF(BGSAVE),
    COMMAND_TYPE_DEF(LASTSAVE),
    COMMAND_TYPE_DEF(SHUTDOWN),
    COMMAND_TYPE_DEF(BGREWRITEAOF),

    COMMAND_TYPE_DEF(INFO),
    COMMAND_TYPE_DEF(MONITOR),
    COMMAND_TYPE_DEF(SLAVEOF),
    COMMAND_TYPE_DEF(CONFIG),

    COMMAND_TYPE_DEF(NONE),
} picoredis_command_type;

typedef struct {
    picoredis_command_type type;
    const char *name;
    size_t name_length;
} picoredis_command_type_t;

static picoredis_t *picoredis_alloc(void)
{
    picoredis_t *ret = (picoredis_t *)malloc(sizeof(picoredis_t));
    memset(ret, 0, sizeof(picoredis_t));
    ret->receive_buf = (char *)malloc(BUFSIZ);
    memset(ret->receive_buf, 0, BUFSIZ);
    return ret;
}

static void picoredis_free(picoredis_t *ctx)
{
    if (!ctx) return;

    free(ctx->receive_buf);
    free(ctx);
    ctx = NULL;
}

static int picoredis_has_error(picoredis_t *ctx)
{
    return ctx->error != NULL;
}

static picoredis_array_t *picoredis_array_alloc(int num)
{
    picoredis_array_t *ret = (picoredis_array_t *)malloc(sizeof(picoredis_array_t));
    memset(ret, 0, sizeof(picoredis_array_t));
    ret->num = num;
    ret->values = (const char **)malloc(sizeof(const char *) * num);
    return ret;
}

static void picoredis_array_free(picoredis_array_t *array)
{
    if (!array) return;

    free(array->values);
    free(array);
    array = NULL;
}

static size_t picoredis_array_num(picoredis_array_t *array)
{
    return array->num;
}

static const char *picoredis_array_get(picoredis_array_t *array, int idx)
{
    assert(0 <= idx && idx < array->num);
    return array->values[idx];
}

static int picoredis_connect_with_ctx(picoredis_t *ctx, const char *host, int port)
{
    ctx->host = host;
    ctx->port = port;

    int sd;
    struct sockaddr_in addr;

    if ((sd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        return -1;
    }

    int flag = fcntl(sd, F_GETFL, 0);
    if (flag < 0) {
        return -1;
    }
    if (fcntl(sd, F_SETFL, flag) < 0) {
        return -1;
    }
    int on = 1;
    if (setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, (char *)&on, sizeof(on)) != 0) {
        return -1;
    }

    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(ctx->port);
    addr.sin_addr.s_addr = inet_addr(ctx->host);

    int connect_result = connect(sd, (struct sockaddr *)&addr, sizeof(addr));
    if (connect_result < 0) {
        close(sd);
        return -1;
    }

    signal(SIGPIPE , SIG_IGN);
    return sd;
}

static picoredis_t *picoredis_connect_with_address(const char *address)
{
    picoredis_t *ctx = picoredis_alloc();
    char *seek_ptr = (char *)address;
    for (; *seek_ptr != '\0' && *seek_ptr != ':'; ++seek_ptr) {}
    size_t hostname_length = seek_ptr - address;
    char *host = (char *)malloc(hostname_length + 1);
    memset(host, 0, hostname_length + 1);
    memcpy(host, address, hostname_length);
    char port[8] = {0};
    memcpy(port, seek_ptr + 1, strlen(seek_ptr + 1));
    ctx->sock = picoredis_connect_with_ctx(ctx, host, atoi(port));
    return ctx;
}

static picoredis_t *picoredis_connect(const char *host, int port)
{
    picoredis_t *ctx = picoredis_alloc();
    ctx->sock = picoredis_connect_with_ctx(ctx, host, port);
    return ctx;
}

static size_t picoredis_total_args_length(int nargs, size_t *lengths)
{
    size_t total_length = 0;
    size_t i = 0;
    for (; i < nargs; ++i) {
        size_t length   = lengths[i];
        size_t num_size = (size_t)log10(length) + 1;
        total_length += (num_size + length);
    }
    return total_length;
}

static char *picoredis_parse_command_args(int nargs, size_t *lengths, const char **values)
{
    static const size_t protocol_char   = 5; // '\r', '\n', '$', '\r', '\n'
    static const size_t protocol_length = 9; // '\', 'r', '\', 'n', '$', '\', 'r', '\', 'n'

    size_t total_length = nargs * protocol_length + picoredis_total_args_length(nargs, lengths);
    char *args_buffer   = (char *)malloc(total_length);
    memset(args_buffer, 0, total_length);
    char *args_set_ptr  = args_buffer;
    size_t i = 0;
    for (; i < nargs; ++i) {
        size_t length     = lengths[i];
        size_t num_size   = (size_t)log10(length) + 1;
        const char *value = values[i];
        size_t bufsize    = protocol_length + num_size + length;
        snprintf(args_set_ptr, bufsize, "\r\n$%zu\r\n%s", length, value);
        if (i + 1 != nargs) {
            size_t diff   = protocol_length - protocol_char;
            args_set_ptr += bufsize - diff;
        }
    }
    return args_buffer;
}

#define COMMAND_DEF(type) { PICOREDIS_ ## type, #type, strlen(#type) }

static picoredis_command_type_t picoredis_get_command_type(picoredis_command_type type)
{
    static picoredis_command_type_t all_command_types[] = {
        COMMAND_DEF(QUIT),
        COMMAND_DEF(AUTH),

        COMMAND_DEF(EXISTS),
        COMMAND_DEF(DEL),
        COMMAND_DEF(TYPE),
        COMMAND_DEF(KEYS),
        COMMAND_DEF(RANDOMKEY),
        COMMAND_DEF(RENAME),
        COMMAND_DEF(RENAMENX),
        COMMAND_DEF(DBSIZE),
        COMMAND_DEF(EXPIRE),
        COMMAND_DEF(EXPIREAT),
        COMMAND_DEF(PERSIST),
        COMMAND_DEF(TTL),
        COMMAND_DEF(SELECT),
        COMMAND_DEF(MOVE),
        COMMAND_DEF(FLUSHDB),
        COMMAND_DEF(FLUSHALL),

        COMMAND_DEF(SET),
        COMMAND_DEF(GET),
        COMMAND_DEF(GETSET),
        COMMAND_DEF(MGET),
        COMMAND_DEF(SETNX),
        COMMAND_DEF(SETEX),
        COMMAND_DEF(MSET),
        COMMAND_DEF(MSETNX),
        COMMAND_DEF(INCR),
        COMMAND_DEF(INCRBY),
        COMMAND_DEF(DECR),
        COMMAND_DEF(DECRBY),
        COMMAND_DEF(APPEND),
        COMMAND_DEF(SUBSTR),

        COMMAND_DEF(RPUSH),
        COMMAND_DEF(LPUSH),
        COMMAND_DEF(LLEN),
        COMMAND_DEF(LRANGE),
        COMMAND_DEF(LTRIM),
        COMMAND_DEF(LINDEX),
        COMMAND_DEF(LSET),
        COMMAND_DEF(LREM),
        COMMAND_DEF(LPOP),
        COMMAND_DEF(RPOP),
        COMMAND_DEF(BLPOP),
        COMMAND_DEF(BRPOP),
        COMMAND_DEF(RPOPLPUSH),

        COMMAND_DEF(SADD),
        COMMAND_DEF(SREM),
        COMMAND_DEF(SPOP),
        COMMAND_DEF(SMOVE),
        COMMAND_DEF(SCARD),
        COMMAND_DEF(SISMEMBER),
        COMMAND_DEF(SINTER),
        COMMAND_DEF(SINTERSTORE),
        COMMAND_DEF(SUNION),
        COMMAND_DEF(SUNIONSTORE),
        COMMAND_DEF(SDIFF),
        COMMAND_DEF(SDIFFSTORE),
        COMMAND_DEF(SMEMBERS),
        COMMAND_DEF(SRANDMEMBER),

        COMMAND_DEF(ZADD),
        COMMAND_DEF(ZREM),
        COMMAND_DEF(ZINCRBY),
        COMMAND_DEF(ZRANK),
        COMMAND_DEF(ZREVRANK),
        COMMAND_DEF(ZRANGE),
        COMMAND_DEF(ZREVRANGE),
        COMMAND_DEF(ZRANGEBYSCORE),
        COMMAND_DEF(ZCOUNT),
        COMMAND_DEF(ZCARD),
        COMMAND_DEF(ZSCORE),
        COMMAND_DEF(ZREMRANGEBYRANK),
        COMMAND_DEF(ZREMRANGEBYSCORE),
        COMMAND_DEF(ZUNIONSTORE),
        COMMAND_DEF(ZINTERSTORE),

        COMMAND_DEF(HSET),
        COMMAND_DEF(HGET),
        COMMAND_DEF(HMGET),
        COMMAND_DEF(HMSET),
        COMMAND_DEF(HINCRBY),
        COMMAND_DEF(HEXISTS),
        COMMAND_DEF(HDEL),
        COMMAND_DEF(HLEN),
        COMMAND_DEF(HKEYS),
        COMMAND_DEF(HVALS),
        COMMAND_DEF(HGETALL),

        COMMAND_DEF(SORT),

        COMMAND_DEF(MULTI),
        COMMAND_DEF(EXEC),
        COMMAND_DEF(DISCARD),
        COMMAND_DEF(WATCH),
        COMMAND_DEF(UNWATCH),

        COMMAND_DEF(SUBSCRIBE),
        COMMAND_DEF(UNSUBSCRIBE),
        COMMAND_DEF(PUBLISH),

        COMMAND_DEF(SAVE),
        COMMAND_DEF(BGSAVE),
        COMMAND_DEF(LASTSAVE),
        COMMAND_DEF(SHUTDOWN),
        COMMAND_DEF(BGREWRITEAOF),

        COMMAND_DEF(INFO),
        COMMAND_DEF(MONITOR),
        COMMAND_DEF(SLAVEOF),
        COMMAND_DEF(CONFIG),

        COMMAND_DEF(NONE),
    };

    size_t i = 0;
    for (; all_command_types[i].type != COMMAND_TYPE_DEF(NONE); ++i) {
        if (all_command_types[i].type == type) {
            return all_command_types[i];
        }
    }
    return all_command_types[i];
}

static size_t picoredis_command_header_size(size_t nargs, picoredis_command_type_t *type)
{
    size_t total_args_num                 = nargs + 1;
    size_t total_args_num_digit           = (size_t)log10(total_args_num) + 1;
    static const size_t protocol_char     = 8;  // '*', '\r', '\n', '$', '\r', '\n', '\r', '\n'
    size_t name_length_num_digit          = (size_t)log10(type->name_length + 1) + 1;
    return protocol_char + total_args_num_digit + name_length_num_digit + type->name_length;
}

static char *picoredis_command_create(picoredis_command_type type, size_t nargs, size_t *lengths, const char **values)
{
    picoredis_command_type_t command_type = picoredis_get_command_type(type);
    char *args          = picoredis_parse_command_args(nargs, lengths, values);
    size_t header_size  = picoredis_command_header_size(nargs, &command_type);
    size_t command_size = header_size + strlen(args) + 1;
    char *command       = (char *)malloc(command_size);
    memset(command, 0, command_size);
    snprintf(command, command_size, "*%zu\r\n$%zu\r\n%s%s\r\n", nargs + 1, command_type.name_length, command_type.name, args);
    return command;
}

static int picoredis_send_command(picoredis_t *ctx, char *command)
{
    int ret = send(ctx->sock, command, strlen(command), 0);
    if (ret <= 0) {
        ctx->error = strerror(errno);
    }
    return ret;
}

static picoredis_reply_t *picoredis_receive_command(picoredis_t *ctx)
{
    memset(ctx->receive_buf, 0, BUFSIZ);
    int recv_result = recv(ctx->sock, ctx->receive_buf, BUFSIZ, 0);
    if (recv_result <= 0) {
        ctx->error = strerror(errno);
        return NULL;
    }
    char *buf = ctx->receive_buf;
    //fprintf(stderr, "buf = [%s]\n", buf);
    char *all_lines[BUFSIZ] = {0};
    size_t i = 0;
    size_t line_top_index = 0;
    size_t line_num = 0;
    for (; buf[i] != '\0'; ++i) {
        if (i > 1 && buf[i - 1] == '\r' && buf[i] == '\n') {
            buf[i - 1] = '\0';
            all_lines[line_num] = buf + line_top_index;
            line_num++;
            line_top_index = i + 1;
        }
    }
    picoredis_reply_t *reply = (picoredis_reply_t *)malloc(sizeof(picoredis_reply_t));
    memset(reply, 0, sizeof(picoredis_reply_t));

    for (i = 0; i < line_num; ++i) {
        char *line = all_lines[i];
        if (i == 0) {
            switch (line[0]) {
            case '+': {
                reply->type = PICOREDIS_REPLY_SINGLE_LINE;
                size_t line_size = strlen(line);
                reply->v.svalue = (char *)malloc(line_size);
                memset(reply->v.svalue, 0, line_size);
                memcpy((void *)reply->v.svalue, line + 1, line_size);
                break;
            }
            case '-': {
                reply->type = PICOREDIS_REPLY_ERROR;
                size_t line_size = strlen(line);
                reply->v.svalue = (char *)malloc(line_size);
                memset(reply->v.svalue, 0, line_size);
                memcpy((void *)reply->v.svalue, line + 1, line_size);
                break;
            }
            case ':':
                reply->type     = PICOREDIS_REPLY_NUM;
                reply->v.ivalue = atoi(line + 1);
                break;
            case '$': {
                reply->type   = PICOREDIS_REPLY_BULK;
                reply->length = atoi(line + 1);
                break;
            }
            case '*':
                reply->type     = PICOREDIS_REPLY_MULTI_BULK;
                reply->v.avalue = picoredis_array_alloc(atoi(line + 1));
                break;
            default:
                break;
            }
        } else {
            switch (reply->type) {
            case PICOREDIS_REPLY_SINGLE_LINE:
                reply->v.ivalue = 0;
                break;
            case PICOREDIS_REPLY_ERROR:
                reply->v.ivalue = -1;
                break;
            case PICOREDIS_REPLY_MULTI_BULK: {
                int is_value_line = (i - 1) % 2;
                if (is_value_line) {
                    size_t size = strlen(line) + 1;
                    char *value = (char *)malloc(size);
                    memset(value, 0, size);
                    memcpy(value, line, size - 1);
                    reply->v.avalue->values[(i - 2) / 2] = value;
                }
                break;
            }
            case PICOREDIS_REPLY_BULK: {
                if (reply->length > 0) {
                    reply->v.svalue = (char *)malloc(reply->length + 1);
                    memset(reply->v.svalue, 0, reply->length + 1);
                    memcpy((void *)reply->v.svalue, line, reply->length);
                }
                break;
            }
            default:
                break;
            }
        }
    }
    return reply;
}

static picoredis_reply_t *picoredis_send_and_reply0(picoredis_t *ctx, picoredis_command_type type)
{
    ctx->error = NULL;
    picoredis_send_command(ctx, picoredis_command_create(type, 0, NULL, NULL));
    if (picoredis_has_error(ctx)) return NULL;

    return picoredis_receive_command(ctx);
}

static picoredis_reply_t *picoredis_send_and_reply1(picoredis_t *ctx, picoredis_command_type type, const char *arg)
{
    size_t lengths[]     = { strlen(arg) };
    const char *values[] = { arg };
    ctx->error = NULL;
    picoredis_send_command(ctx, picoredis_command_create(type, 1, lengths, values));
    if (picoredis_has_error(ctx)) return NULL;

    return picoredis_receive_command(ctx);
}

static picoredis_reply_t *picoredis_send_and_reply2(picoredis_t *ctx, picoredis_command_type type, const char *arg1, const char *arg2)
{
    static const size_t nargs = 2;
    size_t lengths[]     = { strlen(arg1), strlen(arg2) };
    const char *values[] = { arg1, arg2 };
    ctx->error = NULL;
    picoredis_send_command(ctx, picoredis_command_create(type, 2, lengths, values));
    if (picoredis_has_error(ctx)) return NULL;

    return picoredis_receive_command(ctx);
}

static picoredis_reply_t *picoredis_send_and_reply3(picoredis_t *ctx, picoredis_command_type type, const char *arg1, const char *arg2, const char *arg3)
{
    static const size_t nargs = 3;
    size_t lengths[]     = { strlen(arg1), strlen(arg2), strlen(arg3) };
    const char *values[] = { arg1, arg2, arg3 };
    ctx->error = NULL;
    picoredis_send_command(ctx, picoredis_command_create(type, nargs, lengths, values));
    if (picoredis_has_error(ctx)) return NULL;

    return picoredis_receive_command(ctx);
}

static picoredis_reply_t *picoredis_send_and_replyn(picoredis_t *ctx, picoredis_command_type type, size_t nargs, va_list list)
{
    size_t lengths[nargs];
    const char *values[nargs];
    size_t i = 0;
    for (; i < nargs; ++i) {
        char *k    = va_arg(list, char *);
        lengths[i] = strlen(k);
        values[i]  = k;
    }

    ctx->error = NULL;
    picoredis_send_command(ctx, picoredis_command_create(type, nargs, lengths, values));
    if (picoredis_has_error(ctx)) return NULL;

    return picoredis_receive_command(ctx);
}


// ========= Command API  ============ //

static void picoredis_exec_quit(picoredis_t *ctx)
{
    picoredis_send_and_reply0(ctx, PICOREDIS_QUIT);
}

static int picoredis_exec_auth(picoredis_t *ctx, const char *password)
{
    picoredis_reply_t *reply = picoredis_send_and_reply1(ctx, PICOREDIS_AUTH, password);
    if (!reply) {
        ctx->error = "cannot receive reply";
        return 0;
    }

    if (reply->type == PICOREDIS_REPLY_ERROR) {
        ctx->error = "cannot set";
        return 0;
    }
    return 1;
}

static int picoredis_exec_exists(picoredis_t *ctx, const char *key)
{
    picoredis_reply_t *reply = picoredis_send_and_reply1(ctx, PICOREDIS_EXISTS, key);
    return reply ? reply->v.ivalue : 0;
}

static int picoredis_exec_del(picoredis_t *ctx, size_t nargs, ...)
{
    va_list list;
    va_start(list, nargs);
    picoredis_reply_t *reply = picoredis_send_and_replyn(ctx, PICOREDIS_DEL, nargs, list);
    va_end(list);
    return reply ? reply->v.ivalue : 0;
}

static char *picoredis_exec_type(picoredis_t *ctx, const char *key)
{
    picoredis_reply_t *reply = picoredis_send_and_reply1(ctx, PICOREDIS_TYPE, key);
    return reply ? reply->v.svalue : NULL;
}

static picoredis_array_t *picoredis_exec_keys(picoredis_t *ctx, const char *key)
{
    picoredis_reply_t *reply = picoredis_send_and_reply1(ctx, PICOREDIS_KEYS, key);
    return reply ? reply->v.avalue : NULL;
}

static char *picoredis_exec_randomkey(picoredis_t *ctx)
{
    picoredis_reply_t *reply = picoredis_send_and_reply0(ctx, PICOREDIS_RANDOMKEY);
    return reply ? reply->v.svalue : NULL;
}

static int picoredis_exec_rename(picoredis_t *ctx, const char *oldkey, const char *newkey)
{
    picoredis_reply_t *reply = picoredis_send_and_reply2(ctx, PICOREDIS_RENAME, oldkey, newkey);
    return reply ? (reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1) : 0;
}

static int picoredis_exec_renamenx(picoredis_t *ctx, const char *oldkey, const char *newkey)
{
    picoredis_reply_t *reply = picoredis_send_and_reply2(ctx, PICOREDIS_RENAMENX, oldkey, newkey);
    return reply ? reply->v.ivalue : 0;
}

static int picoredis_exec_dbsize(picoredis_t *ctx)
{
    picoredis_reply_t *reply = picoredis_send_and_reply0(ctx, PICOREDIS_DBSIZE);
    return reply ? reply->v.ivalue : 0;
}

static int picoredis_exec_expire(picoredis_t *ctx, const char *key, size_t seconds)
{
    char int_value[64] = {0};
    snprintf(int_value, sizeof(int_value), "%zu", seconds);
    picoredis_reply_t *reply = picoredis_send_and_reply2(ctx, PICOREDIS_EXPIRE, key, int_value);
    return reply ? reply->v.ivalue : 0;
}

static int picoredis_exec_expireat(picoredis_t *ctx, const char *key, time_t unixtime)
{
    char time_value[64] = {0};
    snprintf(time_value, sizeof(time_value), "%zu", unixtime);
    picoredis_reply_t *reply = picoredis_send_and_reply2(ctx, PICOREDIS_EXPIREAT, key, time_value);
    return reply ? reply->v.ivalue : 0;
}

static int picoredis_exec_persist(picoredis_t *ctx, const char *key)
{
    picoredis_reply_t *reply = picoredis_send_and_reply1(ctx, PICOREDIS_PERSIST, key);
    return reply ? reply->v.ivalue : 0;
}

static int picoredis_exec_ttl(picoredis_t *ctx, const char *key)
{
    picoredis_reply_t *reply = picoredis_send_and_reply1(ctx, PICOREDIS_TTL, key);
    return reply ? reply->v.ivalue : 0;
}

static int picoredis_exec_select(picoredis_t *ctx, size_t index)
{
    char index_value[64] = {0};
    snprintf(index_value, sizeof(index_value), "%zu", index);
    picoredis_reply_t *reply = picoredis_send_and_reply1(ctx, PICOREDIS_SELECT, index_value);
    return reply ? (reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1) : 0;
}

static int picoredis_exec_move(picoredis_t *ctx, const char *key, size_t dbindex)
{
    char index_value[64] = {0};
    snprintf(index_value, sizeof(index_value), "%zu", dbindex);
    picoredis_reply_t *reply = picoredis_send_and_reply2(ctx, PICOREDIS_MOVE, key, index_value);
    return reply ? reply->v.ivalue : 0;
}

static int picoredis_exec_flushdb(picoredis_t *ctx)
{
    picoredis_reply_t *reply = picoredis_send_and_reply0(ctx, PICOREDIS_FLUSHDB);
    return reply ? (reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1) : 0;
}

static int picoredis_exec_flushall(picoredis_t *ctx)
{
    picoredis_reply_t *reply = picoredis_send_and_reply0(ctx, PICOREDIS_FLUSHALL);
    return reply ? (reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1) : 0;
}

static int picoredis_exec_watch(picoredis_t *ctx, size_t nargs, ...)
{
    va_list list;
    va_start(list, nargs);
    picoredis_reply_t *reply = picoredis_send_and_replyn(ctx, PICOREDIS_WATCH, nargs, list);
    va_end(list);
    return reply ? (reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1) : 0;
}

static int picoredis_exec_unwatch(picoredis_t *ctx)
{
    picoredis_reply_t *reply = picoredis_send_and_reply0(ctx, PICOREDIS_UNWATCH);
    return reply ? (reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1) : 0;
}

static int picoredis_exec_multi(picoredis_t *ctx)
{
    picoredis_reply_t *reply = picoredis_send_and_reply0(ctx, PICOREDIS_MULTI);
    return reply ? (reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1) : 0;
}

static int picoredis_exec_exec(picoredis_t *ctx)
{
    picoredis_reply_t *reply = picoredis_send_and_reply0(ctx, PICOREDIS_EXEC);
    return reply ? (reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1) : 0;
}

static int picoredis_exec_discard(picoredis_t *ctx)
{
    picoredis_reply_t *reply = picoredis_send_and_reply0(ctx, PICOREDIS_DISCARD);
    return reply ? (reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1) : 0;
}

static picoredis_array_t *picoredis_exec_sort(picoredis_t *ctx, size_t nargs, ...)
{
    va_list list;
    va_start(list, nargs);
    picoredis_reply_t *reply = picoredis_send_and_replyn(ctx, PICOREDIS_SORT, nargs, list);
    va_end(list);
    if (!reply) return NULL;
    if (reply->type == PICOREDIS_REPLY_ERROR) return NULL;
    return reply->v.avalue;
}

static void picoredis_exec_set(picoredis_t *ctx, const char *key, const char *value)
{
    picoredis_reply_t *reply = picoredis_send_and_reply2(ctx, PICOREDIS_SET, key, value);
    if (!reply) {
        ctx->error = "cannot receive reply";
        return;
    }

    if (reply->v.ivalue == -1) {
        ctx->error = "cannot set";
        return;
    }
}

static char *picoredis_exec_get(picoredis_t *ctx, const char *key)
{
    picoredis_reply_t *reply = picoredis_send_and_reply1(ctx, PICOREDIS_GET, key);
    if (!reply) return NULL;
    if (reply->length < 0) return NULL;

    return reply->v.svalue;
}

static char *picoredis_exec_getset(picoredis_t *ctx, const char *key, const char *value)
{
    picoredis_reply_t *reply = picoredis_send_and_reply2(ctx, PICOREDIS_GETSET, key, value);
    if (!reply) return NULL;
    if (reply->length < 0) return NULL;

    return reply->v.svalue;
}

static int picoredis_exec_setnx(picoredis_t *ctx, const char *key, const char *value)
{
    picoredis_reply_t *reply = picoredis_send_and_reply2(ctx, PICOREDIS_SETNX, key, value);
    return reply ? reply->v.ivalue : 0;
}

static int picoredis_exec_setex(picoredis_t *ctx, const char *key, time_t time, const char *value)
{
    char time_value[64] = {0};
    snprintf(time_value, sizeof(time_value), "%zu", time);

    picoredis_reply_t *reply = picoredis_send_and_reply3(ctx, PICOREDIS_SETEX, key, time_value, value);
    return reply ? (reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1) : 0;
}

static int picoredis_exec_mset(picoredis_t *ctx, size_t nargs, ...)
{
    va_list list;
    va_start(list, nargs);
    picoredis_reply_t *reply = picoredis_send_and_replyn(ctx, PICOREDIS_MSET, nargs, list);
    va_end(list);
    if (!reply) return 0;
    return reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1;
}

static int picoredis_exec_msetnx(picoredis_t *ctx, size_t nargs, ...)
{
    va_list list;
    va_start(list, nargs);
    picoredis_reply_t *reply = picoredis_send_and_replyn(ctx, PICOREDIS_MSET, nargs, list);
    va_end(list);
    if (!reply) return 0;
    return reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1;
}

static int picoredis_exec_incr(picoredis_t *ctx, const char *key)
{
    picoredis_reply_t *reply = picoredis_send_and_reply1(ctx, PICOREDIS_INCR, key);
    return reply ? reply->v.ivalue : 0;
}

static int picoredis_exec_incrby(picoredis_t *ctx, const char *key, int value)
{
    char int_value[64] = {0};
    snprintf(int_value, sizeof(int_value), "%d", value);

    picoredis_reply_t *reply = picoredis_send_and_reply2(ctx, PICOREDIS_INCRBY, key, int_value);
    return reply ? reply->v.ivalue : 0;
}

static int picoredis_exec_decr(picoredis_t *ctx, const char *key)
{
    picoredis_reply_t *reply = picoredis_send_and_reply1(ctx, PICOREDIS_DECR, key);
    return reply ? reply->v.ivalue : 0;
}

static int picoredis_exec_decrby(picoredis_t *ctx, const char *key, int value)
{
    char int_value[64] = {0};
    snprintf(int_value, sizeof(int_value), "%d", value);

    picoredis_reply_t *reply = picoredis_send_and_reply2(ctx, PICOREDIS_DECRBY, key, int_value);
    return reply ? reply->v.ivalue : 0;
}

static int picoredis_exec_append(picoredis_t *ctx, const char *key, const char *value)
{
    picoredis_reply_t *reply = picoredis_send_and_reply2(ctx, PICOREDIS_APPEND, key, value);
    return reply ? reply->v.ivalue : 0;
}

static char *picoredis_exec_substr(picoredis_t *ctx, const char *key, int start, int end)
{
    char start_value[64] = {0};
    snprintf(start_value, sizeof(start_value), "%d", start);

    char end_value[64] = {0};
    snprintf(end_value, sizeof(end_value), "%d", end);

    picoredis_reply_t *reply = picoredis_send_and_reply3(ctx, PICOREDIS_SUBSTR, key, start_value, end_value);
    return reply ? reply->v.svalue : NULL;
}

static int picoredis_exec_lpush(picoredis_t *ctx, const char *key, const char *value)
{
    picoredis_reply_t *reply = picoredis_send_and_reply2(ctx, PICOREDIS_LPUSH, key, value);
    return reply ? reply->v.ivalue : 0;
}

static int picoredis_exec_rpush(picoredis_t *ctx, const char *key, const char *value)
{
    picoredis_reply_t *reply = picoredis_send_and_reply2(ctx, PICOREDIS_RPUSH, key, value);
    return reply ? reply->v.ivalue : 0;
}

static int picoredis_exec_llen(picoredis_t *ctx, const char *key)
{
    picoredis_reply_t *reply = picoredis_send_and_reply1(ctx, PICOREDIS_LLEN, key);
    return reply ? reply->v.ivalue : 0;
}

static picoredis_array_t *picoredis_exec_lrange(picoredis_t *ctx, const char *key, int start, int end)
{
    char start_value[64] = {0};
    snprintf(start_value, sizeof(start_value), "%d", start);

    char end_value[64] = {0};
    snprintf(end_value, sizeof(end_value), "%d", end);

    picoredis_reply_t *reply = picoredis_send_and_reply3(ctx, PICOREDIS_LRANGE, key, start_value, end_value);
    return reply ? reply->v.avalue : NULL;
}

static int picoredis_exec_ltrim(picoredis_t *ctx, const char *key, int start, int end)
{
    char start_value[64] = {0};
    snprintf(start_value, sizeof(start_value), "%d", start);

    char end_value[64] = {0};
    snprintf(end_value, sizeof(end_value), "%d", end);

    picoredis_reply_t *reply = picoredis_send_and_reply3(ctx, PICOREDIS_LTRIM, key, start_value, end_value);
    return reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1;
}

static char *picoredis_exec_lindex(picoredis_t *ctx, const char *key, int index)
{
    char int_value[64] = {0};
    snprintf(int_value, sizeof(int_value), "%d", index);

    picoredis_reply_t *reply = picoredis_send_and_reply2(ctx, PICOREDIS_LINDEX, key, int_value);
    return reply ? reply->v.svalue : NULL;
}

static int picoredis_exec_lset(picoredis_t *ctx, const char *key, int index, const char *value)
{
    char int_value[64] = {0};
    snprintf(int_value, sizeof(int_value), "%d", index);

    picoredis_reply_t *reply = picoredis_send_and_reply3(ctx, PICOREDIS_LSET, key, int_value, value);
    return reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1;
}

static int picoredis_exec_lrem(picoredis_t *ctx, const char *key, int count, const char *value)
{
    char int_value[64] = {0};
    snprintf(int_value, sizeof(int_value), "%d", count);

    picoredis_reply_t *reply = picoredis_send_and_reply3(ctx, PICOREDIS_LREM, key, int_value, value);
    return reply ? reply->v.ivalue : 0;
}

static char *picoredis_exec_lpop(picoredis_t *ctx, const char *key)
{
    picoredis_reply_t *reply = picoredis_send_and_reply1(ctx, PICOREDIS_LPOP, key);
    return reply ? reply->v.svalue : NULL;
}

static char *picoredis_exec_rpop(picoredis_t *ctx, const char *key)
{
    picoredis_reply_t *reply = picoredis_send_and_reply1(ctx, PICOREDIS_RPOP, key);
    return reply ? reply->v.svalue : NULL;
}

static char *picoredis_exec_rpoplpush(picoredis_t *ctx, const char *srckey, const char *dstkey)
{
    picoredis_reply_t *reply = picoredis_send_and_reply2(ctx, PICOREDIS_RPOPLPUSH, srckey, dstkey);
    return reply ? reply->v.svalue : NULL;
}

static int picoredis_exec_save(picoredis_t *ctx)
{
    picoredis_reply_t *reply = picoredis_send_and_reply0(ctx, PICOREDIS_SAVE);
    return reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1;
}

static int picoredis_exec_bgsave(picoredis_t *ctx)
{
    picoredis_reply_t *reply = picoredis_send_and_reply0(ctx, PICOREDIS_BGSAVE);
    return reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1;
}

static int picoredis_exec_bgrewriteaof(picoredis_t *ctx)
{
    picoredis_reply_t *reply = picoredis_send_and_reply0(ctx, PICOREDIS_BGREWRITEAOF);
    return reply->type == PICOREDIS_REPLY_ERROR ? 0 : 1;
}

static int picoredis_exec_lastsave(picoredis_t *ctx)
{
    picoredis_reply_t *reply = picoredis_send_and_reply0(ctx, PICOREDIS_LASTSAVE);
    return reply ? reply->v.ivalue : 0;
}

static void picoredis_exec_shutdown(picoredis_t *ctx)
{
    picoredis_send_and_reply0(ctx, PICOREDIS_SHUTDOWN);
}

static char *picoredis_exec_info(picoredis_t *ctx)
{
    picoredis_reply_t *reply = picoredis_send_and_reply0(ctx, PICOREDIS_INFO);
    return reply ? reply->v.svalue : NULL;
}

static void picoredis_error(picoredis_t *ctx)
{
    fprintf(stderr, "%s\n", ctx->error);
}

#endif /* __PICOREDIS_H__ */
