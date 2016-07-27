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

typedef struct {
	const char *host;
	int port;
	const char *error;
	int sock;
	char *receive_buf;
} picoredis_t;

typedef enum {
	PICOREDIS_REPLY_SINGLE_LINE,
	PICOREDIS_REPLY_ERROR,
	PICOREDIS_REPLY_NUM,
	PICOREDIS_REPLY_BULK,
	PICOREDIS_REPLY_MULTI_BULK,
} picoredis_reply_type;

typedef struct {
	picoredis_reply_type type;
	size_t length;
	union {
		char *svalue;
		int ivalue;
	} v;
} picoredis_reply_t;

typedef enum {
	PICOREDIS_SET,
	PICOREDIS_GET,
	PICOREDIS_NONE,
} picoredis_command_type;

typedef struct {
	picoredis_command_type type;
	const char *name;
	size_t name_length;
} picoredis_command_type_t;

picoredis_t *picoredis_alloc(void)
{
	picoredis_t *ret = (picoredis_t *)malloc(sizeof(picoredis_t));
	memset(ret, 0, sizeof(picoredis_t));
	ret->receive_buf = (char *)malloc(BUFSIZ);
	memset(ret->receive_buf, 0, BUFSIZ);
	return ret;
}

void picoredis_free(picoredis_t *ctx)
{
	if (!ctx) return;

	free(ctx->receive_buf);
	free(ctx);
	ctx = NULL;
}

int picoredis_has_error(picoredis_t *ctx)
{
	return ctx->error != NULL;
}

int picoredis_connect_with_ctx(picoredis_t *ctx, const char *host, int port)
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

picoredis_t *picoredis_connect(const char *host, int port)
{
	picoredis_t *ctx = picoredis_alloc();
	ctx->sock = picoredis_connect_with_ctx(ctx, host, port);
	return ctx;
}

size_t picoredis_total_args_length(int nargs, size_t *lengths)
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

char *picoredis_parse_command_args(int nargs, size_t *lengths, const char **values)
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

picoredis_command_type_t picoredis_get_command_type(picoredis_command_type type)
{
	static picoredis_command_type_t all_command_types[] = {
		{ PICOREDIS_SET,  "SET",  3 },
		{ PICOREDIS_GET,  "GET",  3 },
		{ PICOREDIS_NONE, "NONE", 4 }
	};

	size_t i = 0;
	for (; all_command_types[i].type != PICOREDIS_NONE; ++i) {
		if (all_command_types[i].type == type) {
			return all_command_types[i];
		}
	}
	return all_command_types[i];
}

size_t picoredis_command_header_size(size_t nargs, picoredis_command_type_t *type)
{
	size_t total_args_num                 = nargs + 1;
	size_t total_args_num_digit           = (size_t)log10(total_args_num) + 1;
	static const size_t protocol_char     = 8;  // '*', '\r', '\n', '$', '\r', '\n', '\r', '\n'
	size_t name_length_num_digit          = (size_t)log10(type->name_length + 1) + 1;
	return protocol_char + total_args_num_digit + name_length_num_digit + type->name_length;
}

char *picoredis_command_create(picoredis_command_type type, size_t nargs, size_t *lengths, const char **values)
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

int picoredis_send_command(picoredis_t *ctx, char *command)
{
	int ret = send(ctx->sock, command, strlen(command), 0);
    if (ret <= 0) {
		ctx->error = strerror(errno);
    }
	return ret;
}

picoredis_reply_t *picoredis_receive_command(picoredis_t *ctx)
{
	memset(ctx->receive_buf, 0, BUFSIZ);
	int recv_result = recv(ctx->sock, ctx->receive_buf, BUFSIZ, 0);
	if (recv_result <= 0) {
		ctx->error = strerror(errno);
		return NULL;
	}
	char *buf = ctx->receive_buf;
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
			case '+':
				reply->type = PICOREDIS_REPLY_SINGLE_LINE;
				break;
			case '-':
				reply->type = PICOREDIS_REPLY_ERROR;
				break;
			case ':':
				break;
			case '$': {
				reply->type   = PICOREDIS_REPLY_BULK;
				reply->length = atoi(line + 1);
				break;
			}
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
			case PICOREDIS_REPLY_BULK:
				reply->v.svalue = (char *)malloc(reply->length + 1);
				memset(reply->v.svalue, 0, reply->length + 1);
				memcpy((void *)reply->v.svalue, line, reply->length);
				break;
			default:
				break;
			}
		}
	}
	return reply;
}


void picoredis_command_set(picoredis_t *ctx, const char *key, const char *value)
{
	static const size_t nargs = 2;
	size_t lengths[]     = { strlen(key), strlen(value) };
	const char *values[] = { key, value };

	char *command = picoredis_command_create(PICOREDIS_SET, nargs, lengths, values);
	picoredis_send_command(ctx, command);
	if (picoredis_has_error(ctx)) return;

	picoredis_reply_t *reply = picoredis_receive_command(ctx);
	if (!reply) {
		ctx->error = "cannot receive reply";
		return;
	}

	if (reply->v.ivalue == -1) {
		ctx->error = "cannot set";
		return;
	}
}

char *picoredis_command_get(picoredis_t *ctx, const char *key)
{
	static const size_t nargs = 1;
	size_t lengths[]     = { strlen(key) };
	const char *values[] = { key };

	char *command = picoredis_command_create(PICOREDIS_GET, nargs, lengths, values);
	picoredis_send_command(ctx, command);
	if (picoredis_has_error(ctx)) return NULL;

	picoredis_reply_t *reply = picoredis_receive_command(ctx);
	if (!reply) return NULL;

	return reply->v.svalue;
}

void picoredis_error(picoredis_t *ctx)
{
	fprintf(stderr, "%s\n", ctx->error);
}
