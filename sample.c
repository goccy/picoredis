#include "picoredis.h"

int main(int argc, char **argv)
{
	picoredis_t *redis_ctx = picoredis_connect("127.0.0.1", 6379);
	if (picoredis_has_error(redis_ctx)) {
		picoredis_error(redis_ctx);
		return 1;
	}
	static const char *key   = "mykey";
	static const char *value = "myvalue";
	picoredis_command_set(redis_ctx, key, value);
	if (!picoredis_has_error(redis_ctx)) {
		fprintf(stderr, "SET: [%s:%s]\n", key, value);
		char *value = picoredis_command_get(redis_ctx, "mykey");
		if (picoredis_has_error(redis_ctx)) {
			picoredis_error(redis_ctx);
			return 1;
		}
		fprintf(stderr, "GET: [%s]\n", value);
		free(value);
	} else {
		picoredis_error(redis_ctx);
		return 1;
	}
	picoredis_free(redis_ctx);
	return 0;
}
