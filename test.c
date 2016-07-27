#include "picoredis.h"

static size_t test_count = 0;

static void SUCCESS(const char *desc)
{
	fprintf(stderr, "\033[0;32m[PASS]\033[0;0m - (%zu) %s\n", test_count, desc);
}

static void FAILURE(const char *desc)
{
	fprintf(stderr, "\033[0;31m[FAIL]\033[0;0m - (%zu) %s\n", test_count, desc);
}

static void ASSERT_NUMEQ(const char *desc, int from, int to)
{
	test_count++;
	if (from == to) {
		SUCCESS(desc);
	} else {
		FAILURE(desc);
	}
}

static void ASSERT_STREQ(const char *desc, const char *from, const char *to)
{
	test_count++;
	if (strncmp(from, to, strlen(from)) == 0) {
		SUCCESS(desc);
	} else {
		FAILURE(desc);
	}
}

static const char *key   = "key";
static const char *value = "value";

static void test_command_auth(picoredis_t *ctx)
{
	ASSERT_NUMEQ("auth fail", picoredis_exec_auth(ctx, "password"), 0);
}

static void test_command_set_get(picoredis_t *ctx)
{
	picoredis_exec_set(ctx, key, value);
	ASSERT_STREQ("set - get", picoredis_exec_get(ctx, key), value);
}

static void test_command_exists(picoredis_t *ctx)
{
	ASSERT_NUMEQ("exists key", picoredis_exec_exists(ctx, key), 1);
	ASSERT_NUMEQ("not exists key", picoredis_exec_exists(ctx, "not_key"), 0);
}

static void test_command_type(picoredis_t *ctx)
{
	ASSERT_STREQ("'key' type is string", picoredis_exec_type(ctx, key), "string");
}

static void test_command_del(picoredis_t *ctx)
{
	ASSERT_NUMEQ("delete exists key", picoredis_exec_del(ctx, 2, key, "not_key"), 1);
	ASSERT_NUMEQ("fail delete keys", picoredis_exec_del(ctx, 1, key), 0);
}

int main(int argc, char **argv)
{
	picoredis_t *ctx = picoredis_connect("127.0.0.1", 6379);
	if (picoredis_has_error(ctx)) {
		picoredis_error(ctx);
		return 1;
	}
	test_command_auth(ctx);
	test_command_set_get(ctx);
	test_command_exists(ctx);
	test_command_type(ctx);
	test_command_del(ctx);
	return 0;
}
