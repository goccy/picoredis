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

static void ASSERT_PTREQ(const char *desc, void *from, void *to)
{
    test_count++;
    if (from == to) {
        SUCCESS(desc);
    } else {
        FAILURE(desc);
    }
}

static void ASSERT_PTRNEQ(const char *desc, void *from, void *to)
{
    test_count++;
    if (from != to) {
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

static void test_command_keys(picoredis_t *ctx)
{
    picoredis_exec_set(ctx, "foo", value);
    picoredis_exec_set(ctx, "foobar", value);
    picoredis_array_t *array = picoredis_exec_keys(ctx, "foo*");
    size_t i = 0;
    size_t is_ok = 0;
    for (; i < picoredis_array_num(array); ++i) {
        const char *value = picoredis_array_get(array, i);
        if (strncmp(value, "foobar", sizeof("foobar")) == 0 ||
            strncmp(value, "foo", sizeof("foo")) == 0) {
            is_ok = 1;
        } else {
            is_ok = 0;
        }
    }
    ASSERT_NUMEQ("key pattern foo* => foo, foobar", is_ok, 1);
}

static void test_command_randomkey(picoredis_t *ctx)
{
    ASSERT_PTRNEQ("found key by randomkey", picoredis_exec_randomkey(ctx), NULL);
}

static void test_command_rename(picoredis_t *ctx)
{
    picoredis_exec_set(ctx, "old_rename_key", value);
    ASSERT_NUMEQ("rename old => new", picoredis_exec_rename(ctx, "old_rename_key", "new_rename_key"), 1);
    ASSERT_NUMEQ("rename old => old", picoredis_exec_rename(ctx, "old_rename_key", "old_rename_key"), 0);
}

static void test_command_renamenx(picoredis_t *ctx)
{
    picoredis_exec_set(ctx, "first_rename_key", value);
    picoredis_exec_set(ctx, "second_rename_key", value);
    picoredis_exec_set(ctx, "exists_rename_key", value);
    picoredis_exec_del(ctx, 1, "not_exists_rename_key");

    ASSERT_NUMEQ("renamenx to not exists key", picoredis_exec_renamenx(ctx, "first_rename_key", "not_exists_rename_key"), 1);
    ASSERT_NUMEQ("renamenx to exists key", picoredis_exec_renamenx(ctx, "second_rename_key", "exists_rename_key"), 0);
}

static void test_command_dbsize(picoredis_t *ctx)
{
    int exists_some_keys = picoredis_exec_dbsize(ctx) > 1;
    ASSERT_NUMEQ("exists some keys", exists_some_keys, 1);
}

static void test_command_expire(picoredis_t *ctx)
{
    picoredis_exec_del(ctx, 1, "expire_key");
    ASSERT_NUMEQ("cannot set expired key", picoredis_exec_expire(ctx, "expire_key", 10), 0);
    picoredis_exec_set(ctx, "expire_key", value);
    ASSERT_NUMEQ("can set expired key", picoredis_exec_expire(ctx, "expire_key", 10), 1);
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
    test_command_keys(ctx);
    test_command_randomkey(ctx);
    test_command_rename(ctx);
    test_command_renamenx(ctx);
    test_command_dbsize(ctx);
    test_command_expire(ctx);
    return 0;
}
