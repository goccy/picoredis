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

static void test_command_expireat(picoredis_t *ctx)
{
    picoredis_exec_del(ctx, 1, "expire_key");
    ASSERT_NUMEQ("cannot set expired key", picoredis_exec_expireat(ctx, "expire_key", 10), 0);
    picoredis_exec_set(ctx, "expire_key", value);
    ASSERT_NUMEQ("can set expired key", picoredis_exec_expireat(ctx, "expire_key", 10), 1);
}

static void test_command_persist(picoredis_t *ctx)
{
    picoredis_exec_del(ctx, 1, "expire_key");
    ASSERT_NUMEQ("cannot persist for expired key", picoredis_exec_persist(ctx, "expire_key"), 0);
    picoredis_exec_set(ctx, "expire_key", value);
    picoredis_exec_expire(ctx, "expire_key", 10);
    ASSERT_NUMEQ("can persist for expired key", picoredis_exec_persist(ctx, "expire_key"), 1);
}

static void test_command_ttl(picoredis_t *ctx)
{
    picoredis_exec_del(ctx, 1, "expire_key");
    ASSERT_NUMEQ("ttl is -2 (key does not exist)", picoredis_exec_ttl(ctx, "expire_key"), -2);
    picoredis_exec_set(ctx, "expire_key", value);
    ASSERT_NUMEQ("ttl is -1 (key exists but has no associated expire)", picoredis_exec_ttl(ctx, "expire_key"), -1);
    picoredis_exec_expire(ctx, "expire_key", 10);
    ASSERT_NUMEQ("ttl is 10", picoredis_exec_ttl(ctx, "expire_key"), 10);
}

static void test_command_select(picoredis_t *ctx)
{
    ASSERT_NUMEQ("select 1", picoredis_exec_select(ctx, 1), 1);
    ASSERT_NUMEQ("select 0", picoredis_exec_select(ctx, 0), 1);
}

static void test_command_move(picoredis_t *ctx)
{
    picoredis_exec_select(ctx, 1);
    picoredis_exec_del(ctx, 1, "move_key");
    picoredis_exec_select(ctx, 0);
    picoredis_exec_del(ctx, 1, "move_key");

    picoredis_exec_set(ctx, "move_key", value);
    ASSERT_NUMEQ("move key from 0 to 1", picoredis_exec_move(ctx, "move_key", 1), 1);
    picoredis_exec_select(ctx, 1);
    ASSERT_STREQ("exists key on db1", picoredis_exec_get(ctx, "move_key"), value);
    picoredis_exec_select(ctx, 0);
}

static void test_command_flushdb(picoredis_t *ctx)
{
    ASSERT_NUMEQ("flush all keys", picoredis_exec_flushdb(ctx), 1);
    ASSERT_PTREQ("not found keys", picoredis_exec_randomkey(ctx), NULL);
}

static void test_command_flushall(picoredis_t *ctx)
{
    ASSERT_NUMEQ("flush all db", picoredis_exec_flushall(ctx), 1);
    ASSERT_PTREQ("not found keys", picoredis_exec_randomkey(ctx), NULL);
}

static void test_command_watch(picoredis_t *ctx)
{
    picoredis_exec_set(ctx, key, value);
    ASSERT_NUMEQ("watch", picoredis_exec_watch(ctx, 1, key), 1);
}

static void test_command_unwatch(picoredis_t *ctx)
{
    ASSERT_NUMEQ("unwatch", picoredis_exec_unwatch(ctx), 1);
}

static void test_command_multi(picoredis_t *ctx)
{
    ASSERT_NUMEQ("multi", picoredis_exec_multi(ctx), 1);
}

static void test_command_exec(picoredis_t *ctx)
{
    // before executed 'multi' command
    picoredis_exec_set(ctx, key, value);
    picoredis_exec_get(ctx, key);
    ASSERT_NUMEQ("exec", picoredis_exec_exec(ctx), 1);
}

static void test_command_discard(picoredis_t *ctx)
{
    // previous flush by multi
    ASSERT_NUMEQ("discard", picoredis_exec_discard(ctx), 0);
}

static void test_command_sort(picoredis_t *ctx)
{
    ASSERT_PTREQ("sort", picoredis_exec_sort(ctx, 1, key), NULL);
}

static void test_command_getset(picoredis_t *ctx)
{
    picoredis_exec_set(ctx, key, value);
    ASSERT_STREQ("getset", picoredis_exec_getset(ctx, key, "new_value"), value);
    ASSERT_STREQ("get", picoredis_exec_get(ctx, key), "new_value");
}

static void test_command_setnx(picoredis_t *ctx)
{
    ASSERT_NUMEQ("setnx if not exists", picoredis_exec_setnx(ctx, key, value), 0);
    ASSERT_NUMEQ("setnx if not exists", picoredis_exec_setnx(ctx, "lock_key", value), 1);
}

static void test_command_setex(picoredis_t *ctx)
{
    ASSERT_NUMEQ("setex if not exists", picoredis_exec_setex(ctx, key, 10, value), 1);
}

static void test_command_mset(picoredis_t *ctx)
{
    ASSERT_NUMEQ("mset", picoredis_exec_mset(ctx, 2, key, value), 1);
}

static void test_command_msetnx(picoredis_t *ctx)
{
    ASSERT_NUMEQ("msetnx", picoredis_exec_mset(ctx, 2, key, value), 1);
}

static void test_command_incr(picoredis_t *ctx)
{
    picoredis_exec_set(ctx, key, "1");
    ASSERT_NUMEQ("incr", picoredis_exec_incr(ctx, key), 2);
    ASSERT_NUMEQ("incrby", picoredis_exec_incrby(ctx, key, 3), 5);
}

static void test_command_decr(picoredis_t *ctx)
{
    picoredis_exec_set(ctx, key, "10");
    ASSERT_NUMEQ("decr", picoredis_exec_decr(ctx, key), 9);
    ASSERT_NUMEQ("decrby", picoredis_exec_decrby(ctx, key, 4), 5);
}

static void test_command_append(picoredis_t *ctx)
{
    picoredis_exec_set(ctx, key, "hello");
    ASSERT_NUMEQ("append", picoredis_exec_append(ctx, key, "world"), strlen("helloworld"));
}

static void test_command_substr(picoredis_t *ctx)
{
    picoredis_exec_set(ctx, key, "hello");
    ASSERT_STREQ("substr", picoredis_exec_substr(ctx, key, 1, 3), "ell");
}

static void test_command_lrpush(picoredis_t *ctx)
{
    picoredis_exec_del(ctx, 1, key);
    ASSERT_NUMEQ("lpush", picoredis_exec_lpush(ctx, key, "1"), 1);
    ASSERT_NUMEQ("rpush", picoredis_exec_rpush(ctx, key, "3"), 2);
}

static void test_command_llen(picoredis_t *ctx)
{
    picoredis_exec_del(ctx, 1, key);
    picoredis_exec_lpush(ctx, key, "2");
    picoredis_exec_lpush(ctx, key, "3");
    picoredis_exec_lpush(ctx, key, "4");
    ASSERT_NUMEQ("llen", picoredis_exec_llen(ctx, key), 3);
}

static void test_command_lrange(picoredis_t *ctx)
{
    picoredis_exec_del(ctx, 1, key);
    picoredis_exec_lpush(ctx, key, "2");
    picoredis_exec_lpush(ctx, key, "3");
    picoredis_exec_lpush(ctx, key, "4");
    picoredis_array_t *array = picoredis_exec_lrange(ctx, key, 0, 2);
    size_t i  = 0;
    int is_ok = 0;
    for (; i < picoredis_array_num(array); ++i) {
        const char *value = picoredis_array_get(array, i);
        if ((i == 0 && strncmp(value, "4", 1) == 0) ||
            (i == 1 && strncmp(value, "3", 1) == 0) ||
            (i == 2 && strncmp(value, "2", 1) == 0)) {
            is_ok = 1;
        } else {
            is_ok = 0;
        }
    }
    ASSERT_NUMEQ("lrange", is_ok, 1);
}

static void test_command_ltrim(picoredis_t *ctx)
{
    picoredis_exec_del(ctx, 1, key);
    picoredis_exec_lpush(ctx, key, "2");
    picoredis_exec_lpush(ctx, key, "3");
    picoredis_exec_lpush(ctx, key, "4");
    ASSERT_NUMEQ("current list num == 3", picoredis_exec_llen(ctx, key), 3);
    ASSERT_NUMEQ("ltrim", picoredis_exec_ltrim(ctx, key, 0, 1), 1);
    ASSERT_NUMEQ("current list num == 2", picoredis_exec_llen(ctx, key), 2);
}

static void test_command_lindex(picoredis_t *ctx)
{
    picoredis_exec_del(ctx, 1, key);
    picoredis_exec_lpush(ctx, key, "2");
    picoredis_exec_lpush(ctx, key, "3");
    picoredis_exec_lpush(ctx, key, "4");
    ASSERT_NUMEQ("lindex(1) is 3", atoi(picoredis_exec_lindex(ctx, key, 1)), 3);
}

static void test_command_lset(picoredis_t *ctx)
{
    picoredis_exec_del(ctx, 1, key);
    picoredis_exec_lpush(ctx, key, "2");
    picoredis_exec_lpush(ctx, key, "3");
    picoredis_exec_lpush(ctx, key, "4");
    ASSERT_NUMEQ("lset(1) to 5", picoredis_exec_lset(ctx, key, 1, "5"), 1);
    ASSERT_NUMEQ("lindex(1) is 5", atoi(picoredis_exec_lindex(ctx, key, 1)), 5);
}

static void test_command_lrem(picoredis_t *ctx)
{
    picoredis_exec_del(ctx, 1, key);
    picoredis_exec_lpush(ctx, key, "2");
    picoredis_exec_lpush(ctx, key, "3");
    picoredis_exec_lpush(ctx, key, "3");
    picoredis_exec_lpush(ctx, key, "4");
    ASSERT_NUMEQ("lrem 3", picoredis_exec_lrem(ctx, key, 0, "3"), 2);
}

static void test_command_lrpop(picoredis_t *ctx)
{
    picoredis_exec_del(ctx, 1, key);
    picoredis_exec_lpush(ctx, key, "2");
    picoredis_exec_lpush(ctx, key, "3");
    picoredis_exec_lpush(ctx, key, "3");
    picoredis_exec_lpush(ctx, key, "4");
    ASSERT_NUMEQ("lpop", atoi(picoredis_exec_lpop(ctx, key)), 4);
    ASSERT_NUMEQ("rpop", atoi(picoredis_exec_rpop(ctx, key)), 2);
}

static void test_command_rpoplpush(picoredis_t *ctx)
{
    static const char *list1 = "list1";
    static const char *list2 = "list2";
    picoredis_exec_del(ctx, 1, list1);
    picoredis_exec_rpush(ctx, list1, "2");
    picoredis_exec_rpush(ctx, list1, "3");
    picoredis_exec_rpush(ctx, list1, "4");

    picoredis_exec_del(ctx, 1, list2);
    picoredis_exec_lpush(ctx, list2, "1");

    ASSERT_NUMEQ("rpoplpush", atoi(picoredis_exec_rpoplpush(ctx, list1, list2)), 4);
    ASSERT_NUMEQ("list1 len", picoredis_exec_llen(ctx, list1), 2);
    ASSERT_NUMEQ("list2 len", picoredis_exec_llen(ctx, list2), 2);
}

static void test_command_save(picoredis_t *ctx)
{
    ASSERT_NUMEQ("save", picoredis_exec_save(ctx), 1);
}

static void test_command_bgsave(picoredis_t *ctx)
{
    ASSERT_NUMEQ("bgsave", picoredis_exec_bgsave(ctx), 1);
}

static void test_command_bgrewriteaof(picoredis_t *ctx)
{
    ASSERT_NUMEQ("bgrewriteaof", picoredis_exec_bgrewriteaof(ctx), 1);
}

static void test_command_lastsave(picoredis_t *ctx)
{
    int is_ok = picoredis_exec_lastsave(ctx) > 0;
    ASSERT_NUMEQ("lastsave", is_ok, 1);
}

static void test_command_info(picoredis_t *ctx)
{
    ASSERT_PTRNEQ("info", picoredis_exec_info(ctx), NULL);
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
    test_command_expireat(ctx);
    test_command_persist(ctx);
    test_command_ttl(ctx);
    test_command_select(ctx);
    test_command_move(ctx);
    test_command_flushdb(ctx);
    test_command_flushall(ctx);
    test_command_watch(ctx);
    test_command_unwatch(ctx);
    test_command_multi(ctx);
    test_command_exec(ctx);
    test_command_discard(ctx);
    test_command_sort(ctx);
    test_command_getset(ctx);
    test_command_setnx(ctx);
    test_command_setex(ctx);
    test_command_mset(ctx);
    test_command_msetnx(ctx);
    test_command_incr(ctx);
    test_command_decr(ctx);
    test_command_append(ctx);
    test_command_substr(ctx);
    test_command_lrpush(ctx);
    test_command_llen(ctx);
    test_command_lrange(ctx);
    test_command_ltrim(ctx);
    test_command_lindex(ctx);
    test_command_lset(ctx);
    test_command_lrem(ctx);
    test_command_lrpop(ctx);
    test_command_rpoplpush(ctx);
    test_command_save(ctx);
    test_command_bgsave(ctx);
    test_command_bgrewriteaof(ctx);
    test_command_lastsave(ctx);
    test_command_info(ctx);
    return 0;
}
