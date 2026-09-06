/* SPDX-License-Identifier: Apache-2.0 */
/*
 * Smoke test of the C ABI as C sees it: compiled by run_c_tests.sh against the built library and
 * run without a backend. It exercises the header (types, macros, every documented NULL
 * tolerance), the error channel, and the one behaviour an edge device relies on most — that an
 * unreachable server spools to disk and reports DATAHUB_BUFFERED.
 */
#define _DEFAULT_SOURCE 1 /* mkdtemp under -std=c11 */
#include <intellistream_datahub.h>

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int failures = 0;

#define CHECK(cond)                                                                       \
    do {                                                                                  \
        if (cond) {                                                                       \
            printf("ok   %s\n", #cond);                                                   \
        } else {                                                                          \
            printf("FAIL %s (line %d): %s\n", #cond, __LINE__, datahub_last_error());     \
            failures++;                                                                   \
        }                                                                                 \
    } while (0)

/* A port nothing listens on: the connection is refused at once. */
#define UNREACHABLE "http://127.0.0.1:9"

int main(void) {
    /* --- version and error channel ------------------------------------------------------ */
    CHECK(datahub_version() != NULL);
    CHECK(strcmp(datahub_version(), DATAHUB_VERSION) == 0);
    CHECK(DATAHUB_VERSION_MAJOR >= 0 && DATAHUB_VERSION_MINOR >= 0 && DATAHUB_VERSION_PATCH >= 0);
    CHECK(datahub_last_error() != NULL);
    CHECK(strcmp(datahub_last_error(), "") == 0);
    CHECK(strcmp(datahub_status_name(DATAHUB_BUFFERED), "DATAHUB_BUFFERED") == 0);
    CHECK(DATAHUB_TIME_UNSET == INT64_MIN);

    /* --- NULL tolerance ----------------------------------------------------------------- */
    datahub_client *client = NULL;
    CHECK(datahub_client_new(NULL, &client) == DATAHUB_INVALID_ARGUMENT);
    CHECK(client == NULL);
    CHECK(strcmp(datahub_last_error(), "config must not be NULL") == 0);
    datahub_string_free(NULL);
    datahub_config_free(NULL);
    datahub_client_free(NULL);
    datahub_timeseries_free(NULL);
    datahub_message_free(NULL);
    datahub_datapoints_free(NULL, 0);
    CHECK(datahub_listener_close(NULL) == DATAHUB_OK);
    CHECK(datahub_client_buffered_count(NULL) == 0);

    /* --- configuration errors ----------------------------------------------------------- */
    datahub_config *cfg = datahub_config_new();
    CHECK(cfg != NULL);
    CHECK(datahub_client_new(cfg, &client) == DATAHUB_CONFIG);
    CHECK(client == NULL);
    CHECK(strstr(datahub_last_error(), "BASE_URL") != NULL);
    CHECK(datahub_config_set(cfg, "SCOPE", "organization:*") == DATAHUB_OK);
    CHECK(strcmp(datahub_config_get(cfg, "SCOPE"), "organization:*") == 0);
    CHECK(datahub_config_get(cfg, "AUDIENCE") == NULL);
    CHECK(datahub_config_set_buffer_max_bytes(cfg, 0) == DATAHUB_INVALID_ARGUMENT);
    CHECK(datahub_config_load_envfile(cfg, "/nonexistent/gateway.env") == DATAHUB_IO);

    /* --- a client against an unreachable server, spooling to a temp dir ----------------- */
    char spool_dir[] = "/tmp/datahub-c-smoke-XXXXXX";
    CHECK(mkdtemp(spool_dir) != NULL);
    CHECK(datahub_config_set_base_url(cfg, UNREACHABLE) == DATAHUB_OK);
    CHECK(datahub_config_set_token(cfg, "smoke-token") == DATAHUB_OK);
    CHECK(datahub_config_set_buffer_dir(cfg, spool_dir) == DATAHUB_OK);
    CHECK(datahub_client_new(cfg, &client) == DATAHUB_OK);
    CHECK(client != NULL);
    datahub_config_free(cfg); /* the client copied what it needs */

    /* Recent timestamps: the spool keeps a record only while it is inside the retention window. */
    int64_t now_ms = (int64_t)time(NULL) * 1000;
    datahub_datapoint points[2] = {
        { .timestamp_ms = now_ms, .value = 21.5 },
        { .timestamp_ms = now_ms + 1000, .value = 21.6 },
    };
    CHECK(datahub_datapoints_insert(client, "pump-1/temperature", points, 2) == DATAHUB_BUFFERED);
    CHECK(datahub_client_buffered_count(client) == 2);
    CHECK(datahub_client_flush(client) == DATAHUB_BUFFERED);
    CHECK(datahub_client_buffered_count(client) == 2);

    /* Argument validation happens before anything is spooled or sent. */
    datahub_datapoint bad = { .timestamp_ms = 1, .value = NAN };
    CHECK(datahub_datapoints_insert(client, "pump-1/temperature", &bad, 1) == DATAHUB_INVALID_ARGUMENT);
    CHECK(strcmp(datahub_last_error(), "points[0].value is not finite") == 0);
    CHECK(datahub_datapoints_insert(client, "", points, 2) == DATAHUB_INVALID_ARGUMENT);
    CHECK(datahub_datapoints_insert(client, "pump-1/temperature", NULL, 2) == DATAHUB_INVALID_ARGUMENT);
    CHECK(datahub_datapoints_insert(client, "pump-1/temperature", NULL, 0) == DATAHUB_OK);
    CHECK(datahub_client_buffered_count(client) == 2);

    /* Reads are not buffered: a transport failure is an HTTP 503. */
    datahub_timeseries *ts = NULL;
    CHECK(datahub_timeseries_get_by_external_id(client, "pump-1/temperature", &ts) == DATAHUB_HTTP);
    CHECK(ts == NULL);
    CHECK(datahub_last_http_status() == 503);
    CHECK(strncmp(datahub_last_error(), "503", 3) == 0);

    char *body = NULL;
    CHECK(datahub_request_json(client, "PUT", "/events", NULL, &body) == DATAHUB_INVALID_ARGUMENT);
    CHECK(body == NULL);
    CHECK(datahub_request_json(client, "POST", "/events/create", "{oops", &body) == DATAHUB_INVALID_ARGUMENT);
    CHECK(datahub_events_create_json(client, "{\"items\":[{\"externalId\":\"e\"}]}", &body) == DATAHUB_INVALID_ARGUMENT);
    CHECK(strstr(datahub_last_error(), "event create request") != NULL);

    datahub_client_free(client);

    /* The spool survived the client: a new one on the same directory still holds the backlog. */
    cfg = datahub_config_new();
    datahub_config_set_base_url(cfg, UNREACHABLE);
    datahub_config_set_token(cfg, "smoke-token");
    datahub_config_set_buffer_dir(cfg, spool_dir);
    CHECK(datahub_client_new(cfg, &client) == DATAHUB_OK);
    datahub_config_free(cfg);
    CHECK(datahub_client_flush(client) == DATAHUB_BUFFERED);
    CHECK(datahub_client_buffered_count(client) == 2);
    datahub_client_free(client);

    char cleanup[sizeof(spool_dir) + 16];
    snprintf(cleanup, sizeof cleanup, "rm -rf '%s'", spool_dir);
    if (system(cleanup) != 0) {
        printf("warning: could not remove %s\n", spool_dir);
    }

    if (failures == 0) {
        printf("smoke test passed\n");
        return 0;
    }
    printf("%d check(s) failed\n", failures);
    return 1;
}
