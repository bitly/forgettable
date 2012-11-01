#ifndef _FORGETTABLE_H
#define _FORGETTABLE_H

#include <stdbool.h>
#include <json/json.h>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>

unsigned int REDIS_DISCONNECTED = 0,
             REDIS_CONNECT = 1,
             REDIS_RO = 2,
             REDIS_WO = 3;

struct WorkerEndpoint {
    int id;
    char *address;
    int port;
    unsigned int status;
    redisAsyncContext *rdb;
};

struct MultiRequestData {
    struct evhttp_request *req;
    struct evkeyvalq *args;
    char *path;
    int outstanding_requests;
    struct evbuffer *buf;
    struct json_object  *jsonobj;
};

struct MultiRequestTracker {
    struct MultiRequestData *data;
    int worker_index;
};


void GeneralHandler(struct evhttp_request *req, struct evbuffer *buf, void *arg);
void SimpleFinalize(struct evhttp_request *req, void *cb_ctx);
bool ParseHosts(char *hosts_csv_orig);
inline unsigned int find_worker(const char *host);

void finalize_json(struct evhttp_request *req, struct evbuffer *evb,
                   struct evkeyvalq *args, struct json_object *jsobj)
{
    char *json, *jsonp, buf[32];
    
    jsonp = (char *)evhttp_find_header(args, "jsonp");
    json = (char *)json_object_to_json_string(jsobj);
    if (jsonp) {
        evbuffer_add_printf(evb, "%s(%s)\n", jsonp, json);
    } else {
        evbuffer_add_printf(evb, "%s\n", json);
    }
    if (evb) {
        sprintf(buf, "%d", (int)EVBUFFER_LENGTH(evb));
        evhttp_add_header(req->output_headers, "Content-Length", buf);
    }
    json_object_put(jsobj); // Odd free function
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
    evhttp_clear_headers(args);
}

#endif

