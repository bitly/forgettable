#include "forgettable.h"
#include <simplehttp/queue.h>
#include <simplehttp/simplehttp.h>
#include <json/json.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <bitly.h>
#include "helper.h"

#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <hiredis/adapters/libevent.h>

static int NUM_WORKERS;
static time_t processStarted;

struct WorkerEndpoint *workers;

inline unsigned int find_worker(const char *distribution)
{
    return ((unsigned int) crc32((unsigned char *)distribution, strlen(distribution))) % NUM_WORKERS;
}

void GetHandler(struct evhttp_request *req, struct evbuffer *buf, void *arg)
{
    int worker_index;
    const char *distribution, *bin;
    struct evkeyvalq args;
    
    evhttp_parse_query(req->uri, &args);
    distribution = evhttp_find_header(&args, "distribution");
    bin = evhttp_find_header(&args, "bin");

    if (distribution == NULL) {
        _LOG("Error handling GeneralHandler: Could not extract distribution key\n");
        AddOutputHeaders(req->output_headers, "text/plain", buf);
        evhttp_send_reply(req, HTTP_OK, "MISSING_ARG_DISTRIBUTION", buf);
        evhttp_clear_headers(&args);
        return;
    }
    
    worker_index = find_worker(distribution);
    if (workers[worker_index].status != REDIS_CONNECT) {
        _LOG("Shard unavailible: %d\n", worker_idx);
        evhttp_send_reply(req, 500, "SHARD_UNAVAILABLE", NULL);
        return;
    }
    _LOG("Routing get operation to worker %d: %s\n", worker_index, req->uri);

    char *request_str;
    if (bin == NULL) {
        sprintf(&request_str, "ZRANGE %s", host);
    } else {
        //TODO: THIS PART
    }
    
    simplehttp_async_enable(req);
    redisAsyncCommand(workers[worker_index].rdb, GetFinalize, (void*)req, request_str);
    evhttp_clear_headers(&args);
}

void GetFinalize(redisAsyncContext *c, void *r, void *privdata) //(struct evhttp_request *req, void *cb_ctx)
{
    struct evhttp_request *oreq = (struct evhttp_request *)privdata;
    redisReply *reply = r;

    if (reply == NULL) {
        evhttp_send_reply(oreq, 404, "DISTRIBUTION_NOT_FOUND", NULL);
    }

    struct json_object *jsonobj = json_object_new_object();
    struct json_object *result_array = json_object_new_array();

    if (reply->type == REDIS_REPLY_ARRAY) {
        for (j = 0; j < reply->elements; j++) {
            printf("%u) %s\n", j, reply->element[j]->str);
        }
    }
    evhttp_send_reply(oreq, 418, "I'm a little teapot", oreq->input_buffer);
    simplehttp_async_finish(oreq);
}

void connectCallback(const redisAsyncContext *c, int status) {
    WorkerEndpoint *worker = redisAsyncContext->data;
    if (status == REDIS_OK) {
        printf("[%d] Connected to host: %s:%d\n", worker.id, worker.host, worker.port);
        worker.status = REDIS_CONNECT;
    } else {
        printf("[%d] Failed to connect to host: %s:%d: %s\n", worker.id, worker.host, worker.port, c->errstr);
        worker.status = REDIS_DISCONNECTED;
    }
}

bool ParseHosts(char *hosts_csv_orig)
{
    char *hosts_csv, *hosts_to_free, *token, *tmp;
    int num_workers = 1;
    
    tmp = hosts_csv_orig;
    while (*tmp != '\0') {
        if (*tmp == ',') {
            num_workers++;
        }
        ++tmp;
    }
    
    NUM_WORKERS = num_workers;
    workers = (struct WorkerEndpoint *) malloc( num_workers * sizeof(struct WorkerEndpoint));
    
    hosts_to_free = hosts_csv = strdup(hosts_csv_orig);
    int i = 0;
    redisAsyncSetConnectCallback(c, connectCallback);
    while ( (token = strsep(&hosts_csv, ",")) != NULL && i < num_workers) {
        workers[i].address = strdup(strsep(&token, ":"));
        workers[i].port = atoi(strsep(&token, ":"));
        workers[i].id = i;
        workers[i].status = REDIS_DISCONNECTED;
        *(workers[i].rdb) = redisAsyncConnect(workers[i].address, workers[i].port);
        workers[i].rdb->data = (void*)workers[i];
        redisAsyncSetConnectCallback(workers[i].rdb, connectCallback);

        fprintf(stderr, "Added worker: %s @ %d - %d\n", workers[i].address, workers[i].port, workers[i].id);
        i++;
    }
    free(hosts_to_free);
    
    return true;
}

int version_cb(int value)
{
    fprintf(stdout, "Version: %s\n", VERSION);
    exit(0);
}

int main(int argc, char **argv)
{
    processStarted = time(NULL);
    
    define_simplehttp_options();
    option_define_str  ( "shard_hosts" , OPT_REQUIRED , NULL , NULL , NULL       , "comma delinated list of shard hosts with ports (localhost:6060,localhost60601)") ;
    option_define_bool ( "version"     , OPT_OPTIONAL , 0    , NULL , version_cb , VERSION) ;
    
    if (!option_parse_command_line(argc, argv)) {
        return 1;
    }
    
    if (ParseHosts(option_get_str("shard_hosts")) == false) {
        fprintf(stderr, "Could not parse shard hosts\n");
        return 1;
    }
    
    fprintf(stderr, "Version: %s\n", VERSION);
    fprintf(stderr, "use --help for options\n");
    
#ifdef DEBUG
    init_async_connection_pool(1);
#else
    init_async_connection_pool(0);
#endif
    
    simplehttp_init();
    
    /* Set a callback for requests to "/specific". */
    // NOTE: order is important. glob'd names must come last.
    simplehttp_set_cb("/get", GeneralHandler, NULL);
    
    simplehttp_main();
    
    free_options();
    free_async_connection_pool();
    
    int i;
    for (i = 0; i < NUM_WORKERS; i++) {
        free(workers[i].address);
    }
    free(workers);
    
    return 0;
}
