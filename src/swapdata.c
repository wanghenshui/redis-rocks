
#include "server.h"

#include "thpool.h"

//#include "c.h"
/*
		rocksdb submodule
		- c.h for rocksdb api
		- thpoll.h for rocksdb job thread pool, only require `add job` semantic.





*/

threadpool worker_group;


client *fakeRocksClient=NULL;

//static pthread_mutex_t producingHotkeyQueueGIL = PTHREAD_MUTEX_INITIALIZER;

void InitRocksDB(void) {
	
	/* Rocksdb init*/

	//path
	//option
	//background
	
	/* Queues */
	server.producing_hotkey_queue = listCreate();
	server.comsuming_coldkey_queue = listCreate();
	if (pipe(server.producing_hotkey_pipe) == -1) {
		serverLog(LL_WARNING,
			"Can't create the pipe for producing hotkey: %s",
			strerror(errno));
		exit(1);
	}

	if (pipe(server.comsuming_coldkey_pipe) == -1) {
		serverLog(LL_WARNING,
			"Can't create the pipe for comsuming coldkey: %s",
			strerror(errno));
		exit(1);
	}

	anetNonBlock(NULL, server.producing_hotkey_pipe[0]);
	anetNonBlock(NULL, server.producing_hotkey_pipe[1]);
	anetNonBlock(NULL, server.comsuming_coldkey_pipe[0]);
	anetNonBlock(NULL, server.comsuming_coldkey_pipe[1]);
	
	/* create a fake client to swap data */
	fakeRocksClient = createClient(-1);
	fakeRocksClient->flags |= CLIENT_ROCKSDB;

	/* rocksdb job thread pool */
	/* FIXME: need a config*/
	if ((worker_group = thpool_init(4)) == NULL) {
		serverPanic("Error initing thread poll");
	}


	/* FIXME:rocksdb compact thread*/

}


// interface ..
void set() {}


void redis_to_rocksdb(void* obj) {
	robj* o = (robj*)obj;

	switch (o->type) {
	case OBJ_STRING:
		 rdbSaveType(rdb, RDB_TYPE_STRING);
	case OBJ_LIST:
		if (o->encoding == OBJ_ENCODING_QUICKLIST)
			 rdbSaveType(rdb, RDB_TYPE_LIST_QUICKLIST);
		else
			serverPanic("Unknown list encoding");
	case OBJ_SET:
		if (o->encoding == OBJ_ENCODING_INTSET)
			 rdbSaveType(rdb, RDB_TYPE_SET_INTSET);
		else if (o->encoding == OBJ_ENCODING_HT)
			 rdbSaveType(rdb, RDB_TYPE_SET);
		else
			serverPanic("Unknown set encoding");
	case OBJ_ZSET:
		if (o->encoding == OBJ_ENCODING_ZIPLIST)
			 rdbSaveType(rdb, RDB_TYPE_ZSET_ZIPLIST);
		else if (o->encoding == OBJ_ENCODING_SKIPLIST)
			 rdbSaveType(rdb, RDB_TYPE_ZSET_2);
		else
			serverPanic("Unknown sorted set encoding");
	case OBJ_HASH:
		if (o->encoding == OBJ_ENCODING_ZIPLIST)
			 rdbSaveType(rdb, RDB_TYPE_HASH_ZIPLIST);
		else if (o->encoding == OBJ_ENCODING_HT)
			 rdbSaveType(rdb, RDB_TYPE_HASH);
		else
			serverPanic("Unknown hash encoding");
	case OBJ_STREAM:
		 rdbSaveType(rdb, RDB_TYPE_STREAM_LISTPACKS);
	case OBJ_MODULE:
		 rdbSaveType(rdb, RDB_TYPE_MODULE_2);
	default:
		serverPanic("Unknown object type");
	}
}

//怎么展开？伪装命令通知回去？
void rocksdb_to_redis(void* obj) {}


void enqueueColdData() {}

/*
+----------------+     +----------------------------+     +---------------------+     +---------------------+
| hot data limit |     | consumed cold data enqueue |     | pipe event noticed, |     | thread pool add job |
| data cold out  | --> |      pipe event write      | --> |  read and dequeue   | --> |  do queuejob asyc   |
+----------------+     +----------------------------+     +---------------------+     +---------------------+
*/
/* pipe event handle to process cold data */
void swapdataComsumingProcess(aeEventLoop* el, int fd, void* privdata, int mask) {
	UNUSED(el);
	UNUSED(mask);
	UNUSED(privdata);
	char buf[1];
	listNode* ln = NULL;

	/* perhaps missing event? using `while` instead? */
	if (read(server.comsuming_coldkey_pipe[0], buf, 1) == 1) {
		while (listLength(server.comsuming_coldkey_queue)) {
			//need lock or replace to lock-free queue
			ln = listFirst(server.comsuming_coldkey_queue);
			// move encode...
			// rocksdb cmd put
			thpool_add_work(worker_group,redis_to_rocksdb,ln);
			listDelNode(server.comsuming_coldkey_queue, ln);
		}
	}
}




void swapdataProducingProcess(aeEventLoop * el, int fd, void* privdata, int mask) {
	UNUSED(el);
	UNUSED(mask);
	UNUSED(privdata);
	char buf[1];
	listNode *ln = NULL;

	if (read(server.producing_hotkey_pipe[0], buf, 1) == 1) {
		while (listLength(server.producing_hotkey_queue)) {
			/* need lock or replace to lock-free queue */
			ln = listFirst(server.producing_hotkey_queue);
			/* ln->value is a pointer to redis raw data 
				which rocksdb already enqueues,
				just add to dict and ready_keys
			*/
			// processbuffer
			// processcmd
			listDelNode(server.producing_hotkey_queue, ln);
		}
	}
}

void checkColdData() {

}

