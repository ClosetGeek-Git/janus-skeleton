#include <map>
#include <thread>
#include <memory>
#include <vector>
#include <string>

extern "C" {
#include "transports/transport.h"


#include "zhelpers.h"

#include "debug.h"
#include "apierror.h"
#include "config.h"
#include "mutex.h"
#include "utils.h"
}

#define JANUS_SKELETONTRAN_VERSION			1
#define JANUS_SKELETONTRAN_VERSION_STRING	"0.0.1"
#define JANUS_SKELETONTRAN_DESCRIPTION		"This transport plugin adds SKELETONTRAN support to the Janus API."
#define JANUS_SKELETONTRAN_NAME				"JANUS SKELETONTRAN transport plugin"
#define JANUS_SKELETONTRAN_AUTHOR			"Jason Lester"
#define JANUS_SKELETONTRAN_PACKAGE			"janus.transport.skeletontran"

using namespace std;

std::unique_ptr<janus_transport> janusTransport;

int janus_skeletontran_init(janus_transport_callbacks *callback, const char *config_path);
void janus_skeletontran_destroy(void);
int janus_skeletontran_get_api_compatibility(void);
int janus_skeletontran_get_version(void);
const char *janus_skeletontran_get_version_string(void);
const char *janus_skeletontran_get_description(void);
const char *janus_skeletontran_get_name(void);
const char *janus_skeletontran_get_author(void);
const char *janus_skeletontran_get_package(void);
gboolean janus_skeletontran_is_janus_api_enabled(void);
gboolean janus_skeletontran_is_admin_api_enabled(void);
int janus_skeletontran_send_message(janus_transport_session *transport, void *request_id, gboolean admin, json_t *message);
void janus_skeletontran_session_created(janus_transport_session *transport, guint64 session_id);
void janus_skeletontran_session_over(janus_transport_session *transport, guint64 session_id, gboolean timeout, gboolean claimed);
void janus_skeletontran_session_claimed(janus_transport_session *transport, guint64 session_id);
json_t *janus_skeletontran_query_transport(json_t *request);


extern "C" janus_transport* create()
{
    JANUS_LOG(LOG_VERB, "%s created!\n", JANUS_SKELETONTRAN_NAME);

    janusTransport = std::make_unique<janus_transport>(janus_transport {
		.init = janus_skeletontran_init,
		.destroy = janus_skeletontran_destroy,

		.get_api_compatibility = janus_skeletontran_get_api_compatibility,
		.get_version = janus_skeletontran_get_version,
		.get_version_string = janus_skeletontran_get_version_string,
		.get_description = janus_skeletontran_get_description,
		.get_name = janus_skeletontran_get_name,
		.get_author = janus_skeletontran_get_author,
		.get_package = janus_skeletontran_get_package,

		.is_janus_api_enabled = janus_skeletontran_is_janus_api_enabled,
		.is_admin_api_enabled = janus_skeletontran_is_admin_api_enabled,

		.send_message = janus_skeletontran_send_message,
		.session_created = janus_skeletontran_session_created,
		.session_over = janus_skeletontran_session_over,
		.session_claimed = janus_skeletontran_session_claimed,

		.query_transport = janus_skeletontran_query_transport,
        });

    return janusTransport.get();
}

/* Useful stuff */ 
static gint initialized = 0, stopping = 0;
static janus_transport_callbacks *gateway = NULL;
static gboolean notify_events = TRUE;

GHashTable *pending_transaction_hashtable = NULL;
static janus_mutex pending_transactions_hashtable_mutex = JANUS_MUTEX_INITIALIZER;

GHashTable *session_id_hashtable = NULL;
static janus_mutex session_id_hashtable_mutex = JANUS_MUTEX_INITIALIZER;

/* JSON serialization options */
static size_t json_format = JSON_INDENT(3) | JSON_PRESERVE_ORDER;

#define BUFFER_SIZE		8192

/* Parameter validation (for tweaking and queries via Admin API) */
static struct janus_json_parameter request_parameters[] = {
	{"request", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};
static struct janus_json_parameter configure_parameters[] = {
	{"events", JANUS_JSON_BOOL, 0},
	{"json", JSON_STRING, 0},
};
/* Error codes (for the tweaking and queries via Admin API) */
#define JANUS_SKELETONTRAN_ERROR_INVALID_REQUEST		411
#define JANUS_SKELETONTRAN_ERROR_MISSING_ELEMENT		412
#define JANUS_SKELETONTRAN_ERROR_INVALID_ELEMENT		413
#define JANUS_SKELETONTRAN_ERROR_UNKNOWN_ERROR		499


static GThread *handler_thread = NULL;
void *janus_skeletontran_thread(void *data);

typedef struct janus_skeletontran_client {
	gboolean admin;					/* Whether this client is for the Admin or Janus API */
	GAsyncQueue *messages;			/* Queue of outgoing messages to push */
	janus_transport_session *ts;	/* Janus core-transport session */
} janus_skeletontran_client;


typedef struct session_transport {
	gint64 session_id;
    int64_t timeout;
    gboolean admin;
	volatile gint destroyed;
	janus_refcount ref;

} session_transport;

static janus_skeletontran_client client;
static janus_skeletontran_client admin_client;

gboolean has_admin;

static void session_id_hashmap_destroy(session_transport *trns)
{
	if(trns && g_atomic_int_compare_and_exchange(&trns->destroyed, 0, 1))
	{
		janus_refcount_decrease(&trns->ref);
	}
}

static void session_id_hashmap_free(const janus_refcount *session_ref)
{
	session_transport *trns = janus_refcount_containerof(session_ref, session_transport, ref);

	janus_refcount_decrease(&trns->ref);

	free(trns);
}


int janus_skeletontran_init(janus_transport_callbacks *callback, const char *config_path)
{
	if(g_atomic_int_get(&stopping)) {
		return -1;
	}

	if(callback == NULL || config_path == NULL) {
		/* Invalid arguments */
		return -1;
	}

	gateway = callback;

    gboolean success = TRUE;

    pending_transaction_hashtable = g_hash_table_new_full(g_str_hash, g_str_equal, NULL, NULL);
    session_id_hashtable = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, (GDestroyNotify)session_id_hashmap_destroy);

    has_admin = FALSE;

    do
    {
        /* Create the clients */
        memset(&client, 0, sizeof(janus_skeletontran_client));

        client.admin = FALSE;
        client.messages = g_async_queue_new();

        /* Create a transport instance as well */
        client.ts = janus_transport_session_create(&client, NULL);
        
        /* Notify handlers about this new transport */

        /**
         * @brief THIS IS BROKEN. NOTE THAT BOTH ARE SIGNALLING 'admin_api'
                  ARE SIGNALS EVEN NEEDED HERE?
         * 
         */

        // NO CLUE... NEED TO FIGURE OUT. CHECK OUT ORIGINAL NANOMSG TRANSPORT
        char addy[128];
        char admin_addy[128];

        if(notify_events && gateway->events_is_enabled()) {
            json_t *info = json_object();
            json_object_set_new(info, "event", json_string("created"));
            json_object_set_new(info, "admin_api", json_false());
            json_object_set_new(info, "address", json_string(addy));
            gateway->notify_event(janusTransport.get(), client.ts, info);
        }

        if(has_admin == TRUE)
        {
            memset(&admin_client, 0, sizeof(janus_skeletontran_client));

            admin_client.admin = TRUE;
            admin_client.messages = g_async_queue_new();
            /* Create a transport instance as well */
            admin_client.ts = janus_transport_session_create(&admin_client, NULL);
            /* Notify handlers about this new transport */
            if(notify_events && gateway->events_is_enabled()) {
                json_t *info = json_object();
                json_object_set_new(info, "event", json_string("created"));
                json_object_set_new(info, "admin_api", json_true());
                json_object_set_new(info, "address", json_string(admin_addy));
                gateway->notify_event(janusTransport.get(), admin_client.ts, info);
            }
        }

        g_atomic_int_set(&initialized, 1);

        /* Start the handler thread */
        GError *thread_error = NULL;
        handler_thread = g_thread_try_new("skeletontran thread", &janus_skeletontran_thread, NULL, &thread_error);

        if(thread_error != NULL) {
            g_atomic_int_set(&initialized, 0);
            JANUS_LOG(LOG_ERR, "Got error %d (%s) trying to launch the SKELETONTRAN thread...\n", thread_error->code, thread_error->message ? thread_error->message : "??");
            g_error_free(thread_error);
            success = FALSE;
            break;
        }

    }while(FALSE);

    if(success == FALSE)
    {
        g_atomic_int_set(&initialized, 0);
        g_atomic_int_set(&stopping, 1);
        /* If we got here, something went wrong */
        success = FALSE;

        if(client.ts != NULL)
        {
            janus_transport_session_destroy(client.ts);
            client.ts = NULL;
        }

        if(admin_client.ts != NULL)
        {     
            janus_transport_session_destroy(admin_client.ts);
            admin_client.ts = NULL;
        }
        return -1;
        
    }

	g_atomic_int_set(&initialized, 1);
	JANUS_LOG(LOG_INFO, "%s initialized!\n", JANUS_SKELETONTRAN_NAME);

    return 0;
}

void janus_skeletontran_destroy(void) {
	if(!g_atomic_int_get(&initialized))
		return;
	
    g_atomic_int_set(&stopping, 1);

    if(client.ts != NULL)
    {
        janus_transport_session_destroy(client.ts);
        client.ts = NULL;
    }

    if(admin_client.ts != NULL)
    {     
        janus_transport_session_destroy(admin_client.ts);
        admin_client.ts = NULL;
    }

	if(handler_thread != NULL) {
		g_thread_join(handler_thread);
		handler_thread = NULL;
	}

	g_atomic_int_set(&initialized, 0);
	g_atomic_int_set(&stopping, 0);

	JANUS_LOG(LOG_INFO, "%s destroyed!\n", JANUS_SKELETONTRAN_NAME);
}

int janus_skeletontran_get_api_compatibility(void) {	return JANUS_TRANSPORT_API_VERSION; }
int janus_skeletontran_get_version(void) { return JANUS_SKELETONTRAN_VERSION; }
const char *janus_skeletontran_get_version_string(void) { return JANUS_SKELETONTRAN_VERSION_STRING; }
const char *janus_skeletontran_get_description(void) { return JANUS_SKELETONTRAN_DESCRIPTION; }
const char *janus_skeletontran_get_name(void) { return JANUS_SKELETONTRAN_NAME; }
const char *janus_skeletontran_get_author(void) { return JANUS_SKELETONTRAN_AUTHOR; }
const char *janus_skeletontran_get_package(void) { return JANUS_SKELETONTRAN_PACKAGE; }

gboolean janus_skeletontran_is_janus_api_enabled(void) {
	return TRUE;
}

gboolean janus_skeletontran_is_admin_api_enabled(void) {
	return has_admin;
}

void janus_skeletontran_session_created(janus_transport_session *transport, guint64 session_id) {
}
void janus_skeletontran_session_over(janus_transport_session *transport, guint64 session_id, gboolean timeout, gboolean claimed) {
}
void janus_skeletontran_session_claimed(janus_transport_session *transport, guint64 session_id) {
}

json_t *janus_skeletontran_query_transport(json_t *request) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		return NULL;
	}
	
	json_t *response = json_object();
	int error_code = 0;
	char error_cause[512];
	
    JANUS_VALIDATE_JSON_OBJECT(request, request_parameters,	error_code, error_cause, TRUE, JANUS_SKELETONTRAN_ERROR_MISSING_ELEMENT, JANUS_SKELETONTRAN_ERROR_INVALID_ELEMENT);
	
    if(error_code == 0)
	{
        const char *request_text = json_string_value(json_object_get(request, "request"));
        
        if(!strcasecmp(request_text, "configure"))
        {
            // DO CONFIG SHIT HERE
        
        } else {
            JANUS_LOG(LOG_VERB, "Unknown request '%s'\n", request_text);
            error_code = JANUS_SKELETONTRAN_ERROR_INVALID_REQUEST;
            g_snprintf(error_cause, 512, "Unknown request '%s'", request_text);
        }
    }


    if(error_code != 0) {
        json_object_set_new(response, "error_code", json_integer(error_code));
        json_object_set_new(response, "error", json_string(error_cause));
    }
    return response;

}


// MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS 
// MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS 
// MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS 
// MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS 
// MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS 
// MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS // MESSAGE HANDLERS 


int janus_skeletontran_send_message(janus_transport_session *transport, void *request_id, gboolean admin, json_t *message)
{
	if(message == NULL)
		return -1;

	char *payload = json_dumps(message, json_format);
	json_decref(message);
	
    if(payload == NULL)
    {
		JANUS_LOG(LOG_ERR, "Failed to stringify message...\n");
		return -1;
	}

	g_async_queue_push(admin ? admin_client.messages : client.messages, payload);

	return 0;
}


/* Thread */
void *janus_skeletontran_thread(void *data) 
{
	JANUS_LOG(LOG_INFO, "SKELETONTRAN thread started in SKELETONTRAN transport\n");

	while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping))
    {
        // TODO was originally `char *payload = (char*)g_async_queue_pop(items[i].socket == transport_sock ? client.messages : admin_client.messages);`
        // to allow both somethow 

        // this is broken
        char *payload = (char*)g_async_queue_pop(1 ? client.messages : admin_client.messages);

        if(payload == NULL || strlen(payload) <= 1)
        {
            break;
        }
    
        /**
         * @brief do something with payload

            TODO: will try pop return if none in queue? (hence the "try")
         * 
         */       
  
	}

	JANUS_LOG(LOG_INFO, "SKELETONTRAN thread in SKELETONTRAN transport ended\n");
	return NULL;
}


/* ORIGINAL HANDLER (modified from nanomsg)

void *janus_zmq_thread(void *data) 
{
	JANUS_LOG(LOG_INFO, "ZMQ thread started in ZMQ transport\n");

	int fds = 0;
	
    zmq_pollitem_t items[3];

    int64_t oldtime = 0;
    //int64_t send_state_at = 0;
    int64_t send_state_at = zclock_mono () + 1000;

	while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping))
    {

        int time_left = (int) ((send_state_at - zclock_time ()));

        if (time_left < 0)
            time_left = 0;

		fds = 0;
	
		items[fds].socket = transport_zmq_pipe[0];
        items[fds].fd = 0;
		items[fds].events = ZMQ_POLLIN;
        items[fds].revents = 0;
		
        fds++;
	
        items[fds].socket = transport_sock;
        items[fds].fd = 0;
        items[fds].events = ZMQ_POLLIN;
        items[fds].revents = 0;
        
        if(client.messages != NULL && g_async_queue_length(client.messages) > 0)
            items[fds].events |= ZMQ_POLLOUT;
        
        fds++;
	
    	if(has_admin == TRUE)
        {
			// Admin API
			items[fds].socket = transport_admin_sock;
            items[fds].fd = 0;
			items[fds].events = ZMQ_POLLIN;
            items[fds].revents = 0;
			
            if(admin_client.messages != NULL && g_async_queue_length(admin_client.messages) > 0)
				items[fds].events |= ZMQ_POLLOUT;
			
            fds++;
		}

    	int res = zmq_poll (items, fds, time_left);

        if(res == 0)
        {
            // timeout
        }

    	if(res < 0)
        {
            // interrupted
            usleep(1000);
			continue;
        }

		int i = 0;
		for(i = 0; i < fds; i++)
        {
			if(items[i].revents & ZMQ_POLLOUT)
            {
				if(items[i].socket == transport_sock || items[i].socket == transport_admin_sock)
                {
					char *payload = NULL;
					while((payload = (char*)g_async_queue_try_pop(items[i].socket == transport_sock ? client.messages : admin_client.messages)) != NULL)
                    {
                        char *testid = "testPHP";
                        if(zmq_send(items[i].socket, testid, strlen(testid), ZMQ_SNDMORE) < 0)
                            JANUS_LOG(LOG_ERR, "COULDNT SEND ID TO ROUTER %d (%s)\n", errno,  zmq_strerror (errno));
                        if(zmq_send(items[i].socket, "", strlen(""), ZMQ_SNDMORE) < 0)
                            JANUS_LOG(LOG_ERR, "COULDNT SEND \"\" TO ROUTER %d (%s)\n", errno,  zmq_strerror (errno));
						int res = zmq_send(items[i].socket, payload, strlen(payload), 0);
                        if(res < 0)
                            JANUS_LOG(LOG_ERR, "COULDNT SEND PAYLOAD TO ROUTER %d (%s)\n", errno,  zmq_strerror (errno));

    					free(payload);
					}
				}
			}
			
            if(items[i].revents & ZMQ_POLLIN)
            {
				if(items[i].socket == transport_zmq_pipe[0])
                {
				    char *from_php = s_recv(items[i].socket);
  			        free(from_php);
                    
                }else if(items[i].socket == transport_sock || items[i].socket == transport_admin_sock)
                {

                    char *identity = s_recv(items[i].socket);
                    free(s_recv(items[i].socket)); 
					char *from_php = s_recv(items[i].socket);
					
                    JANUS_LOG(LOG_VERB, "Got %s API message (%d bytes)\n", items[i].socket == transport_sock ? "Janus" : "Admin", res);

					json_error_t error;
					json_t *root = json_loads(from_php, 0, &error);
					
					gateway->incoming_request(janusTransport.get(), items[i].socket == transport_sock ? client.ts : admin_client.ts, NULL, items[i].socket == transport_sock ? FALSE : TRUE, root, &error);

                    free(identity);
                    free(from_php);
				}
			}
		}
        
        // TIMEOUT
        if (zclock_mono () >= send_state_at) {
            oldtime = send_state_at;
            send_state_at = zclock_mono () + 1000;
        }

	}

	JANUS_LOG(LOG_INFO, "ZMQ thread in ZMQ transport ended\n");
	return NULL;
}

*/