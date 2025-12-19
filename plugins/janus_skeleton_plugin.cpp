#include <map>
#include <thread>
#include <memory>
#include <vector>
#include <string>

extern "C" {
#include <jansson.h>
#include <plugins/plugin.h>
#include <debug.h>

// from textroom
// already included #include "../debug.h"
#include "apierror.h"
#include "config.h"
#include "mutex.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
}

#define JANUS_SKELETON_PLUGIN_VERSION             1
#define JANUS_SKELETON_PLUGIN_VERSION_STRING      "0.0.1"
#define JANUS_SKELETON_PLUGIN_DESCRIPTION         "The simplest possible Janus plugin."
#define JANUS_SKELETON_PLUGIN_NAME                "JANUS Skeleton plugin"
#define JANUS_SKELETON_PLUGIN_AUTHOR              "Jason Lester"
#define JANUS_SKELETON_PLUGIN_PACKAGE             "janus.plugin.skeleton_plugin"

using namespace std;

std::unique_ptr<janus_plugin> janusPlugin; // org was 'janus_skeleton_plugin'

int janus_skeleton_plugin_init(janus_callbacks *callback, const char *config_path);
void janus_skeleton_plugin_destroy(void);
int janus_skeleton_plugin_get_api_compatibility(void);
int janus_skeleton_plugin_get_version(void);
const char *janus_skeleton_plugin_get_version_string(void);
const char *janus_skeleton_plugin_get_description(void);
const char *janus_skeleton_plugin_get_name(void);
const char *janus_skeleton_plugin_get_author(void);
const char *janus_skeleton_plugin_get_package(void);
void janus_skeleton_plugin_create_session(janus_plugin_session *handle, int *error);
struct janus_plugin_result *janus_skeleton_plugin_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep);
void janus_skeleton_plugin_setup_media(janus_plugin_session *handle);
void janus_skeleton_plugin_incoming_rtp(janus_plugin_session *handle, janus_plugin_rtp* wtf);
void janus_skeleton_plugin_incoming_rtcp(janus_plugin_session *handle, janus_plugin_rtcp* wtf);
void janus_skeleton_plugin_incoming_data(janus_plugin_session *handle, janus_plugin_data* wtf);
void janus_skeleton_plugin_slow_link(janus_plugin_session *handle, int uplink, gboolean wtf, gboolean wth);
void janus_skeleton_plugin_hangup_media(janus_plugin_session *handle);
void janus_skeleton_plugin_destroy_session(janus_plugin_session *handle, int *error);
json_t *janus_skeleton_plugin_query_session(janus_plugin_session *handle);

static volatile gint initialized = 0, stopping = 0;
static janus_callbacks *gateway = NULL;
static GThread *handler_thread;

static GThread *ipc_read_handler_thread;
static void *janus_skeleton_ipc_read_handler_thread(void *data);

int handle_config_from_php(char *identity, char *from_php);

/* SDP template: we only offer data channels */
#define sdp_template \
		"v=0\r\n" \
		"o=- %" SCNu64 " %" SCNu64 " IN IP4 127.0.0.1\r\n"	/* We need current time here */ \
		"s=Janus Skeleton plugin\r\n" \
		"t=0 0\r\n" \
		"m=application 1 UDP/DTLS/SCTP webrtc-datachannel\r\n" \
		"c=IN IP4 1.1.1.1\r\n" \
		"a=sctp-port:5000\r\n"

// mainly used in setup, restart, and ack request handlers
#define JANUS_SKELETON_ERROR_NO_MESSAGE			411
#define JANUS_SKELETON_ERROR_INVALID_JSON		412
#define JANUS_SKELETON_ERROR_MISSING_ELEMENT	413
#define JANUS_SKELETON_ERROR_INVALID_ELEMENT	414
#define JANUS_SKELETON_ERROR_INVALID_REQUEST	415
#define JANUS_SKELETON_ERROR_ALREADY_SETUP		416
#define JANUS_SKELETON_ERROR_UNKNOWN_ERROR		499

// JSON PROTOCOL STUFF

/* Parameter validation */
static struct janus_json_parameter request_parameters[] = {
	{"request", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};

static void *janus_skeleton_handler_thread(void *data);
static void janus_skeleton_hangup_media_handler(janus_plugin_session *handle);

typedef struct janus_skeleton_session {
	// ADD DCID
	janus_plugin_session *handle;
	gint64 dcid;
	gint64 channelid;
	gint64 sdp_sessid;
	gint64 sdp_version;
	janus_mutex mutex;			/* Mutex to lock this session */
	volatile gint setup;
	volatile gint dataready;
	volatile gint hangingup;
	volatile gint destroyed;
	janus_refcount ref;

} janus_skeleton_session;

static GHashTable *sessions;
static janus_mutex sessions_mutex = JANUS_MUTEX_INITIALIZER;

static void janus_skeleton_session_destroy(janus_skeleton_session *session)
{
	if(session && g_atomic_int_compare_and_exchange(&session->destroyed, 0, 1))
	{
		janus_refcount_decrease(&session->ref);
	}
}

static void janus_skeleton_session_free(const janus_refcount *session_ref)
{
	janus_skeleton_session *session = janus_refcount_containerof(session_ref, janus_skeleton_session, ref);
	g_free(session);
}

static void janus_skeleton_channel_destroy(janus_skeleton_session *session)
{
	if(session && g_atomic_int_compare_and_exchange(&session->destroyed, 0, 1))
	{
		janus_refcount_decrease(&session->ref);
	}
}

static void janus_skeleton_channel_free(const janus_refcount *session_ref)
{
	janus_skeleton_session *session = janus_refcount_containerof(session_ref, janus_skeleton_session, ref);
	g_free(session);
}

typedef struct janus_skeleton_message {
	
	janus_plugin_session *handle;
	char *transaction;
	json_t *message;
	json_t *jsep;

} janus_skeleton_message;

static GAsyncQueue *messages_q = NULL;
static janus_skeleton_message exit_message;

static void janus_skeleton_message_free(janus_skeleton_message *msg)
{
	if(!msg || msg == &exit_message)
		return;

	if(msg->handle && msg->handle->plugin_handle)
	{
		janus_skeleton_session *session = (janus_skeleton_session *)msg->handle->plugin_handle;
		janus_refcount_decrease(&session->ref);
	}
	
	msg->handle = NULL;

	g_free(msg->transaction);
	msg->transaction = NULL;
	
	if(msg->message)
		json_decref(msg->message);
	
	msg->message = NULL;
	
	if(msg->jsep)
		json_decref(msg->jsep);
	
	msg->jsep = NULL;

	g_free(msg);
}

static janus_skeleton_session *janus_skeleton_lookup_session(janus_plugin_session *handle)
{
	janus_skeleton_session *session = NULL;
	
	if (g_hash_table_contains(sessions, handle))
	{
		session = (janus_skeleton_session *)handle->plugin_handle;
	}
	
	return session;
}

extern "C" janus_plugin* create()
{
    JANUS_LOG(LOG_VERB, "%s created!\n", JANUS_SKELETON_PLUGIN_NAME);

    janusPlugin = std::make_unique<janus_plugin>(janus_plugin {
			.init = janus_skeleton_plugin_init,
			.destroy = janus_skeleton_plugin_destroy,
			.get_api_compatibility = janus_skeleton_plugin_get_api_compatibility,
			.get_version = janus_skeleton_plugin_get_version,
			.get_version_string = janus_skeleton_plugin_get_version_string,
			.get_description = janus_skeleton_plugin_get_description,
			.get_name = janus_skeleton_plugin_get_name,
			.get_author = janus_skeleton_plugin_get_author,
			.get_package = janus_skeleton_plugin_get_package,
			.create_session = janus_skeleton_plugin_create_session,
			.handle_message = janus_skeleton_plugin_handle_message,
			.setup_media = janus_skeleton_plugin_setup_media,
			.incoming_rtp = janus_skeleton_plugin_incoming_rtp,
			.incoming_rtcp = janus_skeleton_plugin_incoming_rtcp,
			.incoming_data = janus_skeleton_plugin_incoming_data,
			.slow_link = janus_skeleton_plugin_slow_link,
			.hangup_media = janus_skeleton_plugin_hangup_media, 
			.destroy_session = janus_skeleton_plugin_destroy_session,
			.query_session = janus_skeleton_plugin_query_session
        });

    return janusPlugin.get();
}

// janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init
// janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init
// janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init
// janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init
// janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init
// janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init
// janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init
// janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init


static unsigned int table[256];
void setup_crc32_table()
{
	int j;
	unsigned int byte, crc, mask;

	/* Set up the table, if necessary. */

	if (table[1] == 0)
	{
		for (byte = 0; byte <= 255; byte++)
		{
			crc = byte;
			for (j = 7; j >= 0; j--) 
			{
				mask = -(crc & 1);
				crc = (crc >> 1) ^ (0xEDB88320 & mask);
			}
			table[byte] = crc;
		}
	}
}

unsigned int crc32(char *message)
{
	unsigned int crc, word;

	/* Through with table setup, now calculate the CRC. */

	crc = 0xFFFFFFFF;
	while (((word = *(unsigned int *)message) & 0xFF) != 0)
	{
		crc = crc ^ word;
		crc = (crc >> 8) ^ table[crc & 0xFF];
		crc = (crc >> 8) ^ table[crc & 0xFF];
		crc = (crc >> 8) ^ table[crc & 0xFF];
		crc = (crc >> 8) ^ table[crc & 0xFF];
		message = message + 4;
	}
	return ~crc;
}

unsigned int ucrc32(unsigned char *message)
{
	unsigned int crc, word;

	/* Through with table setup, now calculate the CRC. */

	crc = 0xFFFFFFFF;
	while (((word = *(unsigned int *)message) & 0xFF) != 0) {
		crc = crc ^ word;
		crc = (crc >> 8) ^ table[crc & 0xFF];
		crc = (crc >> 8) ^ table[crc & 0xFF];
		crc = (crc >> 8) ^ table[crc & 0xFF];
		crc = (crc >> 8) ^ table[crc & 0xFF];
		message = message + 4;
	}
	return ~crc;
}

// janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init 
// janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init 
// janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init 
// janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init 
// janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init // janus_skeleton_plugin_init 

int janus_skeleton_plugin_init(janus_callbacks *callback, const char *config_path)
{
	char *initvars = getenv("INITVARS");

	if(initvars == NULL)
	{
		JANUS_LOG(LOG_ERR, "Must set environment variable INITVARS (INITVARS == NULL)\n");
		return -1;
	}

	json_error_t json_error;
	json_t *initvars_root = json_loads(initvars, 0, &json_error);

	if(!initvars_root)
	{
		JANUS_LOG(LOG_ERR, "Error parsing INITVARS environmental variable (JSON error: on line %d: %s) in: (%s)\n", json_error.line, json_error.text, initvars);
		return -1;
	}

	// get address
	json_t *id_el = json_object_get(initvars_root, "id");
	if(id_el == NULL)
	{
		JANUS_LOG(LOG_ERR, "Error 'INITVARS' did not provide 'id'\n");
	}

	const char *id_text = json_string_value(id_el);

	if(id_text == NULL || strlen(id_text) > 255)
	{
		JANUS_LOG(LOG_ERR, "Error 'INITVARS' 'id' not properly set, must be string 255 chars or less\n");
	}

	
	if(g_atomic_int_get(&stopping))
	{
		/* Still stopping from before */
		return -1;
	}

	if(callback == NULL || config_path == NULL)
	{
		/* Invalid arguments */
		return -1;
	}

	sessions = g_hash_table_new_full(NULL, NULL, NULL, (GDestroyNotify)janus_skeleton_session_destroy);
	messages_q = g_async_queue_new_full((GDestroyNotify) janus_skeleton_message_free);

	/* This is the callback we'll need to invoke to contact the Janus core */
	gateway = callback;

	g_atomic_int_set(&initialized, 1);

	GError *error = NULL;

	/* Launch the thread that will handle incoming messages */
	handler_thread = g_thread_try_new("skeleton handler", janus_skeleton_handler_thread, NULL, &error);

	if(error != NULL)
	{
		g_atomic_int_set(&initialized, 0);
		JANUS_LOG(LOG_ERR, "Got error %d (%s) trying to launch the skeleton handler thread...\n", error->code, error->message ? error->message : "??");

		g_error_free(error);

		return -1;
	}

	/**
	 * @brief DO STUFF HERE
	 * 
	 */

	if(initvars_root != NULL)
		json_decref(initvars_root);

	JANUS_LOG(LOG_INFO, "%s initialized!\n", JANUS_SKELETON_PLUGIN_NAME);

	return 0;
}

// janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy
// janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy
// janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy
// janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy
// janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy // janus_skeleton_plugin_destroy

void janus_skeleton_plugin_destroy(void)
{
	
	if(!g_atomic_int_get(&initialized))
		return;

	
	g_atomic_int_set(&stopping, 1);
	g_async_queue_push(messages_q, &exit_message);

	if(handler_thread != NULL)
	{
		g_thread_join(handler_thread);
		handler_thread = NULL;
	}

	janus_mutex_lock(&sessions_mutex);
	
		g_hash_table_destroy(sessions);
		sessions = NULL;

	janus_mutex_unlock(&sessions_mutex);

	/**
	 * @todo IS THIS REALLY DONE? WHAT TO DO ABOUT SESSION MUTEX ETC
	 * 
	 */


	g_async_queue_unref(messages_q);
	
	messages_q = NULL;
	
	g_atomic_int_set(&initialized, 0);
	g_atomic_int_set(&stopping, 0);

	JANUS_LOG(LOG_INFO, "%s destroyed!\n", JANUS_SKELETON_PLUGIN_NAME);

}

int janus_skeleton_plugin_get_api_compatibility(void) {
	return JANUS_PLUGIN_API_VERSION;
}

int janus_skeleton_plugin_get_version(void) {
	return JANUS_SKELETON_PLUGIN_VERSION;
}

const char *janus_skeleton_plugin_get_version_string(void) {
	return JANUS_SKELETON_PLUGIN_VERSION_STRING;
}

const char *janus_skeleton_plugin_get_description(void) {
	return JANUS_SKELETON_PLUGIN_DESCRIPTION;
}

const char *janus_skeleton_plugin_get_name(void) {
	return JANUS_SKELETON_PLUGIN_NAME;
}

const char *janus_skeleton_plugin_get_author(void) {
	return JANUS_SKELETON_PLUGIN_AUTHOR;
}

const char *janus_skeleton_plugin_get_package(void) {
	return JANUS_SKELETON_PLUGIN_PACKAGE;
}


// session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers 
// session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers 
// session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers 
// session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers 
// session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers 
// session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers 
// session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers 
// session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers // session handlers 


void janus_skeleton_plugin_create_session(janus_plugin_session *handle, int *error)
{
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
	{
		*error = -1;
		return;
	}
	
	janus_skeleton_session *session = (janus_skeleton_session *)g_malloc0(sizeof(janus_skeleton_session));
	session->handle = handle;
	session->destroyed = 0;

	// CRATE RANDOM DCID HERE AND ADD TO session->dcid
	janus_mutex_init(&session->mutex);
	janus_refcount_init(&session->ref, janus_skeleton_session_free);

	g_atomic_int_set(&session->setup, 0);
	g_atomic_int_set(&session->dataready, 0);
	g_atomic_int_set(&session->hangingup, 0);
	
	janus_mutex_lock(&sessions_mutex);
		g_hash_table_insert(sessions, handle, session);
	janus_mutex_unlock(&sessions_mutex);


	JANUS_LOG(LOG_INFO, "SkeletonSession created.\n");

	return;
}

void janus_skeleton_plugin_destroy_session(janus_plugin_session *handle, int *error)
{
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
	{
		*error = -1;
		return;
	}
	
	janus_mutex_lock(&sessions_mutex);
		janus_skeleton_session *session = janus_skeleton_lookup_session(handle);
		
		if(!session)
		{
			janus_mutex_unlock(&sessions_mutex);
			JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		
			*error = -2;
			return;
		}
		
		JANUS_LOG(LOG_VERB, "Removing Skeleton session...\n");

		janus_skeleton_hangup_media_handler(handle);
		
		g_hash_table_remove(sessions, handle);
	janus_mutex_unlock(&sessions_mutex);

	return;
}

json_t *janus_skeleton_plugin_query_session(janus_plugin_session *handle)
{
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
	{
		return NULL;
	}
	
	janus_mutex_lock(&sessions_mutex);
	janus_skeleton_session *session = janus_skeleton_lookup_session(handle);
	
	if(!session)
	{
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No Skeleton session associated with this handle...\n");
	
		return NULL;
	}
	
	janus_refcount_increase(&session->ref);
	janus_mutex_unlock(&sessions_mutex);
	
	json_t *info = json_object();
	json_object_set_new(info, "destroyed", json_integer(session->destroyed));
	janus_refcount_decrease(&session->ref);
	
	return info;
}


// janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message
// janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message
// janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message
// janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message
// janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message
// janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message
// janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message
// janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message // janus_skeleton_plugin_handle_message


struct janus_plugin_result *janus_skeleton_plugin_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep)
{
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return janus_plugin_result_new(JANUS_PLUGIN_ERROR, g_atomic_int_get(&stopping) ? "Shutting down" : "Plugin not initialized", NULL);

	/* Pre-parse the message */
	int error_code = 0;
	char error_cause[512];

	// This will become the root as soon as parsed
	json_t *root = message;
	json_t *response = NULL;

	const char *request_text = NULL;
	json_t *request = NULL;

	janus_mutex_lock(&sessions_mutex);
	janus_skeleton_session *session = janus_skeleton_lookup_session(handle);

	if(!session)
	{
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");

		error_code = JANUS_SKELETON_ERROR_UNKNOWN_ERROR;
		g_snprintf(error_cause, 512, "%s", "No session associated with this handle...");
		
		goto plugin_response;
	}

	/* Increase the reference counter for this session: we'll decrease it after we handle the message */
	janus_refcount_increase(&session->ref);
	janus_mutex_unlock(&sessions_mutex);
	
	if(g_atomic_int_get(&session->destroyed))
	{
		JANUS_LOG(LOG_ERR, "Session has already been destroyed...\n");
	
		error_code = JANUS_SKELETON_ERROR_UNKNOWN_ERROR;
		g_snprintf(error_cause, 512, "%s", "Session has already been destroyed...");
	
		goto plugin_response;
	}

	if(message == NULL)
	{
		JANUS_LOG(LOG_ERR, "No message??\n");
		error_code = JANUS_SKELETON_ERROR_NO_MESSAGE;
	
		g_snprintf(error_cause, 512, "%s", "No message??");
	
		goto plugin_response;
	}
	
	if(!json_is_object(root))
	{
		JANUS_LOG(LOG_ERR, "JSON error: not an object\n");
	
		error_code = JANUS_SKELETON_ERROR_INVALID_JSON;
		g_snprintf(error_cause, 512, "JSON error: not an object");
	
		goto plugin_response;
	}
	
	/* Get the request first */
	JANUS_VALIDATE_JSON_OBJECT(root, request_parameters, error_code, error_cause, TRUE, JANUS_SKELETON_ERROR_MISSING_ELEMENT, JANUS_SKELETON_ERROR_INVALID_ELEMENT);
	if(error_code != 0)
		goto plugin_response;

	request = json_object_get(root, "request");

	/* Some requests (e.g., 'create' and 'destroy') can be handled synchronously */
	request_text = (const char *)json_string_value(request);

	if(!strcasecmp(request_text, "setup") || !strcasecmp(request_text, "ack") || !strcasecmp(request_text, "restart"))
	{
		/* These messages are handled asynchronously */
		janus_skeleton_message *msg = (janus_skeleton_message *)g_malloc(sizeof(janus_skeleton_message));
		msg->handle = handle;
		msg->transaction = transaction;
		msg->message = root;
		msg->jsep = jsep;

		g_async_queue_push(messages_q, msg);

		return janus_plugin_result_new(JANUS_PLUGIN_OK_WAIT, NULL, NULL);
	
	}else
	{
		JANUS_LOG(LOG_VERB, "Unknown request '%s'\n", request_text);
		
		error_code = JANUS_SKELETON_ERROR_INVALID_REQUEST;
		g_snprintf(error_cause, 512, "Unknown request '%s'", request_text);
	}

plugin_response:
		
		{
			if(!response)
			{
				/* Prepare JSON error event */
				response = json_object();
				json_object_set_new(response, "textroom", json_string("event"));
				json_object_set_new(response, "error_code", json_integer(error_code));
				json_object_set_new(response, "error", json_string(error_cause));
			}
			
			if(root != NULL)
				json_decref(root);
			
			if(jsep != NULL)
				json_decref(jsep);
			
			g_free(transaction);

			if(session != NULL)
				janus_refcount_decrease(&session->ref);
			
			return janus_plugin_result_new(JANUS_PLUGIN_OK, NULL, response);
		}
}


// janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data
// janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data
// janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data
// janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data
// janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data
// janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data
// janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data
// janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data // janus_skeleton_plugin_incoming_data


void janus_skeleton_plugin_incoming_data(janus_plugin_session *handle, janus_plugin_data *packet)
{
	if(handle == NULL || handle->stopped || g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;

	if(packet->binary)
	{
		/* We don't support binary data in the TextRoom plugin, it has to be text */
		JANUS_LOG(LOG_ERR, "Binary data received, dropping...\n");
		
		return;
	}
	
	/* Incoming request from this user: what should we do? */
	janus_skeleton_session *session = (janus_skeleton_session *)handle->plugin_handle;
	
	/*

		json_t *info = json_object();
		json_object_set_new(info, "event", json_string("test"));
		json_object_set_new(info, "room", json_integer(100));
		json_object_set_new(info, "dcid", json_integer(session->dcid));

		gateway->notify_event(janusPlugin.get(),  session ? session->handle : NULL, info);
	*/

	if(!session)
	{
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		return;
	}
	
	janus_refcount_increase(&session->ref);
	
	if(session->destroyed)
	{
		janus_refcount_decrease(&session->ref);
	
		return;
	}
	
	char *buf = packet->buffer;
	uint16_t len = packet->length;
	
	if(buf == NULL || len <= 0)
	{
		janus_refcount_decrease(&session->ref);
		return;
	}

	/**
	 * @brief DO STUFF WITH buf / packet->buffer;
	 * 
	 */

	janus_refcount_decrease(&session->ref);
}

void janus_skeleton_plugin_slow_link(janus_plugin_session *handle, int uplink, gboolean wtf, gboolean wth)
{
	JANUS_LOG(LOG_VERB, "Slow link detected.\n");
}

void janus_skeleton_plugin_hangup_media(janus_plugin_session *handle)
{
	janus_mutex_lock(&sessions_mutex);
	janus_skeleton_hangup_media_handler(handle);
	janus_mutex_unlock(&sessions_mutex);
}


// janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media 
// janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media 
// janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media 
// janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media 
// janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media 
// janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media 
// janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media 
// janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media // janus_skeleton_hangup_media_handler // janus_skeleton_plugin_setup_media 


void janus_skeleton_plugin_setup_media(janus_plugin_session *handle)
{
	JANUS_LOG(LOG_INFO, "[%s-%p] WebRTC media is now available\n", JANUS_SKELETON_PLUGIN_PACKAGE, handle);
	
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	
	janus_mutex_lock(&sessions_mutex);
	
	janus_skeleton_session *session = janus_skeleton_lookup_session(handle);
	
	if(!session)
	{
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
	
		return;
	}
	
	if(session->destroyed)
	{
		janus_mutex_unlock(&sessions_mutex);
		return;
	}
	
	g_atomic_int_set(&session->hangingup, 0);
	janus_mutex_unlock(&sessions_mutex);
}

void janus_skeleton_plugin_incoming_rtp(janus_plugin_session *handle, janus_plugin_rtp* wtf)
{
	JANUS_LOG(LOG_VERB, "Got an RTP message)\n");
}

void janus_skeleton_plugin_incoming_rtcp(janus_plugin_session *handle, janus_plugin_rtcp* wtf)
{
	JANUS_LOG(LOG_VERB, "Got an RTCP message\n");
}

static void janus_skeleton_hangup_media_handler(janus_plugin_session *handle)
{
	JANUS_LOG(LOG_INFO, "[%s-%p] No WebRTC media anymore\n", JANUS_SKELETON_PLUGIN_PACKAGE, handle);
	
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	
	// GET SESSION from handle.
	janus_skeleton_session *session = janus_skeleton_lookup_session(handle);
	
	if(!session)
	{
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		
		return;
	}
	
	if(session->destroyed)
		return;
	
	if(!g_atomic_int_compare_and_exchange(&session->hangingup, 0, 1))
		return;
	
	g_atomic_int_set(&session->dataready, 0);
	g_atomic_int_set(&session->hangingup, 0);
}


// janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread 
// janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread 
// janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread 
// janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread 
// janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread 
// janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread 
// janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread 
// janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread // janus_skeleton_handler_thread 


// 90% of this is used for created new datachannels and maintaining SDP
static void *janus_skeleton_handler_thread(void *data)
{
	JANUS_LOG(LOG_VERB, "Joining skeleton handler thread\n");
	janus_skeleton_message *msg = NULL;
	
	int error_code = 0;
	char error_cause[512];
	
	json_t *root = NULL;
	gboolean do_offer = FALSE, sdp_update = FALSE;
	
	while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping))
	{
		json_t *event = NULL;
		json_t *request = NULL;
		const char *request_text = NULL;

		msg = (janus_skeleton_message*)g_async_queue_pop(messages_q);

		if(msg == &exit_message)
			break;

		if(msg->handle == NULL)
		{
			janus_skeleton_message_free(msg);
			continue;
		}
		
		janus_mutex_lock(&sessions_mutex);
		janus_skeleton_session *session = janus_skeleton_lookup_session(msg->handle);
		
		if(!session)
		{
			janus_mutex_unlock(&sessions_mutex);
			
			JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
			
			janus_skeleton_message_free(msg);
			continue;
		}
		
		if(g_atomic_int_get(&session->destroyed))
		{
			janus_mutex_unlock(&sessions_mutex);
			janus_skeleton_message_free(msg);
			
			continue;
		}
		
		janus_mutex_unlock(&sessions_mutex);
		
		/* Handle request */
		error_code = 0;
		root = msg->message;
		
		if(msg->message == NULL)
		{
			JANUS_LOG(LOG_ERR, "No message??\n");
			
			error_code = JANUS_SKELETON_ERROR_NO_MESSAGE;
			g_snprintf(error_cause, 512, "%s", "No message??");
			
			goto error;
		}
		
		if(!json_is_object(root))
		{
			JANUS_LOG(LOG_ERR, "JSON error: not an object\n");
			error_code = JANUS_SKELETON_ERROR_INVALID_JSON;
			
			g_snprintf(error_cause, 512, "JSON error: not an object");
			
			goto error;
		}
		
		/* Parse request */
		JANUS_VALIDATE_JSON_OBJECT(root, request_parameters, error_code, error_cause, TRUE, JANUS_SKELETON_ERROR_MISSING_ELEMENT, JANUS_SKELETON_ERROR_INVALID_ELEMENT);
		if(error_code != 0)
			goto error;
		
		do_offer = FALSE;
		sdp_update = FALSE;
		
		request = json_object_get(root, "request");
		request_text = json_string_value(request);
		
		do_offer = FALSE;
		
		if(!strcasecmp(request_text, "setup"))
		{
			if(!g_atomic_int_compare_and_exchange(&session->setup, 0, 1))
			{
				JANUS_LOG(LOG_ERR, "PeerConnection already setup\n");
				
				error_code = JANUS_SKELETON_ERROR_ALREADY_SETUP;
				g_snprintf(error_cause, 512, "PeerConnection already setup");
				
				goto error;
			}
			
			do_offer = TRUE;

		}else if(!strcasecmp(request_text, "restart"))
		{
			if(!g_atomic_int_get(&session->setup))
			{
				JANUS_LOG(LOG_ERR, "PeerConnection not setup\n");
				
				error_code = JANUS_SKELETON_ERROR_ALREADY_SETUP;
				g_snprintf(error_cause, 512, "PeerConnection not setup");
				
				goto error;
			}

			sdp_update = TRUE;
			do_offer = TRUE;
		
		}else if(!strcasecmp(request_text, "ack"))
		{
			/* The peer sent their answer back: do nothing */
		
		}else
		{
			JANUS_LOG(LOG_VERB, "Unknown request '%s'\n", request_text);
			
			error_code = JANUS_SKELETON_ERROR_INVALID_REQUEST;
			g_snprintf(error_cause, 512, "Unknown request '%s'", request_text);
			
			goto error;
		}

		/* Prepare JSON event */
		event = json_object();
		json_object_set_new(event, "textroom", json_string("event"));
		json_object_set_new(event, "result", json_string("ok"));

		// THIS IS IT. ADD DCID HERE FROM SESSION
		//json_object_set_new(event, "dcid", json_integer(session->dcid));
		
		if(!do_offer)
		{
			int ret = gateway->push_event(msg->handle, janusPlugin.get(), msg->transaction, event, NULL);
			JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (%s)\n", ret, janus_get_api_error(ret));
		
		}else
		{
			/* Send an offer (whether it's for an ICE restart or not) */
			if(sdp_update)
			{
				/* Renegotiation: increase version */
				session->sdp_version++;
			
			}else
			{
				/* New session: generate new values */
				session->sdp_version = 1;	/* This needs to be increased when it changes */
				session->sdp_sessid = janus_get_real_time();
			}
			
			char sdp[500];
			g_snprintf(sdp, sizeof(sdp), sdp_template, session->sdp_sessid, session->sdp_version);
			
			json_t *jsep = json_pack("{ssss}", "type", "offer", "sdp", sdp);
			
			if(sdp_update)
				json_object_set_new(jsep, "restart", json_true());
			
			/* How long will the Janus core take to push the event? */
			g_atomic_int_set(&session->hangingup, 0);
			gint64 start = janus_get_monotonic_time();
			
			int res = gateway->push_event(msg->handle, janusPlugin.get(), msg->transaction, event, jsep);
			
			JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (took %" SCNu64 " us)\n", res, janus_get_monotonic_time()-start);
			json_decref(jsep);
		}

		json_decref(event);
		janus_skeleton_message_free(msg);
		
		continue;

error:
		{
			/* Prepare JSON error event */
			json_t *event = json_object();
			json_object_set_new(event, "textroom", json_string("error"));
			json_object_set_new(event, "error_code", json_integer(error_code));
			json_object_set_new(event, "error", json_string(error_cause));

			int ret = gateway->push_event(msg->handle, janusPlugin.get(), msg->transaction, event, NULL);

			JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (%s)\n", ret, janus_get_api_error(ret));

			json_decref(event);
			janus_skeleton_message_free(msg);
		}
	}
		
	return NULL;
}
