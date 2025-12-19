#ifndef JANUS_SKELETON_TRANSPORT_COMMON_H
#define JANUS_SKELETON_TRANSPORT_COMMON_H

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

/* Plugin information */
#define JANUS_SKELETONTRAN_VERSION			1
#define JANUS_SKELETONTRAN_VERSION_STRING	"0.0.1"
#define JANUS_SKELETONTRAN_DESCRIPTION		"This transport plugin adds SKELETONTRAN support to the Janus API."
#define JANUS_SKELETONTRAN_NAME				"JANUS SKELETONTRAN transport plugin"
#define JANUS_SKELETONTRAN_AUTHOR			"Jason Lester"
#define JANUS_SKELETONTRAN_PACKAGE			"janus.transport.skeletontran"

/* Error codes (for the tweaking and queries via Admin API) */
#define JANUS_SKELETONTRAN_ERROR_INVALID_REQUEST		411
#define JANUS_SKELETONTRAN_ERROR_MISSING_ELEMENT		412
#define JANUS_SKELETONTRAN_ERROR_INVALID_ELEMENT		413
#define JANUS_SKELETONTRAN_ERROR_UNKNOWN_ERROR		499

/* Buffer size */
#define BUFFER_SIZE		8192

using namespace std;

/* Type definitions */
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

/* Global variables */
extern std::unique_ptr<janus_transport> janusTransport;
extern gint initialized, stopping;
extern janus_transport_callbacks *gateway;
extern gboolean notify_events;
extern GHashTable *pending_transaction_hashtable;
extern janus_mutex pending_transactions_hashtable_mutex;
extern GHashTable *session_id_hashtable;
extern janus_mutex session_id_hashtable_mutex;
extern size_t json_format;
extern GThread *handler_thread;
extern janus_skeletontran_client client;
extern janus_skeletontran_client admin_client;
extern gboolean has_admin;

/* Parameter validation structures */
extern struct janus_json_parameter request_parameters[];
extern struct janus_json_parameter configure_parameters[];

/* Function declarations for lifecycle */
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

/* Function declarations for sessions */
void janus_skeletontran_session_created(janus_transport_session *transport, guint64 session_id);
void janus_skeletontran_session_over(janus_transport_session *transport, guint64 session_id, gboolean timeout, gboolean claimed);
void janus_skeletontran_session_claimed(janus_transport_session *transport, guint64 session_id);
void session_id_hashmap_destroy(session_transport *trns);
void session_id_hashmap_free(const janus_refcount *session_ref);

/* Function declarations for requests */
json_t *janus_skeletontran_query_transport(json_t *request);

/* Function declarations for communication */
int janus_skeletontran_send_message(janus_transport_session *transport, void *request_id, gboolean admin, json_t *message);
void *janus_skeletontran_thread(void *data);

#endif /* JANUS_SKELETON_TRANSPORT_COMMON_H */