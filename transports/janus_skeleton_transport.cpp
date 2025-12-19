#include "janus_skeleton_transport_common.h"

using namespace std;

/* Global variables definitions */
std::unique_ptr<janus_transport> janusTransport;
gint initialized = 0, stopping = 0;
janus_transport_callbacks *gateway = NULL;
gboolean notify_events = TRUE;
GHashTable *pending_transaction_hashtable = NULL;
janus_mutex pending_transactions_hashtable_mutex = JANUS_MUTEX_INITIALIZER;
GHashTable *session_id_hashtable = NULL;
janus_mutex session_id_hashtable_mutex = JANUS_MUTEX_INITIALIZER;
size_t json_format = JSON_INDENT(3) | JSON_PRESERVE_ORDER;
GThread *handler_thread = NULL;
janus_skeletontran_client client;
janus_skeletontran_client admin_client;
gboolean has_admin;

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
