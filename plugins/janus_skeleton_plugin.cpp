#include "janus_skeleton_plugin_common.h"

using namespace std;

/* Global variables definitions */
std::unique_ptr<janus_plugin> janusPlugin;
volatile gint initialized = 0, stopping = 0;
janus_callbacks *gateway = NULL;
GThread *handler_thread = NULL;
GHashTable *sessions = NULL;
janus_mutex sessions_mutex = JANUS_MUTEX_INITIALIZER;
GAsyncQueue *messages_q = NULL;
janus_skeleton_message exit_message;

/* Parameter validation */
struct janus_json_parameter request_parameters[] = {
	{"request", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};

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
