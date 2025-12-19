#include "janus_skeleton_evh_common.h"

using namespace std;

/* Global variables definitions */
std::unique_ptr<janus_eventhandler> janusEventhandler;
volatile gint initialized = 0, stopping = 0;
GThread *pub_thread = NULL, *handler_thread = NULL;
GAsyncQueue *events = NULL, *nfd_queue = NULL;
gboolean group_events = TRUE;
json_t exit_event;
size_t json_format = JSON_INDENT(3) | JSON_PRESERVE_ORDER;
int nfd = -1, nfd_addr = -1, write_nfd[2];

/* Parameter validation (for tweaking via Admin API) */
struct janus_json_parameter request_parameters[] = {
    {"request", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};
struct janus_json_parameter tweak_parameters[] = {
    {"events", JSON_STRING, 0},
    {"grouping", JANUS_JSON_BOOL, 0}
};

extern "C" janus_eventhandler* create()
{
    JANUS_LOG(LOG_VERB, "%s created!\n", JANUS_SKELETONEVH_NAME);

    janusEventhandler = std::make_unique<janus_eventhandler>(janus_eventhandler {
		.init = janus_skeletonevh_init,
		.destroy = janus_skeletonevh_destroy,

		.get_api_compatibility = janus_skeletonevh_get_api_compatibility,
		.get_version = janus_skeletonevh_get_version,
		.get_version_string = janus_skeletonevh_get_version_string,
		.get_description = janus_skeletonevh_get_description,
		.get_name = janus_skeletonevh_get_name,
		.get_author = janus_skeletonevh_get_author,
		.get_package = janus_skeletonevh_get_package,

		.incoming_event = janus_skeletonevh_incoming_event,
		.handle_request = janus_skeletonevh_handle_request,

		.events_mask = JANUS_EVENT_TYPE_NONE
        });

    return janusEventhandler.get();
}

void janus_events_edit_events_mask(const char *list, janus_flags *target) {
    /* Stub implementation for events mask editing */
}
