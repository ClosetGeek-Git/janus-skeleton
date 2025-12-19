#ifndef JANUS_SKELETON_PLUGIN_COMMON_H
#define JANUS_SKELETON_PLUGIN_COMMON_H

#include <map>
#include <thread>
#include <memory>
#include <vector>
#include <string>

extern "C" {
#include <jansson.h>
#include <plugins/plugin.h>
#include <debug.h>
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

/* Plugin information */
#define JANUS_SKELETON_PLUGIN_VERSION             1
#define JANUS_SKELETON_PLUGIN_VERSION_STRING      "0.0.1"
#define JANUS_SKELETON_PLUGIN_DESCRIPTION         "The simplest possible Janus plugin."
#define JANUS_SKELETON_PLUGIN_NAME                "JANUS Skeleton plugin"
#define JANUS_SKELETON_PLUGIN_AUTHOR              "Jason Lester"
#define JANUS_SKELETON_PLUGIN_PACKAGE             "janus.plugin.skeleton_plugin"

/* Error codes */
#define JANUS_SKELETON_ERROR_NO_MESSAGE			411
#define JANUS_SKELETON_ERROR_INVALID_JSON		412
#define JANUS_SKELETON_ERROR_MISSING_ELEMENT	413
#define JANUS_SKELETON_ERROR_INVALID_ELEMENT	414
#define JANUS_SKELETON_ERROR_INVALID_REQUEST	415
#define JANUS_SKELETON_ERROR_ALREADY_SETUP		416
#define JANUS_SKELETON_ERROR_UNKNOWN_ERROR		499

/* SDP template: we only offer data channels */
#define sdp_template \
		"v=0\r\n" \
		"o=- %" SCNu64 " %" SCNu64 " IN IP4 127.0.0.1\r\n"	/* We need current time here */ \
		"s=Janus Skeleton plugin\r\n" \
		"t=0 0\r\n" \
		"m=application 1 UDP/DTLS/SCTP webrtc-datachannel\r\n" \
		"c=IN IP4 1.1.1.1\r\n" \
		"a=sctp-port:5000\r\n"

/* Session structure */
typedef struct janus_skeleton_session {
	janus_plugin_session *handle;
	gint64 dcid;
	gint64 channelid;
	gint64 sdp_sessid;
	gint64 sdp_version;
	janus_mutex mutex;
	volatile gint setup;
	volatile gint dataready;
	volatile gint hangingup;
	volatile gint destroyed;
	janus_refcount ref;
} janus_skeleton_session;

/* Message structure */
typedef struct janus_skeleton_message {
	janus_plugin_session *handle;
	char *transaction;
	json_t *message;
	json_t *jsep;
} janus_skeleton_message;

/* Global variables */
extern std::unique_ptr<janus_plugin> janusPlugin;
extern volatile gint initialized, stopping;
extern janus_callbacks *gateway;
extern GThread *handler_thread;
extern GHashTable *sessions;
extern janus_mutex sessions_mutex;
extern GAsyncQueue *messages_q;
extern janus_skeleton_message exit_message;
extern struct janus_json_parameter request_parameters[];

/* Utility functions */
void janus_skeleton_message_free(janus_skeleton_message *msg);
void janus_skeleton_session_destroy(janus_skeleton_session *session);
void janus_skeleton_session_free(const janus_refcount *session_ref);
janus_skeleton_session *janus_skeleton_lookup_session(janus_plugin_session *handle);

/* Lifecycle functions */
int janus_skeleton_plugin_init(janus_callbacks *callback, const char *config_path);
void janus_skeleton_plugin_destroy(void);

/* API functions */
int janus_skeleton_plugin_get_api_compatibility(void);
int janus_skeleton_plugin_get_version(void);
const char *janus_skeleton_plugin_get_version_string(void);
const char *janus_skeleton_plugin_get_description(void);
const char *janus_skeleton_plugin_get_name(void);
const char *janus_skeleton_plugin_get_author(void);
const char *janus_skeleton_plugin_get_package(void);

/* Session functions */
void janus_skeleton_plugin_create_session(janus_plugin_session *handle, int *error);
void janus_skeleton_plugin_destroy_session(janus_plugin_session *handle, int *error);
json_t *janus_skeleton_plugin_query_session(janus_plugin_session *handle);

/* Handler functions */
struct janus_plugin_result *janus_skeleton_plugin_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep);
void janus_skeleton_plugin_incoming_data(janus_plugin_session *handle, janus_plugin_data* wtf);
void *janus_skeleton_handler_thread(void *data);

/* Media functions */
void janus_skeleton_plugin_setup_media(janus_plugin_session *handle);
void janus_skeleton_plugin_incoming_rtp(janus_plugin_session *handle, janus_plugin_rtp* wtf);
void janus_skeleton_plugin_incoming_rtcp(janus_plugin_session *handle, janus_plugin_rtcp* wtf);
void janus_skeleton_plugin_slow_link(janus_plugin_session *handle, int uplink, gboolean wtf, gboolean wth);
void janus_skeleton_plugin_hangup_media(janus_plugin_session *handle);
void janus_skeleton_hangup_media_handler(janus_plugin_session *handle);

#endif /* JANUS_SKELETON_PLUGIN_COMMON_H */
