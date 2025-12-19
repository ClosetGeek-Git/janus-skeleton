#ifndef JANUS_SKELETON_EVH_COMMON_H
#define JANUS_SKELETON_EVH_COMMON_H

#include <map>
#include <thread>
#include <memory>
#include <vector>
#include <string>

extern "C" {
#include "events/eventhandler.h"
#include <math.h>
#include "debug.h"
#include "config.h"
#include "mutex.h"
#include "utils.h"
#include "refcount.h"
}

/* Plugin information */
#define JANUS_SKELETONEVH_VERSION			1
#define JANUS_SKELETONEVH_VERSION_STRING	  "0.0.1"
#define JANUS_SKELETONEVH_DESCRIPTION      "This is a trivial ZMQ event handler plugin for Janus."
#define JANUS_SKELETONEVH_NAME             "JANUS ZMQ Event handler plugin"
#define JANUS_SKELETONEVH_AUTHOR           "Jason Lester"
#define JANUS_SKELETONEVH_PACKAGE          "janus.eventhandler.helloworldevh"

/* Error codes (for the tweaking via Admin API */
#define JANUS_SKELETONEVH_ERROR_INVALID_REQUEST		411
#define JANUS_SKELETONEVH_ERROR_MISSING_ELEMENT		412
#define JANUS_SKELETONEVH_ERROR_INVALID_ELEMENT		413
#define JANUS_SKELETONEVH_ERROR_UNKNOWN_ERROR		  499

/* External function declarations */
void janus_events_edit_events_mask(const char *list, janus_flags *target);

/* Global variables */
extern std::unique_ptr<janus_eventhandler> janusEventhandler;
extern volatile gint initialized, stopping;
extern GThread *pub_thread, *handler_thread;
extern GAsyncQueue *events, *nfd_queue;
extern gboolean group_events;
extern json_t exit_event;
extern size_t json_format;
extern int nfd, nfd_addr, write_nfd[2];

/* Parameter validation (for tweaking via Admin API) */
extern struct janus_json_parameter request_parameters[];
extern struct janus_json_parameter tweak_parameters[];

/* Function declarations for lifecycle */
int janus_skeletonevh_init(const char *config_path);
void janus_skeletonevh_destroy(void);

/* Function declarations for API */
int janus_skeletonevh_get_api_compatibility(void);
int janus_skeletonevh_get_version(void);
const char *janus_skeletonevh_get_version_string(void);
const char *janus_skeletonevh_get_description(void);
const char *janus_skeletonevh_get_name(void);
const char *janus_skeletonevh_get_author(void);
const char *janus_skeletonevh_get_package(void);

/* Function declarations for handlers */
void janus_skeletonevh_incoming_event(json_t *event);
json_t *janus_skeletonevh_handle_request(json_t *request);

/* Function declarations for threads */
void *janus_skeletonevh_thread(void *data);
void *janus_skeletonevh_handler(void *data);

/* Utility functions */
void janus_skeletonevh_event_free(json_t *event);

#endif /* JANUS_SKELETON_EVH_COMMON_H */
