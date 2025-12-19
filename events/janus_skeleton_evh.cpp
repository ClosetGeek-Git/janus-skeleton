/**
 * ERROR/BUG ALERT: THIS IS PROBABLY *NOT* IN A GOOD STATE
 
   1: HAD A COSS INITIALIZATION ERROR AT 

    const char *request_text = NULL;
    int error_code = 0;
    char error_cause[512];
    
    JANUS_VALIDATE_JSON_OBJECT(request, request_parameters, error_code, error_cause, TRUE, JANUS_SKELETONEVH_ERROR_MISSING_ELEMENT, JANUS_SKELETONEVH_ERROR_INVALID_ELEMENT);
    
    if(error_code != 0)
    {
        goto plugin_response;
    }

   THIS NEEDS TO BE LOOKED OVER BETTER. 
   
   2: HAD AN ERROR FINDING 'janus_events_edit_events_mask', ERROR WAS IT WAS NOT DECLARED AT SCOPE. ADDED IT'S SIGNATURE BUT THIS IS HACKISH. WHY DID IT HAPPEN TO BEGIN WITH

   3: HAD ISSUE RESOLVING 'janusEventhandler.get().events_mask', HAD TO USE '&janusEventhandler.get()->events_mask'

   NOTE:
        #1 CAN BE FIXED USING A DOWHILE 
        THE HELLO WORLD EVENT HANDLER DOESNT USE 2 OR 3. IT INSTEAD SETS .events_mask = JANUS_EVENT_TYPE_ALL. THIS WILL LEAD ALL EVENTS GOING INTO THE EVENT MANAGER

   THESE ARE ALL THINGS TO CONSIDER.

 */

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

void janus_events_edit_events_mask(const char *list, janus_flags *target);

/* Plugin information */
#define JANUS_SKELETONEVH_VERSION			1
#define JANUS_SKELETONEVH_VERSION_STRING	  "0.0.1"
#define JANUS_SKELETONEVH_DESCRIPTION      "This is a trivial ZMQ event handler plugin for Janus."
#define JANUS_SKELETONEVH_NAME             "JANUS ZMQ Event handler plugin"
#define JANUS_SKELETONEVH_AUTHOR           "Jason Lester"
#define JANUS_SKELETONEVH_PACKAGE          "janus.eventhandler.helloworldevh"

using namespace std;

std::unique_ptr<janus_eventhandler> janusEventhandler;

int janus_skeletonevh_init(const char *config_path);
void janus_skeletonevh_destroy(void);
int janus_skeletonevh_get_api_compatibility(void);
int janus_skeletonevh_get_version(void);
const char *janus_skeletonevh_get_version_string(void);
const char *janus_skeletonevh_get_description(void);
const char *janus_skeletonevh_get_name(void);
const char *janus_skeletonevh_get_author(void);
const char *janus_skeletonevh_get_package(void);
void janus_skeletonevh_incoming_event(json_t *event);
json_t *janus_skeletonevh_handle_request(json_t *request);

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

/* Useful stuff */
static volatile gint initialized = 0, stopping = 0;
static GThread *pub_thread, *handler_thread;
static void *janus_skeletonevh_thread(void *data);
static void *janus_skeletonevh_handler(void *data);

/* Queue of events to handle */
static GAsyncQueue *events = NULL, *nfd_queue = NULL;
static gboolean group_events = TRUE;
static json_t exit_event;

static void janus_skeletonevh_event_free(json_t *event)
{
    if(!event || event == &exit_event)
      return;

    json_decref(event);
}

/* JSON serialization options */
static size_t json_format = JSON_INDENT(3) | JSON_PRESERVE_ORDER;

/* Nanomsg stuff */
static int nfd = -1, nfd_addr = -1, write_nfd[2];


/* Parameter validation (for tweaking via Admin API) */
static struct janus_json_parameter request_parameters[] = {
    {"request", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};
static struct janus_json_parameter tweak_parameters[] = {
    {"events", JSON_STRING, 0},
    {"grouping", JANUS_JSON_BOOL, 0}
};
/* Error codes (for the tweaking via Admin API */
#define JANUS_SKELETONEVH_ERROR_INVALID_REQUEST		411
#define JANUS_SKELETONEVH_ERROR_MISSING_ELEMENT		412
#define JANUS_SKELETONEVH_ERROR_INVALID_ELEMENT		413
#define JANUS_SKELETONEVH_ERROR_UNKNOWN_ERROR		  499


// EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT 
// EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT 
// EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT 
// EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT 
// EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT 
// EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT 
// EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT 
// EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT // EVENT INIT 


/* Plugin implementation */
int janus_skeletonevh_init(const char *config_path)
{
    gboolean success = TRUE;
    
    if(g_atomic_int_get(&stopping))
    {
        /* Still stopping from before */
        return -1;
    }
    
    /* Initialize the events queue */
    events = g_async_queue_new_full((GDestroyNotify) janus_skeletonevh_event_free);
    nfd_queue = g_async_queue_new_full((GDestroyNotify) g_free);
    
    g_atomic_int_set(&initialized, 1);

    /* Start the Nanomsg and event handler threads */
    GError *thread_error = NULL;
    pub_thread = g_thread_try_new("janus skeletonevh thread", janus_skeletonevh_thread, NULL, &thread_error);
    
    if(thread_error != NULL)
    {
        g_atomic_int_set(&initialized, 0);
        JANUS_LOG(LOG_FATAL, "Got error %d (%s) trying to launch the NanomsgEventHandler loop thread...\n", thread_error->code, thread_error->message ? thread_error->message : "??");

        g_error_free(thread_error);
        goto error;
    }

    thread_error = NULL;
    handler_thread = g_thread_try_new("janus skeletonevh handler", janus_skeletonevh_handler, NULL, &thread_error);
    
    if(thread_error != NULL)
    {
        g_atomic_int_set(&initialized, 0);
        JANUS_LOG(LOG_FATAL, "Got error %d (%s) trying to launch the NanomsgEventHandler handler thread...\n", thread_error->code, thread_error->message ? thread_error->message : "??");

        g_error_free(thread_error);
        goto error;
    }

    /* Done */
    JANUS_LOG(LOG_INFO, "Setup of ZMQ event handler completed\n");
    goto done;

error:

    if(pub_thread != NULL)
    {   g_async_queue_push(nfd_queue, (char*)"");
        
        g_thread_join(pub_thread);
        pub_thread = NULL;
    }

    /* If we got here, something went wrong */
    success = FALSE;
    
    /**
     * @todo  NOT SURE IF pub_thread NEEDS TO BE KILLED HERE. THE QUESTION IS WHETHER 'janus_skeletonevh_destroy' IS CALLED IF 'janus_skeletonevh_init' RETURNS -1. IF
              SO THAT LEAVES pub_thread BEING CALLED TWICE WHEN IT WAS RIGHT THE FIRST TIME (althogh pup_thread wasing being set the first time. it was creating 2
              new threads each using 'handler_thread' )
     * 
     */

/* Fall through */
done:
    
    if(!success)
      return -1;
    
    JANUS_LOG(LOG_INFO, "%s initialized!\n", JANUS_SKELETONEVH_NAME);

    return 0;
}



// EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY 
// EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY 
// EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY 
// EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY 
// EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY 
// EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY 
// EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY 
// EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY // EVENT DESTROY 



void janus_skeletonevh_destroy(void)
{
    if(!g_atomic_int_get(&initialized))
        return;

    g_atomic_int_set(&stopping, 1);

    g_async_queue_push(events, &exit_event);

    g_async_queue_push(nfd_queue, (char*)"");

    // pup_thread needs to be fixed at init
    // note that these will not 
    if(pub_thread != NULL)
    {
        g_thread_join(pub_thread);
        pub_thread = NULL;
    }

    if(handler_thread != NULL)
    {
        g_thread_join(handler_thread);
        handler_thread = NULL;
    }

    g_async_queue_unref(events);
    events = NULL;

    g_async_queue_unref(nfd_queue);
    nfd_queue = NULL;

    g_atomic_int_set(&initialized, 0);
    g_atomic_int_set(&stopping, 0);

    JANUS_LOG(LOG_INFO, "%s destroyed!\n", JANUS_SKELETONEVH_NAME);
}

int janus_skeletonevh_get_api_compatibility(void) {
	/* Important! This is what your plugin MUST always return: don't lie here or bad things will happen */
	return JANUS_EVENTHANDLER_API_VERSION;
}

int janus_skeletonevh_get_version(void) {
	return JANUS_SKELETONEVH_VERSION;
}

const char *janus_skeletonevh_get_version_string(void) {
	return JANUS_SKELETONEVH_VERSION_STRING;
}

const char *janus_skeletonevh_get_description(void) {
	return JANUS_SKELETONEVH_DESCRIPTION;
}

const char *janus_skeletonevh_get_name(void) {
	return JANUS_SKELETONEVH_NAME;
}

const char *janus_skeletonevh_get_author(void) {
	return JANUS_SKELETONEVH_AUTHOR;
}

const char *janus_skeletonevh_get_package(void) {
	return JANUS_SKELETONEVH_PACKAGE;
}



// INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) 
// INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) 
// INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) 
// INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) 
// INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) 
// INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) 
// INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) 
// INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) // INCOMING EVENTS (HANDLED IN EVENT THREAD) 



void janus_skeletonevh_incoming_event(json_t *event)
{
    if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
    {
        /* Janus is closing or the plugin is */
        return;
    }

    /* Do NOT handle the event here in this callback! Since Janus notifies you right
    * away when something happens, these events are triggered from working threads and
    * not some sort of message bus. As such, performing I/O or network operations in
    * here could dangerously slow Janus down. Let's just reference and enqueue the event,
    * and handle it in our own thread: the event contains a monotonic time indicator of
    * when the event actually happened on this machine, so that, if relevant, we can compute
    * any delay in the actual event processing ourselves. */
    
    json_incref(event);
    g_async_queue_push(events, event);
}



// handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request 
// handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request 
// handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request 
// handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request 
// handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request 
// handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request 
// handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request 
// handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request 
// handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request // handle request 



json_t *janus_skeletonevh_handle_request(json_t *request)
{
    /**
     * @brief THIS SEEMS TO BE A BAREBONES HANDLER. IT IS CAPABLE IF ACCEPTING A REQUEST BUT DOESNT DO ANYTHING USEFUL WITH IT YET
     * 
     */

    JANUS_LOG(LOG_INFO, "Trying handle request ----------------------------------\n");

    if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
    {
      return NULL;
    }
    
    /* We can use this requests to apply tweaks to the logic */
    const char *request_text = NULL;
    int error_code = 0;
    char error_cause[512];
    
    JANUS_VALIDATE_JSON_OBJECT(request, request_parameters, error_code, error_cause, TRUE, JANUS_SKELETONEVH_ERROR_MISSING_ELEMENT, JANUS_SKELETONEVH_ERROR_INVALID_ELEMENT);
    
    if(error_code != 0)
    {
        goto plugin_response;
    }
    
    /* Get the request */
    request_text = json_string_value(json_object_get(request, "request"));
    if(!strcasecmp(request_text, "tweak"))
    {
        /* We only support a request to tweak the current settings */
        JANUS_VALIDATE_JSON_OBJECT(request, tweak_parameters, error_code, error_cause, TRUE, JANUS_SKELETONEVH_ERROR_MISSING_ELEMENT, JANUS_SKELETONEVH_ERROR_INVALID_ELEMENT);
        
        if(error_code != 0)
            goto plugin_response;
        
        /* Events */
        if(json_object_get(request, "events"))
            janus_events_edit_events_mask(json_string_value(json_object_get(request, "events")), &janusEventhandler.get()->events_mask);
        
        /* Grouping */
        if(json_object_get(request, "grouping"))
            group_events = json_is_true(json_object_get(request, "grouping"));
      
    }else
    {
        JANUS_LOG(LOG_VERB, "Unknown request '%s'\n", request_text);
        error_code = JANUS_SKELETONEVH_ERROR_INVALID_REQUEST;
        
        g_snprintf(error_cause, 512, "Unknown request '%s'", request_text);
    }

plugin_response:
    {
        json_t *response = json_object();

        if(error_code == 0)
        {
            /* Return a success */
            json_object_set_new(response, "result", json_integer(200));

        }else
        {
            /* Prepare JSON error event */
            json_object_set_new(response, "error_code", json_integer(error_code));
            json_object_set_new(response, "error", json_string(error_cause));
        }

        return response;

    }
}


// JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER 
// JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER 
// JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER 
// JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER 
// JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER 
// JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER 
// JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER 
// JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER // JANUS EVENT HANDLER 


/* Thread to handle incoming events */
static void *janus_skeletonevh_handler(void *data)
{
    /**
     * @brief when event accurs elsewhere in janus, the event comes within this event plugin via 'janus_skeletonevh_incoming_event(json_t *event)'
              note that 'event' is a jansson json object.

              The 'janus_skeletonevh_incoming_event(json_t *event)' callback doesnt do anything but add it into the 'events' GAsyncQueue.
              This handler than loops trying to pop these events from the queue as they are added. In this specific example it (did) send these events off using zmq (nanomsg originally)
     * 
     */

    JANUS_LOG(LOG_VERB, "Joining NanomsgEventHandler handler thread\n");
    json_t *event = NULL, *output = NULL;
    
    char *event_text = NULL;
    int count = 0, max = group_events ? 100 : 1;

    while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping))
    {
        event = (json_t*)g_async_queue_pop(events);
        
        if(event == &exit_event)
            break;
        
        count = 0;
        output = NULL;

        // This pops events and adds to a json array if using group events
        // if not using group events it will pop one, add it to an array, and then 
        // break leaving a resulting one element array
        while(TRUE)
        {
            /* Handle event: just for fun, let's see how long it took for us to take care of this */
            json_t *created = json_object_get(event, "timestamp");
            
            if(created && json_is_integer(created))
            {
                gint64 then = json_integer_value(created);
                gint64 now = janus_get_monotonic_time();
                JANUS_LOG(LOG_DBG, "Handled event after %"SCNu64" us\n", now-then);
            }
            
            if(!group_events)
            {
                /* We're done here, we just need a single event */
                output = event;
                break;
            }
            
            /* If we got here, we're grouping */
            if(output == NULL)
                output = json_array();
            
            json_array_append_new(output, event);
            /* Never group more than a maximum number of events, though, or we might stay here forever */
            count++;

            if(count == max)
                break;
            
            event = (json_t*)g_async_queue_try_pop(events);
            
            if(event == NULL || event == &exit_event)
                break;
        
        }

       // this takes the resulting event array (output) and dumps the json object to text (JSON.stringify)
        // it then adds the resultng json string to another q and calls zmq_send to trigger the zmq handler to
        // pull from the q. This originally was very much an example level operation, doing very little actual 
        // work via zmq/nanomsg
        if(!g_atomic_int_get(&stopping))
        {
            // Since this a simple plugin, it does the same for all events: so just convert to string...
            event_text = json_dumps(output, json_format);
            
            if(event_text == NULL)
            {
                JANUS_LOG(LOG_WARN, "Failed to stringify event, event lost...\n");
                
                // Nothing we can do... get rid of the event
                json_decref(output);
                output = NULL;
                continue;
            }

            g_async_queue_push(nfd_queue, event_text);

           //zmq_send(write_zmq_pipe[1], "x", 1, 0);
        }

        /* Done, let's unref the event */
        json_decref(output);
        output = NULL;
    }
    
    return NULL;
}


// NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD 
// NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD 
// NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD 
// NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD 
// NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD 
// NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD 
// NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD 
// NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD // NANOMSG/ZMQ IO THREAD 


void *janus_skeletonevh_thread(void *data)
{
    while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping))
    {
        char *payload = (char*)g_async_queue_try_pop(nfd_queue);
        if(strlen(payload) <= 1)
        {
            break;
        }

        /**
         * @brief do something with payload
         * 
         */
  	}

    return NULL;
}

/**
 * @brief original (modified for zmq)

void *janus_skeletonevh_thread(void *data)
{
    zmq_pollitem_t items[2];

    while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping))
    {
        int fds = 0;
        items[0].socket = write_zmq_pipe[0];
        items[0].fd = 0;
        items[0].events = ZMQ_POLLIN;
        items[0].revents = 0;
        fds++;

        if(g_async_queue_length(nfd_queue) > 0)
        {
          items[1].socket = zmq_push;
          items[1].fd = 0;
          items[1].events |= ZMQ_POLLOUT;
          items[1].revents = 0;
          fds++;
        }
        
        int rc = zmq_poll (items, fds, 1000*1000);
		
        if(rc <= 0)
        {
          // interrupted
          usleep(1000);
          continue;
        }
        
        if(fds == 2 && items[1].revents & ZMQ_POLLOUT)
        { 
            if(items[1].socket == zmq_push)
            {
                char *payload = NULL;
                while((payload = g_async_queue_try_pop(nfd_queue)) != NULL)
                {
                  //JANUS_LOG(LOG_INFO, "Trying to send event %s", payload);
                  int res = zmq_send(zmq_push, payload, strlen(payload), 0);
                  //JANUS_LOG(LOG_HUGE, "Written %ld bytes\n", strlen(payload));
                  g_free(payload);
                }
                if(items[0].revents & ZMQ_POLLIN)
                {
                  char *signal_str = s_recv(write_zmq_pipe[0]);
                  free(signal_str);
                  // IF HERE IT'S SAFE TO ASSUME THAT 'write_zmq_pipe' WAS USED TO SIGNAL QUEUED EVENT/S
                  // SO 'continue'. HOWEVER MAY CHECK 'signal_str' FOR MSG RATHER THAN 'x'
                  continue;
                }
            }
        
        } // MAY TURN TO 'else if(fds == 1 && items[0].revents & ZMQ_POLLIN) BECAUSE IF 'fds == 2' IT CAN BE ASSUMED THAT 'ZMQ_POLLIN' IS SIGNAL FOR QUEUE,  
          // BUT 'zmq_poll' CAN HAVE AN EDGE TRIGGERED BEHAVIOR IN SOME CASES SO NEED TO RESEARCH AND TEST MORE. IN THIS STATE IT WILL MAKE SURE TO GET ALL
          // MESSAGES, EVEN IF SEND/RECV PATTERN BEHAVES IN UNEXPECTED MANNER
        if(items[0].revents & ZMQ_POLLIN)
        {
          char *x = s_recv(write_zmq_pipe[0]);
          free(x);
          // IF HERE IT'S SAFE TO ASSUME THAT 'write_zmq_pipe' WAS _NOT_ USED TO SIGNAL QUEUED EVENT/S
          // BUT MAY CHECK 'x' FOR MSG RATHER THAN 'x'          
        }
  	}

    return NULL;
}

 * 
 */