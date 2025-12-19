#include "janus_skeleton_evh_common.h"

using namespace std;

void janus_skeletonevh_event_free(json_t *event)
{
    if(!event || event == &exit_event)
      return;

    json_decref(event);
}

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
