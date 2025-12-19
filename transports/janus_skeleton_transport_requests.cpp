#include "janus_skeleton_transport_common.h"

using namespace std;

/* Parameter validation (for tweaking and queries via Admin API) */
struct janus_json_parameter request_parameters[] = {
	{"request", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};
struct janus_json_parameter configure_parameters[] = {
	{"events", JANUS_JSON_BOOL, 0},
	{"json", JSON_STRING, 0},
};

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