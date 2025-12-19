#include "janus_skeleton_plugin_common.h"

using namespace std;

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

void *janus_skeleton_handler_thread(void *data)
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
