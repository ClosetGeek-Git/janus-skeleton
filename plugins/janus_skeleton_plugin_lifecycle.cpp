#include "janus_skeleton_plugin_common.h"

using namespace std;

int janus_skeleton_plugin_init(janus_callbacks *callback, const char *config_path)
{
	char *initvars = getenv("INITVARS");

	if(initvars == NULL)
	{
		JANUS_LOG(LOG_ERR, "Must set environment variable INITVARS (INITVARS == NULL)\n");
		return -1;
	}

	json_error_t json_error;
	json_t *initvars_root = json_loads(initvars, 0, &json_error);

	if(!initvars_root)
	{
		JANUS_LOG(LOG_ERR, "Error parsing INITVARS environmental variable (JSON error: on line %d: %s) in: (%s)\n", json_error.line, json_error.text, initvars);
		return -1;
	}

	// get address
	json_t *id_el = json_object_get(initvars_root, "id");
	if(id_el == NULL)
	{
		JANUS_LOG(LOG_ERR, "Error 'INITVARS' did not provide 'id'\n");
	}

	const char *id_text = json_string_value(id_el);

	if(id_text == NULL || strlen(id_text) > 255)
	{
		JANUS_LOG(LOG_ERR, "Error 'INITVARS' 'id' not properly set, must be string 255 chars or less\n");
	}

	
	if(g_atomic_int_get(&stopping))
	{
		/* Still stopping from before */
		return -1;
	}

	if(callback == NULL || config_path == NULL)
	{
		/* Invalid arguments */
		return -1;
	}

	sessions = g_hash_table_new_full(NULL, NULL, NULL, (GDestroyNotify)janus_skeleton_session_destroy);
	messages_q = g_async_queue_new_full((GDestroyNotify) janus_skeleton_message_free);

	/* This is the callback we'll need to invoke to contact the Janus core */
	gateway = callback;

	g_atomic_int_set(&initialized, 1);

	GError *error = NULL;

	/* Launch the thread that will handle incoming messages */
	handler_thread = g_thread_try_new("skeleton handler", janus_skeleton_handler_thread, NULL, &error);

	if(error != NULL)
	{
		g_atomic_int_set(&initialized, 0);
		JANUS_LOG(LOG_ERR, "Got error %d (%s) trying to launch the skeleton handler thread...\n", error->code, error->message ? error->message : "??");

		g_error_free(error);

		return -1;
	}

	/**
	 * @brief DO STUFF HERE
	 * 
	 */

	if(initvars_root != NULL)
		json_decref(initvars_root);

	JANUS_LOG(LOG_INFO, "%s initialized!\n", JANUS_SKELETON_PLUGIN_NAME);

	return 0;
}

void janus_skeleton_plugin_destroy(void)
{
	
	if(!g_atomic_int_get(&initialized))
		return;

	
	g_atomic_int_set(&stopping, 1);
	g_async_queue_push(messages_q, &exit_message);

	if(handler_thread != NULL)
	{
		g_thread_join(handler_thread);
		handler_thread = NULL;
	}

	janus_mutex_lock(&sessions_mutex);
	
		g_hash_table_destroy(sessions);
		sessions = NULL;

	janus_mutex_unlock(&sessions_mutex);

	/**
	 * @todo IS THIS REALLY DONE? WHAT TO DO ABOUT SESSION MUTEX ETC
	 * 
	 */


	g_async_queue_unref(messages_q);
	
	messages_q = NULL;
	
	g_atomic_int_set(&initialized, 0);
	g_atomic_int_set(&stopping, 0);

	JANUS_LOG(LOG_INFO, "%s destroyed!\n", JANUS_SKELETON_PLUGIN_NAME);

}

int janus_skeleton_plugin_get_api_compatibility(void) {
	return JANUS_PLUGIN_API_VERSION;
}

int janus_skeleton_plugin_get_version(void) {
	return JANUS_SKELETON_PLUGIN_VERSION;
}

const char *janus_skeleton_plugin_get_version_string(void) {
	return JANUS_SKELETON_PLUGIN_VERSION_STRING;
}

const char *janus_skeleton_plugin_get_description(void) {
	return JANUS_SKELETON_PLUGIN_DESCRIPTION;
}

const char *janus_skeleton_plugin_get_name(void) {
	return JANUS_SKELETON_PLUGIN_NAME;
}

const char *janus_skeleton_plugin_get_author(void) {
	return JANUS_SKELETON_PLUGIN_AUTHOR;
}

const char *janus_skeleton_plugin_get_package(void) {
	return JANUS_SKELETON_PLUGIN_PACKAGE;
}
