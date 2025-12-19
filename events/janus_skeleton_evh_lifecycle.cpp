#include "janus_skeleton_evh_common.h"

using namespace std;

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
