#include "janus_skeleton_transport_common.h"

using namespace std;

/* Plugin implementation */
int janus_skeletontran_init(janus_transport_callbacks *callback, const char *config_path)
{
	if(g_atomic_int_get(&stopping)) {
		return -1;
	}

	if(callback == NULL || config_path == NULL) {
		/* Invalid arguments */
		return -1;
	}

	gateway = callback;

    gboolean success = TRUE;

    pending_transaction_hashtable = g_hash_table_new_full(g_str_hash, g_str_equal, NULL, NULL);
    session_id_hashtable = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, (GDestroyNotify)session_id_hashmap_destroy);

    has_admin = FALSE;

    do
    {
        /* Create the clients */
        memset(&client, 0, sizeof(janus_skeletontran_client));

        client.admin = FALSE;
        client.messages = g_async_queue_new();

        /* Create a transport instance as well */
        client.ts = janus_transport_session_create(&client, NULL);
        
        /* Notify handlers about this new transport */

        /**
         * @brief THIS IS BROKEN. NOTE THAT BOTH ARE SIGNALLING 'admin_api'
                  ARE SIGNALS EVEN NEEDED HERE?
         * 
         */

        // NO CLUE... NEED TO FIGURE OUT. CHECK OUT ORIGINAL NANOMSG TRANSPORT
        char addy[128];
        char admin_addy[128];

        if(notify_events && gateway->events_is_enabled()) {
            json_t *info = json_object();
            json_object_set_new(info, "event", json_string("created"));
            json_object_set_new(info, "admin_api", json_false());
            json_object_set_new(info, "address", json_string(addy));
            gateway->notify_event(janusTransport.get(), client.ts, info);
        }

        if(has_admin == TRUE)
        {
            memset(&admin_client, 0, sizeof(janus_skeletontran_client));

            admin_client.admin = TRUE;
            admin_client.messages = g_async_queue_new();
            /* Create a transport instance as well */
            admin_client.ts = janus_transport_session_create(&admin_client, NULL);
            /* Notify handlers about this new transport */
            if(notify_events && gateway->events_is_enabled()) {
                json_t *info = json_object();
                json_object_set_new(info, "event", json_string("created"));
                json_object_set_new(info, "admin_api", json_true());
                json_object_set_new(info, "address", json_string(admin_addy));
                gateway->notify_event(janusTransport.get(), admin_client.ts, info);
            }
        }

        g_atomic_int_set(&initialized, 1);

        /* Start the handler thread */
        GError *thread_error = NULL;
        handler_thread = g_thread_try_new("skeletontran thread", &janus_skeletontran_thread, NULL, &thread_error);

        if(thread_error != NULL) {
            g_atomic_int_set(&initialized, 0);
            JANUS_LOG(LOG_ERR, "Got error %d (%s) trying to launch the SKELETONTRAN thread...\n", thread_error->code, thread_error->message ? thread_error->message : "??");
            g_error_free(thread_error);
            success = FALSE;
            break;
        }

    }while(FALSE);

    if(success == FALSE)
    {
        g_atomic_int_set(&initialized, 0);
        g_atomic_int_set(&stopping, 1);
        /* If we got here, something went wrong */
        success = FALSE;

        if(client.ts != NULL)
        {
            janus_transport_session_destroy(client.ts);
            client.ts = NULL;
        }

        if(admin_client.ts != NULL)
        {     
            janus_transport_session_destroy(admin_client.ts);
            admin_client.ts = NULL;
        }
        return -1;
        
    }

	g_atomic_int_set(&initialized, 1);
	JANUS_LOG(LOG_INFO, "%s initialized!\n", JANUS_SKELETONTRAN_NAME);

    return 0;
}

void janus_skeletontran_destroy(void) {
	if(!g_atomic_int_get(&initialized))
		return;
	
    g_atomic_int_set(&stopping, 1);

    if(client.ts != NULL)
    {
        janus_transport_session_destroy(client.ts);
        client.ts = NULL;
    }

    if(admin_client.ts != NULL)
    {     
        janus_transport_session_destroy(admin_client.ts);
        admin_client.ts = NULL;
    }

	if(handler_thread != NULL) {
		g_thread_join(handler_thread);
		handler_thread = NULL;
	}

	g_atomic_int_set(&initialized, 0);
	g_atomic_int_set(&stopping, 0);

	JANUS_LOG(LOG_INFO, "%s destroyed!\n", JANUS_SKELETONTRAN_NAME);
}

int janus_skeletontran_get_api_compatibility(void) {	return JANUS_TRANSPORT_API_VERSION; }
int janus_skeletontran_get_version(void) { return JANUS_SKELETONTRAN_VERSION; }
const char *janus_skeletontran_get_version_string(void) { return JANUS_SKELETONTRAN_VERSION_STRING; }
const char *janus_skeletontran_get_description(void) { return JANUS_SKELETONTRAN_DESCRIPTION; }
const char *janus_skeletontran_get_name(void) { return JANUS_SKELETONTRAN_NAME; }
const char *janus_skeletontran_get_author(void) { return JANUS_SKELETONTRAN_AUTHOR; }
const char *janus_skeletontran_get_package(void) { return JANUS_SKELETONTRAN_PACKAGE; }

gboolean janus_skeletontran_is_janus_api_enabled(void) {
	return TRUE;
}

gboolean janus_skeletontran_is_admin_api_enabled(void) {
	return has_admin;
}