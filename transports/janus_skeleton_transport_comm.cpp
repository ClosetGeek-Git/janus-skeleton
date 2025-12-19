#include "janus_skeleton_transport_common.h"

using namespace std;

int janus_skeletontran_send_message(janus_transport_session *transport, void *request_id, gboolean admin, json_t *message)
{
	if(message == NULL)
		return -1;

	char *payload = json_dumps(message, json_format);
	json_decref(message);
	
    if(payload == NULL)
    {
		JANUS_LOG(LOG_ERR, "Failed to stringify message...\n");
		return -1;
	}

	g_async_queue_push(admin ? admin_client.messages : client.messages, payload);

	return 0;
}

/* Thread */
void *janus_skeletontran_thread(void *data) 
{
	JANUS_LOG(LOG_INFO, "SKELETONTRAN thread started in SKELETONTRAN transport\n");

	while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping))
    {
        // TODO was originally `char *payload = (char*)g_async_queue_pop(items[i].socket == transport_sock ? client.messages : admin_client.messages);`
        // to allow both somethow 

        // this is broken
        char *payload = (char*)g_async_queue_pop(1 ? client.messages : admin_client.messages);

        if(payload == NULL || strlen(payload) <= 1)
        {
            break;
        }
    
        /**
         * @brief do something with payload

            TODO: will try pop return if none in queue? (hence the "try")
         * 
         */       
  
	}

	JANUS_LOG(LOG_INFO, "SKELETONTRAN thread in SKELETONTRAN transport ended\n");
	return NULL;
}