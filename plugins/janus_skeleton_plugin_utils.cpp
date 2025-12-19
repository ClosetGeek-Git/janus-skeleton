#include "janus_skeleton_plugin_common.h"

using namespace std;

void janus_skeleton_message_free(janus_skeleton_message *msg)
{
	if(!msg || msg == &exit_message)
		return;

	if(msg->handle && msg->handle->plugin_handle)
	{
		janus_skeleton_session *session = (janus_skeleton_session *)msg->handle->plugin_handle;
		janus_refcount_decrease(&session->ref);
	}
	
	msg->handle = NULL;

	g_free(msg->transaction);
	msg->transaction = NULL;
	
	if(msg->message)
		json_decref(msg->message);
	
	msg->message = NULL;
	
	if(msg->jsep)
		json_decref(msg->jsep);
	
	msg->jsep = NULL;

	g_free(msg);
}

void janus_skeleton_session_destroy(janus_skeleton_session *session)
{
	if(session && g_atomic_int_compare_and_exchange(&session->destroyed, 0, 1))
	{
		janus_refcount_decrease(&session->ref);
	}
}

void janus_skeleton_session_free(const janus_refcount *session_ref)
{
	janus_skeleton_session *session = janus_refcount_containerof(session_ref, janus_skeleton_session, ref);
	g_free(session);
}

janus_skeleton_session *janus_skeleton_lookup_session(janus_plugin_session *handle)
{
	janus_skeleton_session *session = NULL;
	
	if (g_hash_table_contains(sessions, handle))
	{
		session = (janus_skeleton_session *)handle->plugin_handle;
	}
	
	return session;
}

/* CRC32 utility functions */
static unsigned int table[256];

void setup_crc32_table()
{
	int j;
	unsigned int byte, crc, mask;

	/* Set up the table, if necessary. */

	if (table[1] == 0)
	{
		for (byte = 0; byte <= 255; byte++)
		{
			crc = byte;
			for (j = 7; j >= 0; j--) 
			{
				mask = -(crc & 1);
				crc = (crc >> 1) ^ (0xEDB88320 & mask);
			}
			table[byte] = crc;
		}
	}
}

unsigned int crc32(char *message)
{
	unsigned int crc, word;

	/* Through with table setup, now calculate the CRC. */

	crc = 0xFFFFFFFF;
	while (((word = *(unsigned int *)message) & 0xFF) != 0)
	{
		crc = crc ^ word;
		crc = (crc >> 8) ^ table[crc & 0xFF];
		crc = (crc >> 8) ^ table[crc & 0xFF];
		crc = (crc >> 8) ^ table[crc & 0xFF];
		crc = (crc >> 8) ^ table[crc & 0xFF];
		message = message + 4;
	}
	return ~crc;
}

unsigned int ucrc32(unsigned char *message)
{
	unsigned int crc, word;

	/* Through with table setup, now calculate the CRC. */

	crc = 0xFFFFFFFF;
	while (((word = *(unsigned int *)message) & 0xFF) != 0) {
		crc = crc ^ word;
		crc = (crc >> 8) ^ table[crc & 0xFF];
		crc = (crc >> 8) ^ table[crc & 0xFF];
		crc = (crc >> 8) ^ table[crc & 0xFF];
		crc = (crc >> 8) ^ table[crc & 0xFF];
		message = message + 4;
	}
	return ~crc;
}
