#include "janus_skeleton_plugin_common.h"

using namespace std;

void janus_skeleton_plugin_create_session(janus_plugin_session *handle, int *error)
{
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
	{
		*error = -1;
		return;
	}
	
	janus_skeleton_session *session = (janus_skeleton_session *)g_malloc0(sizeof(janus_skeleton_session));
	session->handle = handle;
	session->destroyed = 0;

	// CRATE RANDOM DCID HERE AND ADD TO session->dcid
	janus_mutex_init(&session->mutex);
	janus_refcount_init(&session->ref, janus_skeleton_session_free);

	g_atomic_int_set(&session->setup, 0);
	g_atomic_int_set(&session->dataready, 0);
	g_atomic_int_set(&session->hangingup, 0);
	
	janus_mutex_lock(&sessions_mutex);
		g_hash_table_insert(sessions, handle, session);
	janus_mutex_unlock(&sessions_mutex);


	JANUS_LOG(LOG_INFO, "SkeletonSession created.\n");

	return;
}

void janus_skeleton_plugin_destroy_session(janus_plugin_session *handle, int *error)
{
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
	{
		*error = -1;
		return;
	}
	
	janus_mutex_lock(&sessions_mutex);
		janus_skeleton_session *session = janus_skeleton_lookup_session(handle);
		
		if(!session)
		{
			janus_mutex_unlock(&sessions_mutex);
			JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		
			*error = -2;
			return;
		}
		
		JANUS_LOG(LOG_VERB, "Removing Skeleton session...\n");

		janus_skeleton_hangup_media_handler(handle);
		
		g_hash_table_remove(sessions, handle);
	janus_mutex_unlock(&sessions_mutex);

	return;
}

json_t *janus_skeleton_plugin_query_session(janus_plugin_session *handle)
{
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
	{
		return NULL;
	}
	
	janus_mutex_lock(&sessions_mutex);
	janus_skeleton_session *session = janus_skeleton_lookup_session(handle);
	
	if(!session)
	{
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No Skeleton session associated with this handle...\n");
	
		return NULL;
	}
	
	janus_refcount_increase(&session->ref);
	janus_mutex_unlock(&sessions_mutex);
	
	json_t *info = json_object();
	json_object_set_new(info, "destroyed", json_integer(session->destroyed));
	janus_refcount_decrease(&session->ref);
	
	return info;
}

void janus_skeleton_plugin_setup_media(janus_plugin_session *handle)
{
	JANUS_LOG(LOG_INFO, "[%s-%p] WebRTC media is now available\n", JANUS_SKELETON_PLUGIN_PACKAGE, handle);
	
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	
	janus_mutex_lock(&sessions_mutex);
	
	janus_skeleton_session *session = janus_skeleton_lookup_session(handle);
	
	if(!session)
	{
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
	
		return;
	}
	
	if(session->destroyed)
	{
		janus_mutex_unlock(&sessions_mutex);
		return;
	}
	
	g_atomic_int_set(&session->hangingup, 0);
	janus_mutex_unlock(&sessions_mutex);
}

void janus_skeleton_plugin_incoming_rtp(janus_plugin_session *handle, janus_plugin_rtp* wtf)
{
	JANUS_LOG(LOG_VERB, "Got an RTP message)\n");
}

void janus_skeleton_plugin_incoming_rtcp(janus_plugin_session *handle, janus_plugin_rtcp* wtf)
{
	JANUS_LOG(LOG_VERB, "Got an RTCP message\n");
}

void janus_skeleton_plugin_slow_link(janus_plugin_session *handle, int uplink, gboolean wtf, gboolean wth)
{
	JANUS_LOG(LOG_VERB, "Slow link detected.\n");
}

void janus_skeleton_plugin_hangup_media(janus_plugin_session *handle)
{
	janus_mutex_lock(&sessions_mutex);
	janus_skeleton_hangup_media_handler(handle);
	janus_mutex_unlock(&sessions_mutex);
}

void janus_skeleton_hangup_media_handler(janus_plugin_session *handle)
{
	JANUS_LOG(LOG_INFO, "[%s-%p] No WebRTC media anymore\n", JANUS_SKELETON_PLUGIN_PACKAGE, handle);
	
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	
	// GET SESSION from handle.
	janus_skeleton_session *session = janus_skeleton_lookup_session(handle);
	
	if(!session)
	{
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		
		return;
	}
	
	if(session->destroyed)
		return;
	
	if(!g_atomic_int_compare_and_exchange(&session->hangingup, 0, 1))
		return;
	
	g_atomic_int_set(&session->dataready, 0);
	g_atomic_int_set(&session->hangingup, 0);
}
