#include "janus_skeleton_transport_common.h"

using namespace std;

void session_id_hashmap_destroy(session_transport *trns)
{
	if(trns && g_atomic_int_compare_and_exchange(&trns->destroyed, 0, 1))
	{
		janus_refcount_decrease(&trns->ref);
	}
}

void session_id_hashmap_free(const janus_refcount *session_ref)
{
	session_transport *trns = janus_refcount_containerof(session_ref, session_transport, ref);

	janus_refcount_decrease(&trns->ref);

	free(trns);
}

void janus_skeletontran_session_created(janus_transport_session *transport, guint64 session_id) {
}

void janus_skeletontran_session_over(janus_transport_session *transport, guint64 session_id, gboolean timeout, gboolean claimed) {
}

void janus_skeletontran_session_claimed(janus_transport_session *transport, guint64 session_id) {
}