#include "janus_skeleton_evh_common.h"

using namespace std;

/* Thread to handle incoming events */
void *janus_skeletonevh_handler(void *data)
{
    /**
     * @brief when event accurs elsewhere in janus, the event comes within this event plugin via 'janus_skeletonevh_incoming_event(json_t *event)'
               note that 'event' is a jansson json object.

               The 'janus_skeletonevh_incoming_event(json_t *event)' callback doesnt do anything but add it into the 'events' GAsyncQueue.
               This handler than loops trying to pop these events from the queue as they are added. In this specific example it (did) send these events off using zmq (nanomsg originally)
     * 
     */

    JANUS_LOG(LOG_VERB, "Joining NanomsgEventHandler handler thread\n");
    json_t *event = NULL, *output = NULL;
    
    char *event_text = NULL;
    int count = 0, max = group_events ? 100 : 1;

    while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping))
    {
        event = (json_t*)g_async_queue_pop(events);
        
        if(event == &exit_event)
            break;
        
        count = 0;
        output = NULL;

        // This pops events and adds to a json array if using group events
        // if not using group events it will pop one, add it to an array, and then 
        // break leaving a resulting one element array
        while(TRUE)
        {
            /* Handle event: just for fun, let's see how long it took for us to take care of this */
            json_t *created = json_object_get(event, "timestamp");
            
            if(created && json_is_integer(created))
            {
                gint64 then = json_integer_value(created);
                gint64 now = janus_get_monotonic_time();
                JANUS_LOG(LOG_DBG, "Handled event after %"SCNu64" us\n", now-then);
            }
            
            if(!group_events)
            {
                /* We're done here, we just need a single event */
                output = event;
                break;
            }
            
            /* If we got here, we're grouping */
            if(output == NULL)
                output = json_array();
            
            json_array_append_new(output, event);
            /* Never group more than a maximum number of events, though, or we might stay here forever */
            count++;

            if(count == max)
                break;
            
            event = (json_t*)g_async_queue_try_pop(events);
            
            if(event == NULL || event == &exit_event)
                break;
        
        }

       // this takes the resulting event array (output) and dumps the json object to text (JSON.stringify)
        // it then adds the resultng json string to another q and calls zmq_send to trigger the zmq handler to
        // pull from the q. This originally was very much an example level operation, doing very little actual 
        // work via zmq/nanomsg
        if(!g_atomic_int_get(&stopping))
        {
            // Since this a simple plugin, it does the same for all events: so just convert to string...
            event_text = json_dumps(output, json_format);
            
            if(event_text == NULL)
            {
                JANUS_LOG(LOG_WARN, "Failed to stringify event, event lost...\n");
                
                // Nothing we can do... get rid of the event
                json_decref(output);
                output = NULL;
                continue;
            }

            g_async_queue_push(nfd_queue, event_text);

           //zmq_send(write_zmq_pipe[1], "x", 1, 0);
        }

        /* Done, let's unref the event */
        json_decref(output);
        output = NULL;
    }
    
    return NULL;
}

void *janus_skeletonevh_thread(void *data)
{
    while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping))
    {
        char *payload = (char*)g_async_queue_try_pop(nfd_queue);
        if(strlen(payload) <= 1)
        {
            break;
        }

        /**
         * @brief do something with payload
         * 
         */
  	}

    return NULL;
}

/**
 * @brief original (modified for zmq)

void *janus_skeletonevh_thread(void *data)
{
    zmq_pollitem_t items[2];

    while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping))
    {
        int fds = 0;
        items[0].socket = write_zmq_pipe[0];
        items[0].fd = 0;
        items[0].events = ZMQ_POLLIN;
        items[0].revents = 0;
        fds++;

        if(g_async_queue_length(nfd_queue) > 0)
        {
          items[1].socket = zmq_push;
          items[1].fd = 0;
          items[1].events |= ZMQ_POLLOUT;
          items[1].revents = 0;
          fds++;
        }
        
        int rc = zmq_poll (items, fds, 1000*1000);
		
        if(rc <= 0)
        {
          // interrupted
          usleep(1000);
          continue;
        }
        
        if(fds == 2 && items[1].revents & ZMQ_POLLOUT)
        { 
            if(items[1].socket == zmq_push)
            {
                char *payload = NULL;
                while((payload = g_async_queue_try_pop(nfd_queue)) != NULL)
                {
                  //JANUS_LOG(LOG_INFO, "Trying to send event %s", payload);
                  int res = zmq_send(zmq_push, payload, strlen(payload), 0);
                  //JANUS_LOG(LOG_HUGE, "Written %ld bytes\n", strlen(payload));
                  g_free(payload);
                }
                if(items[0].revents & ZMQ_POLLIN)
                {
                  char *signal_str = s_recv(write_zmq_pipe[0]);
                  free(signal_str);
                  // IF HERE IT'S SAFE TO ASSUME THAT 'write_zmq_pipe' WAS USED TO SIGNAL QUEUED EVENT/S
                  // SO 'continue'. HOWEVER MAY CHECK 'signal_str' FOR MSG RATHER THAN 'x'
                  continue;
                }
            }
        
        } // MAY TURN TO 'else if(fds == 1 && items[0].revents & ZMQ_POLLIN) BECAUSE IF 'fds == 2' IT CAN BE ASSUMED THAT 'ZMQ_POLLIN' IS SIGNAL FOR QUEUE,  
          // BUT 'zmq_poll' CAN HAVE AN EDGE TRIGGERED BEHAVIOR IN SOME CASES SO NEED TO RESEARCH AND TEST MORE. IN THIS STATE IT WILL MAKE SURE TO GET ALL
          // MESSAGES, EVEN IF SEND/RECV PATTERN BEHAVES IN UNEXPECTED MANNER
        if(items[0].revents & ZMQ_POLLIN)
        {
          char *x = s_recv(write_zmq_pipe[0]);
          free(x);
          // IF HERE IT'S SAFE TO ASSUME THAT 'write_zmq_pipe' WAS _NOT_ USED TO SIGNAL QUEUED EVENT/S
          // BUT MAY CHECK 'x' FOR MSG RATHER THAN 'x'          
        }
  	}

    return NULL;
}

 * 
 */
