/* 
 * Version: MPL 2.0
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is FreeSWITCH Modular Event Handler Software Library / Soft-Switch Application
 *
 * Portions created by the Initial Developer are Copyright (C)
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 * 
 * Cui Gang <cuigang0120@aliyun.com>
 *
 *
 *
 */

#include <switch.h>
#include <rdkafka.h>

SWITCH_MODULE_LOAD_FUNCTION(mod_event_kafka_load);
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_event_kafka_shutdown);
SWITCH_MODULE_RUNTIME_FUNCTION(mod_event_kafka_runtime);
SWITCH_MODULE_DEFINITION(mod_event_kafka, mod_event_kafka_load, mod_event_kafka_shutdown, mod_event_kafka_runtime);

static struct {
	switch_event_node_t *node;
	int debug;
} globals;

static struct {
	char *brokers;
	char *topic;
} prefs;

static int run = 1;
// kafka
rd_kafka_t *rk;         /* Producer instance handle */
rd_kafka_topic_t *rkt;  /* Topic object */
rd_kafka_conf_t *conf;  /* Temporary configuration object */

static void dr_msg_cb (rd_kafka_t *rk,
                       const rd_kafka_message_t *rkmessage, void *opaque) {
        if (rkmessage->err)
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Kafka - Message delivery failed: %s\n",
                        rd_kafka_err2str(rkmessage->err));
}

static void event_handler(switch_event_t *event)
{
	// Serialize the event into a JSON string
	char* pjson;
	switch_event_serialize_json(event, &pjson);

	if (rd_kafka_produce(
                /* Topic object */
                rkt,
                /* Use builtin partitioner to select partition*/
                RD_KAFKA_PARTITION_UA,
                /* Make a copy of the payload. */
                RD_KAFKA_MSG_F_COPY,
                /* Message payload (value) and length */
                pjson, strlen(pjson),
                /* Optional key and its length */
                NULL, 0,
                /* Message opaque, provided in
                 * delivery report callback as
                 * msg_opaque. */
                NULL) == -1) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Kafka - Failed to produce to topic %s: %s\n",
			rd_kafka_topic_name(rkt), rd_kafka_err2str(rd_kafka_last_error()) );
    }
    rd_kafka_poll(rk, 0/*non-blocking*/);
	switch_safe_free(pjson);
}

static void free_prefs()
{
	switch_safe_free(prefs.brokers);
	switch_safe_free(prefs.topic);
}

SWITCH_DECLARE_GLOBAL_STRING_FUNC(set_pref_brokers, prefs.brokers);
SWITCH_DECLARE_GLOBAL_STRING_FUNC(set_pref_topic, prefs.topic);

static switch_status_t config(void)
{
	char *cf = "event_kafka.conf";
	switch_xml_t cfg, xml, settings, param;

	memset(&prefs, 0, sizeof(prefs));

	if (!(xml = switch_xml_open_cfg(cf, &cfg, NULL))) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Open of %s failed\n", cf);
	} else {
		if ((settings = switch_xml_child(cfg, "settings"))) {
			for (param = switch_xml_child(settings, "param"); param; param = param->next) {
				char *var = (char *) switch_xml_attr_soft(param, "name");
				char *val = (char *) switch_xml_attr_soft(param, "value");

				if (!strcmp(var, "brokers")) {
					set_pref_brokers(val);
				}else if (!strcmp(var, "topic")) {
					set_pref_topic(val);
				}
			}
		}
		switch_xml_free(xml);
	}

	//if (zstr(prefs.endpoint)) {
	//	set_pref_endpoint("tcp://127.0.0.1:5556");
	//}

	return SWITCH_STATUS_SUCCESS;
}

SWITCH_MODULE_LOAD_FUNCTION(mod_event_kafka_load)
{
	char errstr[512];
	// Load config XML
	config();

	// Set up the kafka 
	conf = rd_kafka_conf_new();
	if (rd_kafka_conf_set(conf, "bootstrap.servers", prefs.brokers,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Kafka - %s\n", errstr);
            return SWITCH_STATUS_GENERR;
    }

	rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
	
	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Kafka - Failed to create new producer: %s\n", errstr);
            return SWITCH_STATUS_GENERR;
    }

    rkt = rd_kafka_topic_new(rk, prefs.topic, NULL);
    if (!rkt) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Kafka - Failed to create topic object: %s\n", 
            	rd_kafka_err2str(rd_kafka_last_error()));
            rd_kafka_destroy(rk);
            
            return SWITCH_STATUS_GENERR;
    }

	memset(&globals, 0, sizeof(globals));
	// Subscribe to all switch events of any subclass
	// Store a pointer to ourself in the user data
	if (switch_event_bind_removable(modname, SWITCH_EVENT_ALL, SWITCH_EVENT_SUBCLASS_ANY, event_handler, NULL, &globals.node) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Couldn't bind!\n");
		return SWITCH_STATUS_GENERR;
	}


	/* connect my internal structure to the blank pointer passed to me */
	*module_interface = switch_loadable_module_create_module_interface(pool, modname);
	
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Module loaded\n");
	/* indicate that the module should continue to be loaded */
	return SWITCH_STATUS_SUCCESS;
}

SWITCH_MODULE_RUNTIME_FUNCTION(mod_event_kafka_runtime) 
{

	while(run==1) {
		switch_yield(100000);
	}
	run = 2;
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "mod_event_kafka runloop end!\n");
	// Tell the switch to stop calling this runtime loop
	return SWITCH_STATUS_TERM;
}

SWITCH_MOD_DECLARE(switch_status_t) mod_event_kafka_shutdown(void)
{
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Shutdown requested, sending term message to runloop\n");
	run = 0;
	while(run==0) {
		switch_yield(10000);
	}

	rd_kafka_flush(rk, 10*1000 /* wait for max 10 seconds */);
    /* Destroy topic object */
    rd_kafka_topic_destroy(rkt);
    /* Destroy the producer instance */
    rd_kafka_destroy(rk);

	switch_event_unbind(&globals.node);
	
	free_prefs();

	return SWITCH_STATUS_SUCCESS;
}

