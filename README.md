heka-stream-aggregator
======================
Stream aggregation for [Mozilla Heka](http://hekad.readthedocs.org/)

StreamAggregatorFilter
===========
This is a filter that can be used for aggregating multiple payloads into a single message.
It accepts an encoder option that will be used prior to aggregating each payload.
Each payload is separated by the delimitter config value.

Config: 

- stream_aggregator_tag:
	Optional tagging for identifying new pack down the pipeline since you will lose any Fields previously held.
	This setting creates a new Heka Field called "StreamAggregatorTag" and is given the value of this option. Defaults to "aggregated"

- flush_interval: 
	Interval at which accumulated payloads should be compressed in milliseconds.
	Defaults to 1000 (i.e. one second)

- flush_bytes:
	Number of payloads that, if processed, will trigger them to be compressed.
	Defaults to 10.

- encoder:
	This option will run each Payload through the specified encoder prior to aggregating.(required)

To Build
========

See [Building *hekad* with External Plugins](http://hekad.readthedocs.org/en/latest/installing.html#build-include-externals)
for compiling in plugins.

Edit cmake/plugin_loader.cmake file and add

    add_external_plugin(git https://github.com/michaelgibson/heka-stream-aggregator master)

Build Heka:
	. ./build.sh


Config
======
	[filter_stream_aggregator]
	type = "StreamAggregatorFilter"
	message_matcher = "Fields[decoded] == 'True'"
	stream_aggregator_tag = "aggregated"
	flush_interval = 30000
	flush_bytes = 1000000
	encoder = "encoder_json"
	delimitter = "\n" # Default
