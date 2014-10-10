heka-stream-aggregator
======================
Stream aggregation for [Mozilla Heka](http://hekad.readthedocs.org/)

StreamAggregatorFilter
===========


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
