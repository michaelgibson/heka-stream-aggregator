/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2012
# the Initial Developer. All Rights Reserved.
#
# ***** END LICENSE BLOCK *****/

package stream_aggregator

import (
    "time"
    "encoding/json"
    . "github.com/mozilla-services/heka/pipeline"
    "strings"
)

type StreamSplitterDecoderConfig struct {
    // Keyed to the message field that should be filled in, the value will be
    // interpolated so it can use capture parts from the message match.
    Delimiter string `toml:"delimiter"` // Delimiter used to append to end of each protobuf for splitting on when decoding later.
                                          // Defaults to '\n'
}

type StreamSplitterDecoder struct {
    *StreamSplitterDecoderConfig
    dRunner         DecoderRunner
}

func (ld *StreamSplitterDecoder) ConfigStruct() interface{} {
    return &StreamSplitterDecoderConfig{
        Delimiter:     "\n",
    }
}

func (ld *StreamSplitterDecoder) Init(config interface{}) (err error) {
    ld.StreamSplitterDecoderConfig = config.(*StreamSplitterDecoderConfig)
    return
}

// Heka will call this to give us access to the runner.
func (ld *StreamSplitterDecoder) SetDecoderRunner(dr DecoderRunner) {
    ld.dRunner = dr
}


// Runs the message payload against decoder's map of JSONPaths. If
// there's a match, the message will be populated based on the
// decoder's message template, with capture values interpolated into
// the message template values.
func (ld *StreamSplitterDecoder) Decode(pack *PipelinePack) (packs []*PipelinePack, err error) {
    var f interface{}
    delimiter := ld.Delimiter
    
    bodySlice := strings.Split(pack.Message.GetPayload(), delimiter)
 
    for _,element := range bodySlice {
        err := json.Unmarshal([]byte(element), &f)
        if err == nil {

        newpack := ld.dRunner.NewPack()
        newpack.Message.SetPayload(string(element))
        newpack.Message.SetTimestamp(time.Now().UTC().UnixNano())
        packs = append(packs, newpack)
    }
    }
    pack.Recycle(nil)
    return
}

func init() {
        RegisterPlugin("StreamSplitterDecoder", func() interface{} {
                return new(StreamSplitterDecoder)
        })
}
