package stream_aggregator


import (
        "fmt"
        "time"
        "github.com/mozilla-services/heka/message"
        . "github.com/mozilla-services/heka/pipeline"
        "code.google.com/p/go-uuid/uuid"
        "sync"
)

type StreamAggregatorFilter struct {
        *StreamAggregatorFilterConfig
        batchChan chan []byte
        backChan  chan []byte
        msgLoopCount uint
}

type StreamAggregatorFilterConfig struct {
        Delimiter          string `toml:"delimiter"` // Delimiter used to append to end of each protobuf for splitting on when decoding later.
                                                       // Defaults to '\n'
        FlushInterval uint32 `toml:"flush_interval"`
        FlushBytes    int    `toml:"flush_bytes"`
        StreamAggregatorTag       string `toml:"stream_aggregator_tag"`
        EncoderName   string `toml:"encoder"`
}

func (f *StreamAggregatorFilter) ConfigStruct() interface{} {
        return &StreamAggregatorFilterConfig{
        Delimiter:     "\n",
        FlushInterval: 1000,
        FlushBytes:    10,
        StreamAggregatorTag:       "aggregated",
        }
}

func (f *StreamAggregatorFilter) Init(config interface{}) (err error) {
        f.StreamAggregatorFilterConfig = config.(*StreamAggregatorFilterConfig)
        f.batchChan = make(chan []byte)
        f.backChan = make(chan []byte, 2)

        if f.StreamAggregatorTag == "" {
            return fmt.Errorf(`A stream_aggregator_tag value must be specified for the StreamAggregatorTag Field`)
        }

        if f.EncoderName == "" {
            return fmt.Errorf(`An encoder must be specified`)
        }

        return
}

func (f *StreamAggregatorFilter) committer(fr FilterRunner, h PluginHelper, wg *sync.WaitGroup) {
        initBatch := make([]byte, 0, 10000)
        f.backChan <- initBatch
        var (
            tag string
            outBatch []byte
        )
        tag = f.StreamAggregatorTag

        for outBatch = range f.batchChan {
                pack := h.PipelinePack(f.msgLoopCount)
                if pack == nil {
                        fr.LogError(fmt.Errorf("exceeded MaxMsgLoops = %d",
                                h.PipelineConfig().Globals.MaxMsgLoops))
            break   
                }
        
                tagField, _ := message.NewField("StreamAggregatorTag", tag, "")
                pack.Message.AddField(tagField)
                pack.Message.SetUuid(uuid.NewRandom())
                pack.Message.SetPayload(string(outBatch))
                fr.Inject(pack)

                outBatch = outBatch[:0]
                f.backChan <- outBatch
        }
        wg.Done()
}

func (f *StreamAggregatorFilter) receiver(fr FilterRunner, h PluginHelper, encoder Encoder, wg *sync.WaitGroup) {
        var (
                pack *PipelinePack
                ok bool   
                e        error
        )
        ok = true
        delimiter := f.Delimiter
        outBatch := make([]byte, 0, 10000)
        outBytes := make([]byte, 0, 10000)
        ticker := time.Tick(time.Duration(f.FlushInterval) * time.Millisecond)
        inChan := fr.InChan()

        for ok {
                select {  
                case pack, ok = <-inChan:
                        if !ok {
                                // Closed inChan => we're shutting down, flush data
                                if len(outBatch) > 0 {
                                        f.batchChan <- outBatch
                                }
                                close(f.batchChan)
                                break
                        } 
                        f.msgLoopCount = pack.MsgLoopCount

                        if outBytes, e = encoder.Encode(pack); e != nil {
                                fr.LogError(fmt.Errorf("Error encoding message: %s", e))
                        } else {
                            if len(outBytes) > 0 {
                                outBatch = append(outBatch, outBytes...)
                                outBatch = append(outBatch, delimiter...)

                                if len(outBatch) > f.FlushBytes {
                                        f.batchChan <- outBatch
                                        outBatch = <-f.backChan
                                }
                            }
                            outBytes = outBytes[:0]
                        } 
                        pack.Recycle()
                case <-ticker:
                        if len(outBatch) > 0 {
                        f.batchChan <- outBatch
                        outBatch = <-f.backChan
                        } 
                }
        }

        wg.Done()
}

func (f *StreamAggregatorFilter) Run(fr FilterRunner, h PluginHelper) (err error) {
        base_name := f.EncoderName
        full_name := fr.Name() + "-" + f.EncoderName
        encoder, ok := h.Encoder(base_name, full_name)
        if !ok {
            return fmt.Errorf("Encoder not found: %s", full_name)
        }

        var wg sync.WaitGroup
        wg.Add(2)
        go f.receiver(fr, h, encoder, &wg)
        go f.committer(fr, h, &wg)
        wg.Wait()

    return
}

func init() {
    RegisterPlugin("StreamAggregatorFilter", func() interface{} {
        return new(StreamAggregatorFilter)
    })
}
