package stream_aggregator


import (
        "fmt"
        "time"
        "github.com/mozilla-services/heka/message"
        . "github.com/mozilla-services/heka/pipeline"
        "github.com/pborman/uuid"
        "sync"
)

type StreamAggregatorBatch struct {
	queueCursor string
	count       int64
	batch       []byte
}

type MsgPack struct {
	bytes       []byte
	queueCursor string
}

type StreamAggregatorFilter struct {
        *StreamAggregatorFilterConfig
	sentMessageCount int64
	dropMessageCount int64
	count            int64
	backChan         chan []byte
	recvChan         chan MsgPack
	batchChan        chan StreamAggregatorBatch // Chan to pass completed batches
	outBatch         []byte
	queueCursor      string
	conf             *StreamAggregatorFilterConfig
	fr               FilterRunner
	outputBlock      *RetryHelper
	pConfig          *PipelineConfig
	reportLock       sync.Mutex
	stopChan         chan bool
	flushTicker      *time.Ticker
	encoder		 Encoder
	msgLoopCount 	 uint
}

type StreamAggregatorFilterConfig struct {
        Delimiter          string `toml:"delimiter"` // Delimiter used to append to end of each protobuf for splitting on when decoding later.
                                                     // Defaults to '\n'
        FlushInterval uint32 `toml:"flush_interval"`
        FlushBytes    int    `toml:"flush_bytes"`
	// Number of messages that triggers a flush
	// (default to 10)
	FlushCount int `toml:"flush_count"`
        StreamAggregatorTag       string `toml:"stream_aggregator_tag"`
        EncoderName   string `toml:"encoder"`
	UseBuffering bool `toml:"use_buffering"`
}

func (f *StreamAggregatorFilter) ConfigStruct() interface{} {
        return &StreamAggregatorFilterConfig{
        Delimiter:     "\n",
        FlushInterval: 1000,
        FlushBytes:    10,
	FlushCount:            10,
        StreamAggregatorTag:       "aggregated",
	UseBuffering:          true,
        }
}

func (f *StreamAggregatorFilter) Init(config interface{}) (err error) {
        f.conf = config.(*StreamAggregatorFilterConfig)

	f.batchChan = make(chan StreamAggregatorBatch)
	f.backChan = make(chan []byte, 2)
	f.recvChan = make(chan MsgPack, 100)

        if f.conf.StreamAggregatorTag == "" {
            return fmt.Errorf(`A stream_aggregator_tag value must be specified for the StreamAggregatorTag Field`)
        }

        if f.conf.EncoderName == "" {
            return fmt.Errorf(`An encoder must be specified`)
        }

        return
}

func (f *StreamAggregatorFilter) Prepare(fr FilterRunner, h PluginHelper) error {

	f.fr = fr
	f.pConfig = h.PipelineConfig()
	f.stopChan = fr.StopChan()
        base_name := f.conf.EncoderName
        full_name := fr.Name() + "-" + f.conf.EncoderName

        encoder, ok := h.Encoder(base_name, full_name)
        if !ok {
            return fmt.Errorf("Encoder not found: %s", full_name)
        }
	f.encoder = encoder

	var err error
	f.outputBlock, err = NewRetryHelper(RetryOptions{
		MaxDelay:   "5s",
		MaxRetries: -1,
	})
	if err != nil {
		return fmt.Errorf("can't create retry helper: %s", err.Error())
	}

	f.outBatch = make([]byte, 0, 10000)
	go f.committer(h)


	if f.conf.FlushInterval > 0 {
		d, err := time.ParseDuration(fmt.Sprintf("%dms", f.conf.FlushInterval))
		if err != nil {
			return fmt.Errorf("can't create flush ticker: %s", err.Error())
		}
		f.flushTicker = time.NewTicker(d)
	}
	go f.batchSender()
	return nil

}

func (f *StreamAggregatorFilter) ProcessMessage(pack *PipelinePack) error {
	var (
		outBytes []byte
		err error
	)

	f.msgLoopCount = pack.MsgLoopCount
	
	if len(pack.Message.GetPayload()) > 0 {
		outBytes, err = f.encoder.Encode(pack)
	} 
	if err != nil {
		return fmt.Errorf("can't encode: %s", err)
	}

	if outBytes != nil {
		f.recvChan <- MsgPack{bytes: outBytes, queueCursor: pack.QueueCursor}
	}

	return nil
}

func (f *StreamAggregatorFilter) batchSender() {
	ok := true
        delimiter := f.conf.Delimiter
	for ok {
		select {
		case <-f.stopChan:
			ok = false
			continue
		case pack := <-f.recvChan:
			f.outBatch = append(f.outBatch, pack.bytes...)
			f.outBatch = append(f.outBatch, delimiter...)
			f.queueCursor = pack.queueCursor
			f.count++
			if len(f.outBatch) > 0 && (len(f.outBatch) > f.conf.FlushBytes) {
				f.sendBatch()
			}
		case <-f.flushTicker.C:
			if len(f.outBatch) > 0 {
				f.sendBatch()
			}
		}
	}
}

func (f *StreamAggregatorFilter) sendBatch() {
	b := StreamAggregatorBatch{
		queueCursor: f.queueCursor,
		count:       f.count,
		batch:       f.outBatch,
	}
	f.count = 0
	select {
	case <-f.stopChan:
		return
	case f.batchChan <- b:
	}
	select {
	case <-f.stopChan:
	case f.outBatch = <-f.backChan:
	}
}

func (f *StreamAggregatorFilter) committer(h PluginHelper) {
	f.backChan <- make([]byte, 0, 10000)

	var (
	    b StreamAggregatorBatch
            tag string
	)
	tag = f.conf.StreamAggregatorTag

	ok := true
	for ok {
		select {
		case <-f.stopChan:
			ok = false
			continue
		case b, ok = <-f.batchChan:
			if !ok {
				continue
			}
		}

		pack, _ := h.PipelinePack(f.msgLoopCount)
		tagField, _ := message.NewField("StreamAggregatorTag", tag, "")
                pack.Message.AddField(tagField)
                pack.Message.SetUuid(uuid.NewRandom())
                pack.Message.SetPayload(string(b.batch))

		f.fr.Inject(pack)
		f.fr.UpdateCursor(b.queueCursor)

		b.batch = b.batch[:0]
		f.backChan <- b.batch
	}
}


func (f *StreamAggregatorFilter) CleanUp() {
	if f.flushTicker != nil {
		f.flushTicker.Stop()
	}
}

func init() {
    RegisterPlugin("StreamAggregatorFilter", func() interface{} {
        return new(StreamAggregatorFilter)
    })
}
