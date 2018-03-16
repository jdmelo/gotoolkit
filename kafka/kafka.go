package kafka

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/bsm/sarama-cluster"
)

type ConsumerConfig struct {
	Name           string        //客户端名称，用于监控查看问题
	Url            []string      //可多个，逗号分隔
	Topics         []string      //可多个，逗号分隔
	GroupId        string        //消费组
	FromOffsets    string        //消费配置：偏移量，支持（Newest，Oldest）二种，默认使用Oldest
	CommitInterval time.Duration //消费配置：多久提交一次偏移量，默认1秒一次
}

type KafkaClusterConsumer struct {
	running  bool
	wg       sync.WaitGroup
	mu       sync.Mutex
	groupId  string
	consumer *cluster.Consumer // 消费消息
	done     chan struct{}
}

// 实例化消费者
func NewKafkaClusterConsumer(cfg *ConsumerConfig) (*KafkaClusterConsumer, error) {
	if cfg.GroupId == "" {
		log.Error("group id not configured")
		return nil, errors.New("group id not configured")
	}

	config := cluster.NewConfig()
	config.ClientID = cfg.Name
	config.Group.Return.Notifications = true
	if cfg.CommitInterval > 0 {
		config.Consumer.Offsets.CommitInterval = cfg.CommitInterval
	}
	if cfg.FromOffsets == "Newest" {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	consumer, err := cluster.NewConsumer(cfg.Url, cfg.GroupId, cfg.Topics, config)
	if err != nil {
		log.Errorf("Invoke NewConsumer failed. Error: %#v.", err)
		return nil, err
	}

	return &KafkaClusterConsumer{
		groupId:  cfg.Name,
		consumer: consumer,
		done:     make(chan struct{}),
	}, nil
}

func (kcc *KafkaClusterConsumer) CheckConsumeResult() {
	kcc.wg.Add(1)

	go func() {
		defer kcc.wg.Done()

		for {
			select {
			case <-kcc.done:
				log.Info("KafkaClusterConsumer stop, CheckConsumeResult exit.")
				return
			case <-kcc.consumer.Notifications():
				log.Debug("KafkaClusterConsumer consume success.")
			case e := <-kcc.consumer.Errors():
				log.Errorf("KafkaClusterConsumer consume failed. Err: %#v.", e)
			}
		}
	}()
}

func (kcc *KafkaClusterConsumer) Close() error {
	close(kcc.done)
	kcc.wg.Wait()
	err := kcc.consumer.Close()
	kcc.running = false

	return err
}

// 接收消息
func (kcc *KafkaClusterConsumer) ListenMsg(consumeFunc func(m *sarama.ConsumerMessage)) {
	kcc.mu.Lock()
	defer kcc.mu.Unlock()
	if kcc.running {
		return
	}
	kcc.running = true

	// 监听消息
	kcc.wg.Add(1)
	go func() {
		defer kcc.wg.Done()

		for {
			select {
			case <-kcc.done:
				log.Info("KafkaClusterConsumer stop, Listen exit.")
				return
			case msg := <-kcc.consumer.Messages():
				kcc.consumer.MarkOffset(msg, "") // MarkOffset 并不是实时写入kafka，有可能在程序crash时丢掉未提交的offset
				// 消费消息.
				log.Debugf("KafkaClusterConsumer ConsumeMsg: GroupID: %s, Topic: %s, Partition: %v, Offset: %v, Timestamp: %v",
					kcc.groupId, msg.Topic, msg.Partition, msg.Offset, msg.Timestamp)
				if consumeFunc != nil {
					consumeFunc(msg)
				}
			}
		}
	}()
}

type ProducerConfig struct {
	Name       string        //客户端名称，用于监控查看问题
	Url        []string      //可多个，逗号分隔
	AckRule    string        //发送配置：ack规则（NoResponse、WaitForLocal、WaitForAll）默认为WaitForLocal
	AckTimeout time.Duration //发送配置：等待Ack最大时间，默认10秒
}

type KafkaSyncProducer struct {
	sp sarama.SyncProducer
}

// 实例化生产者
func NewKafkaSyncProducer(cfg *ProducerConfig) (*KafkaSyncProducer, error) {
	config := sarama.NewConfig()
	config.ClientID = cfg.Name
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 100 * time.Millisecond

	switch cfg.AckRule {
	case "WaitForAll":
		config.Producer.RequiredAcks = sarama.WaitForAll
	case "NoResponse":
		config.Producer.RequiredAcks = sarama.NoResponse
	default:
		config.Producer.RequiredAcks = sarama.WaitForLocal
	}
	if cfg.AckTimeout > 0 {
		config.Producer.Timeout = cfg.AckTimeout
	}
	producer, err := sarama.NewSyncProducer(cfg.Url, config)
	if err != nil {
		log.Errorf("Invoke NewSyncProducer failed. Error: %#v.", err)
		return nil, err
	}

	return &KafkaSyncProducer{
		sp: producer,
	}, nil
}

func (ksp *KafkaSyncProducer) Close() error {
	if err := ksp.sp.Close(); err != nil {
		log.Errorf("Invoke SyncProducer Close failed. Error: %#v.", err)
		return err
	}

	return nil
}

// 同步发送，重试3次
func (ksp *KafkaSyncProducer) Send(topic, key string, value interface{}) error {
	v, err := json.Marshal(value)
	if err != nil {
		log.Errorf("Invoke json marshal failed. Err: %#v.", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(v),
	}
	p, offset, err := ksp.sp.SendMessage(msg)
	if err != nil {
		log.Errorf("KafkaSyncProducer SendMessage failed. Error: %#v.", err)
		return err
	}
	log.Debugf("KafkaSyncProducer send msg success. Partition: %v, Offset: %v.", p, offset)

	return nil
}

type KafkaAsyncProducer struct {
	asp  sarama.AsyncProducer
	done chan struct{}
	wg   sync.WaitGroup
}

func NewKafkaAsyncProducer(cfg *ProducerConfig) (*KafkaAsyncProducer, error) {
	config := sarama.NewConfig()

	config.ClientID = cfg.Name
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 100 * time.Millisecond

	switch cfg.AckRule {
	case "WaitForAll":
		config.Producer.RequiredAcks = sarama.WaitForAll
	case "NoResponse":
		config.Producer.RequiredAcks = sarama.NoResponse
	default:
		config.Producer.RequiredAcks = sarama.WaitForLocal
	}
	if cfg.AckTimeout > 0 {
		config.Producer.Timeout = cfg.AckTimeout
	}
	producer, err := sarama.NewAsyncProducer(cfg.Url, config)
	if err != nil {
		log.Errorf("Invoke NewAsyncProducer failed. Error: %#v.", err)
		return nil, err
	}

	return &KafkaAsyncProducer{
		asp:  producer,
		done: make(chan struct{}),
	}, nil
}

func (kap *KafkaAsyncProducer) Close() error {
	close(kap.done)
	kap.wg.Wait()
	err := kap.asp.Close()

	return err
}

func (kap *KafkaAsyncProducer) CheckProduceResult() {
	kap.wg.Add(1)

	go func() {
		defer kap.wg.Done()

		for {
			select {
			case <-kap.done:
				log.Info("KafkaAsyncProducer stop, CheckProduceResult Exit")
				return
			case <-kap.asp.Successes():
				log.Debug("KafkaAsyncProducer produce msg success.")
			case e := <-kap.asp.Errors():
				log.Errorf("KafkaAsyncProducer produce msg failed. Err: %#v.", e)
			}
		}
	}()

}

// 异步发送
func (kap *KafkaAsyncProducer) AsyncSend(topic, key string, value interface{}) error {
	v, err := json.Marshal(value)
	if err != nil {
		log.Errorf("Invoke json marshal failed. Err: %#v.", err)
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(v),
	}

	// 是否设置超时.
	kap.asp.Input() <- msg
	log.Debug("KafkaAsyncProducer send msg success.")

	return nil
}
