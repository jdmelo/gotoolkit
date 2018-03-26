package main

import (
	"fmt"
	"jd.com/jvirt/jvirt-common/utils/kafka"
	"time"
	log "github.com/Sirupsen/logrus"
	"github.com/Shopify/sarama"
	"encoding/json"
)

var (
	topic = "iaas_jvirt_jcs_instance_change_ag"
)

type InstanceChangeAgMsg struct {
	Region        string   `json:"region"`   // 地域，cn-north-1：华北-北京，cn-east-1：华东-宿迁，cn-east-2：华东-上海，cn-south-1：华南-广州
	Az            string   `json:"az"`       // 可用区
	AppCode       string   `json:"app_code"` // 业务系统使用，常量:jcloud
	UserID        string   `json:"user_id"`
	UserPin       string   `json:"user_pin"`
	Timestamp     int64    `json:"timestamp"`
	ResourceId    string   `json:"resource_id"`
	ResourceType  string   `json:"resource_type"`
	Action        string   `json:"action"` // InstanceRemoveFromAg|InstanceJoinToAg
	AgId          string   `json:"ag_id"`
	ResourcesInAg []string `json:"resources_in_ag"`
}

func ConsumeMsgFunc(msg *sarama.ConsumerMessage) {
	fmt.Println("Key:", string(msg.Key), "Partition:", msg.Partition, "Offset:", msg.Offset)
	value := &InstanceChangeAgMsg{}
	if err := json.Unmarshal(msg.Value, value); err != nil {
		fmt.Printf("Invoke Unmarshal failed. Err: %#v.\n", err)
		return
	}
	fmt.Printf("%#v.\n", value)
}

func main()  {
	log.SetLevel(log.DebugLevel)
	c := &kafka.ProducerConfig{
		Name: "jvirt-jcs",
		Url:  []string{"192.168.170.129:9092", "192.168.178.161:9092", "192.168.178.162:9092"},
	}
	sp, err := kafka.NewKafkaSyncProducer(c)
	if err != nil {
		fmt.Printf("Invoke NewKafkaSyncProducer failed. Err: %#v.\n", err)
	} else {
		fmt.Println("Invoke NewKafkaSyncProducer pass.")
	}
	defer sp.Close()

	//instId := "i-wsxedcrf"
	instId := "2"
	msg := &InstanceChangeAgMsg{
		Region:        "cn-north-1",
		Az:            "az1",
		AppCode:       "Jcloud-iaas-jvirt-jcs",
		UserID:        "1111-2222-3333-4444",
		UserPin:       "succ@jd.com",
		Timestamp:     time.Now().Unix(),
		ResourceId:    instId,
		ResourceType:  "vm",
		Action:        "InstanceRemoveFromAg",
		AgId:          "8",
		ResourcesInAg: []string{"i-qwertyui", "1-asdfghjk"},
	}
	if err := sp.Send(topic, instId, msg); err != nil {
		fmt.Printf("Invoke KafkaSyncProducer send failed. Err: %#v.\n", err)
	} else {
		fmt.Println("Invoke KafkaSyncProducer send pass.")
	}

	c1 := &kafka.ConsumerConfig{
		Name:    "jvirt-jcs-consumer1",
		Url:     []string{"192.168.170.129:9092", "192.168.178.161:9092", "192.168.178.162:9092"},
		Topics:  []string{topic},
		GroupId: "jvirt-jcs-cg",
		//FromOffsets: "Oldest",
	}

	kcc, err := kafka.NewKafkaClusterConsumer(c1)
	if err != nil {
		fmt.Printf("Invoke NewKafkaSyncProducer failed. Err: %#v.\n", err)
	} else {
		fmt.Println("Invoke NewKafkaSyncProducer pass.\n")
	}
	defer kcc.Close()

	kcc.CheckConsumeResult()

	kcc.ListenMsg(ConsumeMsgFunc)

	time.Sleep(5 * time.Second)
}
