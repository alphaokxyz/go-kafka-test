package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Kafka broker 地址
	brokerAddress := "192.168.178.134:9094"
	// Kafka 主题名称
	topic := "my-topic"

	// 创建一个 Kafka 阅读器
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{brokerAddress},
		Topic:     topic,
		Partition: 0, // 分区号
		MinBytes:  10e3,
		MaxBytes:  10e6,
	})

	// 从 Kafka 主题接收消息
	for {
		message, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("Failed to read message:", err)
		}

		fmt.Printf("Received message: Key=%s, Value=%s\n", string(message.Key), string(message.Value))
	}

	// 注意：在实际应用中，需要处理关闭 Kafka 阅读器的逻辑，以便在不再需要时正确地关闭连接。
}
