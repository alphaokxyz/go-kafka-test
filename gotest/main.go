package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Kafka broker 地址
	brokerAddress := "192.168.178.134:9094"
	// Kafka 主题名称
	topic := "my-topic"

	// 创建一个 Kafka 写入器
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
	})

	// 发送消息到 Kafka 主题
	message := kafka.Message{
		Key:   []byte("Key-1"),
		Value: []byte("Hell, World!"), // 将消息的值从 "hello world" 改为 "hell world"
	}

	err := w.WriteMessages(context.Background(), message)
	if err != nil {
		log.Fatal("Failed to write message:", err)
	}

	log.Println("Message sent successfully!")

	// 关闭 Kafka 写入器
	if err := w.Close(); err != nil {
		log.Fatal("Failed to close writer:", err)
	}
}
