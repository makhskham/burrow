package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/makhskham/burrow/pkg/consumer"
	"github.com/makhskham/burrow/pkg/producer"
)

var brokerAddr string

func main() {
	root := &cobra.Command{
		Use:   "burrow-cli",
		Short: "Burrow command-line client",
	}
	root.PersistentFlags().StringVarP(&brokerAddr, "broker", "b", "localhost:9092", "broker address")
	root.AddCommand(produceCmd(), consumeCmd())
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func produceCmd() *cobra.Command {
	var topic string
	var partitionID int32
	var acks int32

	cmd := &cobra.Command{
		Use:   "produce <message>",
		Short: "Produce a message to a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			p, err := producer.New(brokerAddr)
			if err != nil {
				return err
			}
			defer p.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			offset, err := p.Send(ctx, topic, partitionID, producer.AcksMode(acks), []byte(args[0]))
			if err != nil {
				return err
			}
			fmt.Printf("OK offset=%d\n", offset)
			return nil
		},
	}
	cmd.Flags().StringVarP(&topic, "topic", "t", "default", "topic name")
	cmd.Flags().Int32VarP(&partitionID, "partition", "p", 0, "partition ID")
	cmd.Flags().Int32VarP(&acks, "acks", "a", 1, "acks: 0=none 1=leader -1=all")
	return cmd
}

func consumeCmd() *cobra.Command {
	var topic string
	var partitionID int32
	var group string
	var from int64

	cmd := &cobra.Command{
		Use:   "consume",
		Short: "Consume messages from a topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := consumer.New(brokerAddr, group)
			if err != nil {
				return err
			}
			defer c.Close()
			offset := from
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				msgs, _, err := c.Fetch(ctx, topic, partitionID, offset, 1<<20)
				cancel()
				if err != nil {
					return err
				}
				for _, m := range msgs {
					fmt.Printf("[%d] %s\n", m.Offset, string(m.Payload))
					offset = m.Offset + 1
				}
				if len(msgs) == 0 {
					time.Sleep(100 * time.Millisecond)
				}
			}
		},
	}
	cmd.Flags().StringVarP(&topic, "topic", "t", "default", "topic name")
	cmd.Flags().Int32VarP(&partitionID, "partition", "p", 0, "partition ID")
	cmd.Flags().StringVarP(&group, "group", "g", "default", "consumer group")
	cmd.Flags().Int64VarP(&from, "from", "f", 0, "start offset")
	return cmd
}
