// Copyright © 2021 Joel Baranick <jbaranick@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// 	  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"crypto/rand"
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/kadaan/mqtt_sub/version"
	"github.com/spf13/cobra"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	rootCmd = &cobra.Command{
		Use:          "mqtt_sub [flags]",
		Short:        "mqtt_sub is a simple MQTT version 5/3.1.1 client that will subscribe to topics and print the messages that it receives.",
		Example:      `mqtt_sub -h localhost -p 1883 -t test/events -u test_user -P @/run/secrets/TEST_EVENTS_PASSWORD -W 2 -C 1`,
		PreRun:       pre,
		RunE:         run,
		SilenceUsage: true,
	}
	c = &Config{}
)

type Config struct {
	Host         string
	Port         uint16
	Topic        string
	Username     string
	PasswordFile string
	ClientId     string
	Timeout      uint32
	Count        uint32
	Reconnect    bool
	PrintVersion bool
	Verbose      int
}

type MQTTClient struct {
	Client              MQTT.Client
	Opts                *MQTT.ClientOptions
	Topic               string
	lock                *sync.Mutex // use for shutdown
	messageCount        uint32
	messageCountChannel chan bool
	timeoutChannel      <-chan time.Time
	maxMessageCount     uint32
	stopped             bool
}

func (m *MQTTClient) connect() (MQTT.Client, error) {
	if !m.stopped {
		m.Client = MQTT.NewClient(m.Opts)
		if token := m.Client.Connect(); token.Wait() && token.Error() != nil {
			return nil, token.Error()
		}
		return m.Client, nil
	}
	return nil, nil
}

func (m *MQTTClient) shutdown() {
	m.stopped = true
	if m.Client != nil {
		m.lock.Lock()
		defer m.lock.Unlock()
		if m.Client != nil {
			token := m.Client.Unsubscribe(c.Topic)
			token.Wait()
			if token.Error() != nil {
				MQTT.CRITICAL.Printf("Failed to unsubscribe to %s: %s", m.Topic, token.Error())
			}
			m.Client.Disconnect(100)
			m.Client = nil
		}
	}
}

func (m *MQTTClient) subscribeOnConnect(client MQTT.Client) {
	if !m.stopped {
		var qos byte = 0
		if c.Reconnect {
			qos = 1
		}
		token := client.Subscribe(m.Topic, qos, m.onMessageReceived)
		token.Wait()
		if token.Error() != nil {
			MQTT.CRITICAL.Printf("Failed to subscribe to %s: %s", m.Topic, token.Error())
			os.Exit(1)
		}
	}
}

func (m *MQTTClient) connectionLost(_ MQTT.Client, reason error) {
	MQTT.WARN.Printf("Connection Lost: %s\n", reason)
	if !c.Reconnect {
		os.Exit(1)
	}
}

func (m *MQTTClient) onMessageReceived(_ MQTT.Client, message MQTT.Message) {
	if !m.stopped {
		if m.maxMessageCount > 0 {
			m.messageCount += 1
			if m.messageCount >= m.maxMessageCount {
				m.Client.Unsubscribe(m.Topic)
				m.messageCountChannel <- true
			}
		}
		fmt.Println(string(message.Payload()))
	}
}

func init() {
	rootCmd.SetVersionTemplate(version.Print())
	rootCmd.Flags().StringVar(&c.Host, "host", "localhost", "Specify the host to connect to.")
	_ = rootCmd.MarkFlagRequired("host")
	rootCmd.Flags().Uint16Var(&c.Port, "port", 1883, "Connect to the port specified.")
	rootCmd.Flags().StringVar(&c.Topic, "topic", "", "The MQTT topic to subscribe to.")
	rootCmd.Flags().StringVar(&c.Username, "username", "", "Provide a username to be used for authenticating with the broker.")
	rootCmd.Flags().StringVar(&c.PasswordFile, "password-file", "", "Provide a path to a file containing a password to be used for authenticating with the broker.")
	_ = rootCmd.MarkFlagFilename("password-file")
	rootCmd.Flags().StringVar(&c.ClientId, "client-id", "", "The client id for the subscription.")
	rootCmd.Flags().Uint32Var(&c.Timeout, "timeout", 0, "Provide a timeout as an integer number of seconds. mqtt_sub will stop processing messages and disconnect after this number of seconds has passed.")
	rootCmd.Flags().Uint32Var(&c.Count, "count", 0, "Disconnect and exit the program immediately after the given count of messages have been received.")
	rootCmd.Flags().CountVarP(&c.Verbose, "verbose", "v", "Enable verbose logging. Can be specified multiple times.")
	rootCmd.Flags().BoolVar(&c.Reconnect, "reconnect", true, "Reconnect on connection loss")
	rootCmd.Flags().BoolVar(&c.PrintVersion, "version", false, "Print version")
}

func pre(_ *cobra.Command, _ []string) {
	if c.PrintVersion {
		_, _ = fmt.Fprintf(os.Stdout, "%s\n", version.Print())
		os.Exit(0)
	}
}

func run(_ *cobra.Command, _ []string) error {
	MQTT.CRITICAL = log.New(os.Stderr, "[CRIT] ", 0)
	if c.Verbose > 0 {
		MQTT.ERROR = log.New(os.Stderr, "[ERROR] ", 0)
	}
	if c.Verbose > 1 {
		MQTT.WARN = log.New(os.Stderr, "[WARN] ", 0)
	}
	if c.Verbose > 2 {
		MQTT.DEBUG = log.New(os.Stderr, "[DEBUG] ", 0)
	}

	opts := MQTT.NewClientOptions()
	opts.SetCleanSession(false)
	if len(c.ClientId) > 0 {
		opts.SetClientID(c.ClientId)
	} else {
		opts.SetClientID(getRandomClientId())
	}
	opts.SetKeepAlive(time.Second * 60)
	if c.Reconnect {
		opts.SetConnectRetry(true)
		opts.SetAutoReconnect(true)
		opts.SetConnectRetryInterval(time.Millisecond * 250)
		opts.SetMaxReconnectInterval(time.Second * 30)
	} else {
		opts.SetAutoReconnect(false)
	}
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", c.Host, c.Port))
	if len(c.Username) > 0 {
		opts.SetUsername(c.Username)
	}
	if len(c.PasswordFile) > 0 {
		if content, err := os.ReadFile(c.PasswordFile); err != nil {
			return err
		} else {
			opts.SetPassword(string(content))
		}
	}

	client := &MQTTClient{Opts: opts}
	client.lock = new(sync.Mutex)
	client.Topic = c.Topic

	if c.Count > 0 {
		client.maxMessageCount = c.Count
		client.messageCountChannel = make(chan bool, 1)
		defer close(client.messageCountChannel)
	}

	opts.SetOrderMatters(false)
	opts.SetOnConnectHandler(client.subscribeOnConnect)
	opts.SetConnectionLostHandler(client.connectionLost)

	mqttClient, err := client.connect()
	if err != nil {
		return err
	}

	defer func() {
		if mqttClient != nil {
			client.shutdown()
		}
	}()

	if c.Timeout > 0 {
		client.timeoutChannel = time.After(time.Duration(c.Timeout) * time.Second)
	}

	ctrlCChannel := make(chan os.Signal)
	defer close(ctrlCChannel)
	signal.Notify(ctrlCChannel, os.Interrupt, syscall.SIGTERM)
	select {
	case <-ctrlCChannel:
	case <-client.timeoutChannel:
	case <-client.messageCountChannel:
		client.shutdown()
		break
	}

	return nil
}

func getRandomClientId() string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, 14)
	_, _ = rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return "mqtt_sub-" + string(bytes)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
