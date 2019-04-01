package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

// listenPubSubChannels listens for messages on Redis pubsub channels. The
// onStart function is called after the channels are subscribed. The onMessage
// function is called for each message.
func listenPubSubChannels(ctx context.Context, redisServerAddr string,
	onStart func() error,
	onMessage func(channel string, data []byte) error,
	channels ...string) error {
	// A ping is set to the server with this period to test for the health of
	// the connection and server.
	const healthCheckPeriod = time.Minute

	// c, err := redis.Dial("tcp", redisServerAddr,
	// 	// Read timeout on server should be greater than ping period.
	// 	redis.DialReadTimeout(healthCheckPeriod+10*time.Second),
	// 	redis.DialWriteTimeout(10*time.Second))
	// if err != nil {
	// 	return err
	// }
	// defer c.Close()

	conn := newPool().Get()
	defer conn.Close()
	psc := redis.PubSubConn{Conn: conn}

	if err := psc.Subscribe(redis.Args{}.AddFlat(channels)...); err != nil {
		return err
	}

	done := make(chan error, 1)

	// Start a goroutine to receive notifications from the server.
	go func() {
		for {
			switch n := psc.Receive().(type) {
			case error:
				done <- n
				return
			case redis.Message:
				if err := onMessage(n.Channel, n.Data); err != nil {
					done <- err
					return
				}
			case redis.Subscription:
				switch n.Count {
				case len(channels):
					// Notify application when all channels are subscribed.
					if err := onStart(); err != nil {
						done <- err
						return
					}
				case 0:
					// Return from the goroutine when all channels are unsubscribed.
					done <- nil
					return
				}
			}
		}
	}()

	ticker := time.NewTicker(healthCheckPeriod)
	defer ticker.Stop()
	var err error
loop:
	for err == nil {
		select {
		case <-ticker.C:
			// Send ping to test health of connection and server. If
			// corresponding pong is not received, then receive on the
			// connection will timeout and the receive goroutine will exit.
			if err = psc.Ping(""); err != nil {
				break loop
			}
		case <-ctx.Done():
			break loop
		case err := <-done:
			// Return error from the receive goroutine.
			return err
		}
	}

	// Signal the receiving goroutine to exit by unsubscribing from all channels.
	psc.Unsubscribe()

	// Wait for goroutine to complete.
	return <-done
}

func publish() {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.Close()

	c.Do("PUBLISH", "c1", "{message: 'hello'}")
	c.Do("PUBLISH", "c2", "world")
	c.Do("PUBLISH", "c1", "goodbye")
}

func newPool() *redis.Pool {
	return &redis.Pool{
		// Maximum number of idle connections in the pool.
		MaxIdle: 80,
		// max number of connections
		MaxActive: 12000,
		// Dial is an application supplied function for creating and
		// configuring a connection.
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				panic(err.Error())
			}
			return c, err
		},
	}
}

// User user information
type User struct {
	Username  string `json:"username"`
	MobileID  int    `json:"mobile_id"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

func setStruct(c redis.Conn, key string, user User) error {
	// serialize User object to JSON
	json, err := json.Marshal(user)
	if err != nil {
		return err
	}

	// SET object
	_, err = c.Do("SET", key, json)
	if err != nil {
		return err
	}

	return nil
}

func getStruct(c redis.Conn, key string) (*User, error) {
	s, err := redis.String(c.Do("GET", key))
	if err == redis.ErrNil {
		fmt.Println("User does not exist")
	} else if err != nil {
		return nil, err
	}
	usr := User{}
	err = json.Unmarshal([]byte(s), &usr)

	return &usr, nil
}

// This example shows how receive pubsub notifications with cancelation and
// health checks.
func main() {
	usr := User{
		Username:  "otto",
		MobileID:  1234567890,
		Email:     "ottoM@repoman.com",
		FirstName: "Otto",
		LastName:  "Maddox",
	}
	conn := newPool().Get()
	if err := setStruct(conn, "user_"+usr.Username, usr); err != nil {
		fmt.Println(err)
	}
	user, err := getStruct(conn, "user_otto")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%+v\n", *user)

	// PubSub demo
	ctx, cancel := context.WithCancel(context.Background())
	err = listenPubSubChannels(ctx,
		":6379",
		func() error {
			// The start callback is a good place to backfill missed
			// notifications. For the purpose of this example, a goroutine is
			// started to send notifications.
			fmt.Println("subsribing channels succesfully!")
			go publish()
			return nil
		},
		func(channel string, message []byte) error {
			fmt.Printf("channel: %s, received message: %s\n", channel, message)

			// For the purpose of this example, cancel the listener's context
			// after receiving last message sent by publish().
			if string(message) == "goodbye" {
				cancel()
			}
			return nil
		},
		"c1", "c2")

	if err != nil {
		fmt.Println(err)
		return
	}

}
