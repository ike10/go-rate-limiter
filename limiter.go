package main

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/mux"
)

type redisType struct {
	Pool *redis.Pool
}

var (
	redisClient *redisType
	once        sync.Once
)

const (
	// RedisIP is a IP where redis server is hosted
	RedisIP = "0.0.0.0"
)

const (
	// BUCKET time bucket for Key Generation.
	// We will append the minute value to the original key to create time bucket for Keys
	BUCKET = 1 * 60
	// EXPIRY is a time after which keys should expire in redis
	EXPIRY = 5 * 60
	// THRESHOLD is a rate limiting threshold after which e=we should fail the request
	THRESHOLD = 10
)

// GetRedisConn creates a Singleton for redis connection pool and returns Redis connection instance
func GetRedisConn() redis.Conn {
	once.Do(func() {
		redisPool := &redis.Pool{
			MaxActive: 100,
			Dial: func() (redis.Conn, error) {
				rc, err := redis.Dial("tcp", RedisIP+":6379")
				if err != nil {
					fmt.Println("Error connecting to redis:", err.Error())
					return nil, err
				}
				return rc, nil
			},
		}
		redisClient = &redisType{
			Pool: redisPool,
		}
	})
	return redisClient.Pool.Get()
}

// GetKey returns a Key to be stored in Redis.
// It appends the minute value of Unix time stamp to create buckets for Key
func GetKey(IP string) string {
	bucket := time.Now().Unix() / BUCKET
	IP = IP + strconv.FormatInt(bucket, 10)
	return IP
}

// Middle ware to checkif the Threshould per IP is reached
func rateLimitMiddleWare(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn := GetRedisConn()
		defer conn.Close()
		IPAddress := r.Header.Get("X-Real-Ip")
		if IPAddress == "" {
			IPAddress = r.Header.Get("X-Forwarded-For")
		}
		if IPAddress == "" {
			IPAddress = r.RemoteAddr
		}
		IPAddress = GetKey(IPAddress)
		fmt.Println("IP:", IPAddress)
		val, err := redis.Int(conn.Do("GET", IPAddress))
		if err != nil {
			conn.Do("SET", IPAddress, 1)
			conn.Do("EXPIRE", IPAddress, EXPIRY)
		} else {
			if val > THRESHOLD {
				err := errors.New("Max Rate Limiting Reached, Please try after some time")
				w.Write([]byte(err.Error()))
				return
			}
			conn.Do("SET", IPAddress, val+1)
		}
		fmt.Println("IP count:", val)
		next.ServeHTTP(w, r)
	})
}

func rateLimitMiddleWarePost(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		fmt.Println("Calling middle ware Post")
	})
}

func ping(w http.ResponseWriter, r *http.Request) {
	fmt.Println("This is a Test Endpoint")
}

func main() {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/ping", ping)
	router.Use(rateLimitMiddleWarePost)
	router.Use(rateLimitMiddleWare)
	http.ListenAndServe(":8081", router)
}
