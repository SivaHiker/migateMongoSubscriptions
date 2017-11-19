package main

import (
	"os"
	"fmt"
	"bufio"
	"time"
	"bytes"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io"
)

func main(){

	var recordsCount int64
	file, err := os.Open("/home/siva/LatestAppOpenUsers_20170512_to_20171107.txt")
	defer file.Close()

	if err != nil {
		println(err)
	}

	fromSession, err := mgo.Dial("10.15.0.145")
	if err != nil {
		panic(err)
	}
	defer fromSession.Close()

	c := fromSession.DB("subscription").C("channel_subscriptions")
	fmt.Println(c.Name)


	toSession, err := mgo.Dial("10.15.0.145")
	if err != nil {
		panic(err)
	}
	defer toSession.Close()

	c2 := fromSession.DB("subscription").C("channel_subscriptions")
	fmt.Println(c.Name)


	// Start reading from the file with a reader.
	reader := bufio.NewReader(file)

	limiter := time.Tick(time.Nanosecond * 333333333)

	for {
		var buffer bytes.Buffer
		var line []byte
		line, _, err = reader.ReadLine()
		buffer.Write(line)
		println(buffer.String())
		// If we're just at the EOF, break
		if err != nil {
			break
          } else {
          	 uidString := string(line[:])
          	 uid :=uidString[0:16]
			 <-limiter
			 usrSubscription := Subscription{}
			 collection := c.Find(bson.M{"user_id": uid}).One(&usrSubscription)
			 err:=c2.Insert(collection)
			 if(err!=nil){
			 	fmt.Println("Not able to insert the records")
          	} else {
				 recordsCount++
			 }
			fmt.Println("Number of records exported from the DB",recordsCount)
		}

		fmt.Println("Final Number of records exported from the DB",recordsCount)

	}

	if err != io.EOF {
		fmt.Printf(" > Failed!: %v\n", err)
	}

}

type UserInfo struct {
	HttpUserData UserHTTPData `json:"UserData"`
	HttpFlag bool `json:"flag"`
}


type UserHTTPData struct {
	EncryptedToken string `json:"encrypted_token"`
	Msisdn         string `json:"msisdn"`
	PlatformToken  string `json:"platform_token"`
	PlatformUID    string `json:"platform_uid"`
	PubKey         string `json:"pub_key"`
	RsaKey         string `json:"rsa_key"`
	Token          string `json:"token"`
	UID            string `json:"uid"`
	UUID           string `json:"uuid"`
}

type Subscription struct {
	ChannelID int    `json:"channel_id"`
	CreatedAt string `json:"created_at"`
	Status    int    `json:"status"`
	TagID     int    `json:"tag_id"`
	TagType   int    `json:"tag_type"`
	UpdatedAt string `json:"updated_at"`
	UserID    string `json:"user_id"`
}