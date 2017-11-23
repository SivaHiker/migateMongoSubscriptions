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
	"encoding/json"
)

func main(){

	var recordsCount int64
	file, err := os.Open("/home/siva/LatestAppOpenUsers_20170512_to_20171107.txt")
	defer file.Close()

	if err != nil {
		println(err)
	}

	outputfile1, err := os.Create("resultRecords.json")
	if(err!=nil){
		fmt.Println("Not able to create a file")
	}
	defer outputfile1.Close()

	fromSession, err := mgo.Dial("10.9.33.3")
	if err != nil {
		panic(err)
	}
	defer fromSession.Close()

	c := fromSession.DB("subscription").C("channel_subscriptions")
	fmt.Println(c.Name)

	//toSession, err := mgo.Dial("10.15.0.145")
	//if err != nil {
	//	panic(err)
	//}
	//defer toSession.Close()
	//
	//c2 := fromSession.DB("subscription").C("channel_subscriptions")
	//fmt.Println(c.Name)


	// Start reading from the file with a reader.
	reader := bufio.NewReader(file)

	limiter := time.Tick(time.Nanosecond * 100000000)

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
			 usrSubscription := []Subscription{}
			 err := c.Find(bson.M{"user_id": uid}).All(&usrSubscription)
             if(err !=nil){
             	fmt.Println("Not able to query the records")
			 }
             fmt.Println(len(usrSubscription))
             fmt.Println("The overall usersubscriptions",usrSubscription)
			 for _,subs := range usrSubscription {
			 	fmt.Println("individual use subscription",subs)
			 	fmt.Println(subs.PlatformUID)
				 fmt.Println(subs.UserID)
				 mongoJson, err := json.Marshal(subs)
				 if err != nil {
					 fmt.Println(err)
					 return
				 }
				 fmt.Println(string(mongoJson))
				 outputfile1.WriteString(string(mongoJson)+"\n")
				 recordsCount++
				 fmt.Println("Number of records exported from the DB",recordsCount)
			 }
		}
	}
	fmt.Println("Final Number of records exported from the DB",recordsCount)
	if err != io.EOF {
		fmt.Printf(" > Failed!: %v\n", err)
	}

}


type Subscription struct {
	ChannelID int    `json:"channel_id"`
	PlatformUID string `json:"platform_uid"`
	CreatedAt time.Time `json:"created_at"`
	Status    int    `json:"status"`
	TagID     int    `json:"tag_id"`
	TagType   int    `json:"tag_type"`
	UpdatedAt time.Time `json:"updated_at"`
	UserID    string `json:"user_id"`
}

type UserInfo struct {
	UserData UserData `json:"UserData"`
	Flag bool `json:"flag"`
}

type UserData struct {
	Msisdn string `json:"msisdn"`
	Token  string `json:"token"`
	UID    string `json:"uid"`
	PlatformUID string `json:"platformuid"`
	PlatformToken string `json:"platformtoken"`
}