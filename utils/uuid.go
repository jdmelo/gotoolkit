package utils

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	mathrandom "math/rand"
	"time"
)

var character = []byte("abcdefghijklmnopqrstuvwxyz0123456789")

var chLen = len(character)

func Uuid() string {
	var uuidLen = 10
	buf := make([]byte, uuidLen, uuidLen)
	max := big.NewInt(int64(chLen))
	for i := 0; i < uuidLen; i++ {
		random, err := rand.Int(rand.Reader, max)
		if err != nil {
			mathrandom.Seed(time.Now().UnixNano())
			buf[i] = character[mathrandom.Intn(chLen)]
			continue
		}
		buf[i] = character[random.Int64()]
	}
	return string(buf)
}

func GenerateUuid(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, Uuid())
}

func GenerateUuid4(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, Uuid4())
}

func String(u []byte) string {
	buf := make([]byte, 36)
	const dash byte = '-'
	hex.Encode(buf[0:8], u[0:4])
	buf[8] = dash
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = dash
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = dash
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = dash
	hex.Encode(buf[24:], u[10:])

	return string(buf)
}

func Uuid4() string {
	var uuidLen = 16
	buf := make([]byte, uuidLen, uuidLen)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	v := byte(4)
	buf[6] = (buf[6] & 0x0f) | (v << 4)
	buf[8] = (buf[8] & 0xbf) | 0x80

	return String(buf)
}
