package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"time"
)

type JssClient struct {
	SecretKey  string
	AccessKey  string
	StorageUrl string
	BucketName string
}

func (t *JssClient) expires() string {
	duration := time.Duration(8) * time.Hour
	date := strings.Replace(time.Now().Add(-duration).Format(time.RFC1123Z), "+0800", "GMT", -1)
	return date
}

func (t *JssClient) generateSign(method string, contentType string, expires string, resource string) string {

	h := hmac.New(sha1.New, []byte(t.SecretKey))
	param := []string{method, "", contentType, expires, resource}
	finalstring := strings.Join(param, "\n")
	io.WriteString(h, finalstring)
	sign := base64.StdEncoding.EncodeToString(h.Sum(nil))

	return "jingdong " + t.AccessKey + ":" + sign
}

func (t *JssClient) generateRequest(method string, filename string, body []byte, contentType string) (request *http.Request, err error) {
	key := t.BucketName + "/" + filename
	finalUrl := t.StorageUrl + key
	// finalUrl := "http://" + t.BucketName + "." + t.StorageUrl[7:] + filename
	request, _ = http.NewRequest(method, finalUrl, bytes.NewReader(body))
	date := t.expires()
	request.Header.Add("Date", date)
	sign := t.generateSign(method, contentType, date, "/"+key)
	if body != nil {
		request.Header.Add("Content-Type", contentType)
		request.Header.Add("Content-length", strconv.Itoa(len(body)))
	}
	request.Header.Add("Authorization", sign)
	request.Header.Add("Connection", "Keep-Alive")
	request.Header.Add("User-Agent", "EBS-JSS-GOWAPPER/1.0.0")

	return
}

func (t *JssClient) processError(request *http.Request, response *http.Response, err error) (errstr string) {
	reqHeader := "request header is :"
	putResult := " request put result:"
	if err != nil {
		putResult += err.Error()
	} else {
		putResult = fmt.Sprintf(putResult+" response code:%v", response.StatusCode)
		if dump, err := httputil.DumpResponse(response, true); err == nil {
			putResult = fmt.Sprintf(putResult+" response body:%v", response.StatusCode, string(dump))
		}
	}
	if dump, err := httputil.DumpRequest(request, false); err == nil {
		reqHeader += string(dump)
	}
	errstr = reqHeader + putResult
	return
}

func (t *JssClient) Init(accessKey, secretKey, storageUrl, bucketName string) {
	t.AccessKey = accessKey
	t.SecretKey = secretKey
	t.StorageUrl = storageUrl
	t.BucketName = bucketName
}

func (t *JssClient) UploadFile(src string, content []byte) (url string, err error) {
	request, _ := t.generateRequest("PUT", src, content, "application/octet-stream")
	client := &http.Client{}
	var resp *http.Response
	resp, err = client.Do(request)
	if resp != nil {
		defer resp.Body.Close()
	}

	if err == nil && resp.StatusCode == http.StatusOK {
		return t.StorageUrl + t.BucketName + "/" + src, nil
	} else {
		err = errors.New("jss upload err:" + t.processError(request, resp, err))
	}

	return
}

func (t *JssClient) DownloadFile(src string) (content []byte, err error) { //src is the key of the file in jss
	request, _ := t.generateRequest("GET", src, nil, "")

	client := &http.Client{}
	var resp *http.Response
	resp, err = client.Do(request)
	if resp != nil {
		defer resp.Body.Close()
	}

	if err == nil && resp.StatusCode == http.StatusOK {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		content = buf.Bytes()
	} else {
		err = errors.New("jss download err:" + t.processError(request, resp, err))
	}

	return
}

func (t *JssClient) DeleteFile(src string) (err error) {
	request, _ := t.generateRequest("DELETE", src, nil, "")

	client := &http.Client{}
	var resp *http.Response
	resp, err = client.Do(request)
	if resp != nil {
		defer resp.Body.Close()
	}

	if err == nil && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent) {
	} else {
		err = errors.New("jss delete err:" + t.processError(request, resp, err))
	}
	return
}

func (t *JssClient) GetFileMd5(src string) (md5 string, err error) {
	request, _ := t.generateRequest("HEAD", src, nil, "")

	client := &http.Client{}
	resp, err := client.Do(request)
	if resp != nil {
		defer resp.Body.Close()
	}

	if err == nil && resp.StatusCode == http.StatusOK {
		md5 = strings.Replace(resp.Header.Get("Etag"), "\"", "", -1)
	} else {
		err = errors.New("jss delete err:" + t.processError(request, resp, err))
	}
	return
}

func main() {
	jss := &JssClient{}
	jss.Init("accessKey", "secretKey", "http://oss-internal.cn-east-1.jcloudcs.com/", "jcs-test-bucket")
	jss.UploadFile("1.txt", []byte("Here is ebs sq gov test...."))
	jss.DeleteFile("1.txt")
	download, err := jss.DownloadFile("1.txt")
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Success")
		fmt.Println(string(download))
	}
}
