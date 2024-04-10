// Copyright 2024 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package uploader

import (
	"context"
	"fmt"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/livekit/egress/pkg/types"
	"github.com/livekit/protocol/logger"
)

type PUTUploader struct {
	endpoint string
	timeout  time.Duration
}

func NewPUTUploader(endpoint string, timeout time.Duration) *PUTUploader {
	return &PUTUploader{
		endpoint: endpoint,
		timeout:  timeout,
	}
}

func (u *PUTUploader) upload(localFilepath, storageFilepath string, outputType types.OutputType) (string, int64, error) {
	file, err := os.Open(localFilepath)
	if err != nil {
		return "", 0, wrap("HTTP", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return "", 0, wrap("HTTP", err)
	}

	contentType := mime.TypeByExtension(filepath.Ext(localFilepath))
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	ctx, cancel := context.WithTimeout(context.Background(), u.timeout)
	defer cancel()

	requestURL := fmt.Sprintf("%s/%s", u.endpoint, storageFilepath)
	req, err := http.NewRequestWithContext(ctx, "PUT", requestURL, file)
	if err != nil {
		return "", 0, wrap("HTTP", err)
	}

	req.Header.Set("Content-Type", contentType)
	req.ContentLength = fileInfo.Size() // Set the content length for the request

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, wrap("HTTP", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", 0, fmt.Errorf("HTTP upload failed with status code: %d", resp.StatusCode)
	}

	logger.Infow("file uploaded successfully", "endpoint", u.endpoint, "path", storageFilepath)
	return requestURL, fileInfo.Size(), nil
}
