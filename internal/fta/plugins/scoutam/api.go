// Copyright 2026. Triad National Security, LLC. All rights reserved.

package scoutam

import (
	"bytes"
	"cmp"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

// apiClient is a minimal client for the ScoutAM REST API (see /v1/security/login, /v1/batchfile,
// /v1/request/batchstage, /v1/filesystems). FTA hosts are not expected to have samcli or direct
// gRPC access to ScoutAM, but they can reach the REST API which fronts the same operations.
type apiClient struct {
	baseURL    string
	httpClient *http.Client
}

func newAPIClient(baseURL string, insecureSkipVerify bool) *apiClient {
	return &apiClient{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: insecureSkipVerify}, // #nosec G402 -- ScoutAM test/internal deployments commonly use self-signed certs; operators opt in via config
			},
		},
	}
}

// login exchanges an account/password for a bearer token (see `samcli config auth`).
func (c *apiClient) login(ctx context.Context, username, password string) (string, error) {
	reqBody := struct {
		Acct string `json:"acct"`
		Pass string `json:"pass"`
	}{Acct: username, Pass: password}

	var respBody struct {
		Response string `json:"response"`
	}
	if err := c.do(ctx, http.MethodPost, "/v1/security/login", "", reqBody, &respBody); err != nil {
		return "", err
	}
	return respBody.Response, nil
}

type filesystemInfo struct {
	FSID    string `json:"fsid"`
	Mount   string `json:"mount"`
	Mounted bool   `json:"mounted"`
}

// filesystems lists ScoutFS filesystems ScoutAM knows about, along with their mount points.
func (c *apiClient) filesystems(ctx context.Context, token string) ([]filesystemInfo, error) {
	var respBody struct {
		FSIDs []filesystemInfo `json:"fsids"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/filesystems", token, nil, &respBody); err != nil {
		return nil, err
	}
	return respBody.FSIDs, nil
}

// resolveFilesystem finds the ScoutAM filesystem mounted at (or above) path.
func (c *apiClient) resolveFilesystem(ctx context.Context, token string, path string) (filesystemInfo, error) {
	fsids, err := c.filesystems(ctx, token)
	if err != nil {
		return filesystemInfo{}, fmt.Errorf("failed to list ScoutAM filesystems: %v", err)
	}

	var bestMatch filesystemInfo
	for _, fsid := range fsids {
		if !pathIsWithin(fsid.Mount, path) {
			continue
		}
		if len(fsid.Mount) > len(bestMatch.Mount) {
			bestMatch = fsid
		}
	}
	if bestMatch.FSID == "" {
		return filesystemInfo{}, fmt.Errorf("no ScoutAM filesystem mount found for path %v", path)
	}
	return bestMatch, nil
}

// serverAPIPath translates an FTA-local path into the absolute server-side path ScoutAM accepts.
// apiRoot is the server-side ScoutFS directory represented by ftaRoot on the FTA host.
func serverAPIPath(ftaPath string, ftaRoot string, apiRoot string) (string, error) {
	localRelative, err := filepath.Rel(ftaRoot, ftaPath)
	if err != nil || !pathIsWithin(ftaRoot, ftaPath) {
		return "", fmt.Errorf("FTA path %q is outside configured FTA root %q", ftaPath, ftaRoot)
	}

	return filepath.Join(apiRoot, localRelative), nil
}

func pathIsWithin(root string, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

// batchStage requests ScoutAM stage the given paths (online or offline; ScoutAM notifies for both),
// publishing completion notifications to topic.
func (c *apiClient) batchStage(ctx context.Context, token string, paths []string, fsid string, topic string) error {
	reqBody := struct {
		Path  []string `json:"path"`
		FSID  string   `json:"fsid"`
		Topic string   `json:"topic"`
	}{Path: paths, FSID: fsid, Topic: topic}

	return c.do(ctx, http.MethodPut, "/v1/request/batchstage", token, reqBody, nil)
}

// cancelStage drops pending stage requests for paths. Files whose stage job is already running
// can't be cancelled and will still come online.
func (c *apiClient) cancelStage(ctx context.Context, token string, paths []string, fsid string) error {
	reqBody := struct {
		Filenames []string `json:"filenames"`
		FSID      string   `json:"fsid"`
	}{Filenames: paths, FSID: fsid}

	return c.do(ctx, http.MethodPut, "/v1/scheduler/stagecancelfiles", token, reqBody, nil)
}

// cancelStages cancels paths in batches. ScoutAM stops a batch at its first bad file, so a failed
// batch is retried one file at a time.
func (c *apiClient) cancelStages(ctx context.Context, token string, paths []string, fsid string, batchSize int) (failed int, firstErr error) {
	for start := 0; start < len(paths); start += batchSize {
		batch := paths[start:min(start+batchSize, len(paths))]
		err := c.cancelStage(ctx, token, batch, fsid)
		if err == nil {
			continue
		}
		if len(batch) == 1 {
			failed++
			firstErr = cmp.Or(firstErr, err)
			continue
		}
		for _, path := range batch {
			if err := c.cancelStage(ctx, token, []string{path}, fsid); err != nil {
				failed++
				firstErr = cmp.Or(firstErr, err)
			}
		}
	}
	return failed, firstErr
}

func (c *apiClient) do(ctx context.Context, method string, path string, token string, reqBody any, respBody any) error {
	var bodyReader io.Reader
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %v", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request to %v %v failed: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response from %v %v: %v", method, path, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%v %v returned status %v: %s", method, path, resp.StatusCode, respData)
	}

	if respBody == nil || len(respData) == 0 {
		return nil
	}
	if err := json.Unmarshal(respData, respBody); err != nil {
		return fmt.Errorf("failed to parse response from %v %v: %v", method, path, err)
	}
	return nil
}
