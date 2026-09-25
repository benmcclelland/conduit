// Copyright 2026. Triad National Security, LLC. All rights reserved.

package scoutam

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/plugin"
	"google.golang.org/protobuf/types/known/anypb"
)

// stageEvent mirrors the message ScoutAM publishes to its configured NATS "stage" notification
// (see `samcli notify nats --stage`) when a requested stage completes (successfully or not).
type stageEvent struct {
	Inode        uint64
	Filename     string
	RequestTime  string
	CompleteTime string
	Error        string
}

// Setup for scoutam - used only when reading data from a Versity/ScoutFS filesystem. Requests
// ScoutAM stage every requested file via the REST API (FTA hosts are not expected to have samcli
// or gRPC access) and waits for stage-completion notifications on a per-request NATS topic before
// returning so the transfer stage can proceed. Files are always staged, even if already online:
// ScoutAM still publishes an (immediate) completion notification for them, so there's no need to
// pre-check online/offline status.
func (p *ScoutAMPlugin) Setup(transferID uuid.UUID, pathInfo *plugin.PluginPathInfo, pathType proto.LeaseType, action string, options map[string]*anypb.Any, baseDest bool, updateTransferProgress plugin.UpdateTransferProgress) (*proto.FTAPluginErrors, *plugin.PluginPathInfo) {
	scoutAMConfig := DefaultScoutAMPluginConfig()
	err := plugin.GetPluginConfigsFromViper(ScoutAMPluginKey, &scoutAMConfig)
	if err != nil {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					LeasePath:  "",
					PErr:       proto.Error_ERROR_INVALID_CONDUIT_CONFIG,
					ErrMessage: fmt.Sprintf("failed to get scoutam config: %v", err),
				},
			},
		}, nil
	}

	// pathInfo.TransferPath tells the transfer plugin what final path to use for its transfer
	pathInfo.TransferPath = pathInfo.ResolvedFTAPath

	// If the file path is not a SOURCE for the transfer -> no need to get the
	// file from tape
	if pathType != proto.LeaseType_SOURCE {
		return &proto.FTAPluginErrors{}, pathInfo
	}

	ds, err := os.Lstat(pathInfo.ResolvedFTAPath)
	if err != nil {
		p.log.Errorf("Bad Source path %s passed to ScoutAM Setup: err = %v", pathInfo.OriginalUserPath, err)
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					LeasePath:  pathInfo.OriginalUserPath,
					PErr:       proto.Error_ERROR_STAT_FAILED,
					ErrMessage: fmt.Sprintf("Bad path (%s) for Archive Stager: err = %v", pathInfo.OriginalUserPath, err),
				},
			},
		}, nil
	}
	ctx := context.Background()
	client := newAPIClient(scoutAMConfig.APIBaseURL, scoutAMConfig.APIInsecureSkipVerify)
	token, err := client.login(ctx, scoutAMConfig.APIUsername, scoutAMConfig.APIPassword)
	if err != nil {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					LeasePath:  pathInfo.OriginalUserPath,
					PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
					ErrMessage: fmt.Sprintf("failed to authenticate to ScoutAM API: %v", err),
				},
			},
		}, nil
	}

	fsid, err := client.resolveFSID(ctx, token, pathInfo.ResolvedFTAPath)
	if err != nil {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					LeasePath:  pathInfo.OriginalUserPath,
					PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
					ErrMessage: fmt.Sprintf("failed to resolve ScoutAM filesystem for %s: %v", pathInfo.OriginalUserPath, err),
				},
			},
		}, nil
	}

	nc, err := nats.Connect(strings.Join(scoutAMConfig.NatsServers, ","))
	if err != nil {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					LeasePath:  pathInfo.OriginalUserPath,
					PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
					ErrMessage: fmt.Sprintf("failed to connect to ScoutAM stage notification bus: %v", err),
				},
			},
		}, nil
	}
	defer nc.Close()

	// Each Setup call gets its own topic (rather than reusing transferID, which is shared by every
	// source/destination path in the transfer) so concurrent Setup calls never see each other's
	// notifications. Every message received on this topic is necessarily one of ours.
	topic := fmt.Sprintf("%s.%s", scoutAMConfig.NatsStageTopicPrefix, uuid.New().String())

	// The queue is unbounded and its push is non-blocking, since notifications for an early batch
	// can arrive (and pile up) while we're still walking the directory and submitting later
	// batches - a blocking/fixed-size channel here could stall the NATS callback or drop messages.
	queue := newStageEventQueue()
	sub, err := nc.Subscribe(topic, func(msg *nats.Msg) {
		var evt stageEvent
		if jsonErr := json.Unmarshal(msg.Data, &evt); jsonErr != nil {
			p.log.Errorf("Failed to parse ScoutAM stage notification: %v", jsonErr)
			return
		}
		queue.push(evt)
	})
	if err != nil {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					LeasePath:  pathInfo.OriginalUserPath,
					PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
					ErrMessage: fmt.Sprintf("failed to subscribe to ScoutAM stage notifications: %v", err),
				},
			},
		}, nil
	}
	defer sub.Unsubscribe()

	// Subscription is active before we request any stage so a fast completion can't be missed.
	// Files are submitted in batches of BatchSize (rather than one request for the whole
	// directory) so ScoutAM can start staging early files while we're still walking a large
	// directory, and so no single request body gets too large. Never loop single-file requests.
	batchSize := scoutAMConfig.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultScoutAMBatchSize
	}

	var total int
	batch := make([]string, 0, batchSize)
	submitBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := client.batchStage(ctx, token, batch, fsid, topic); err != nil {
			return err
		}
		total += len(batch)
		updateTransferProgress(&proto.ETCDStatusDetails{
			PluginStatus: fmt.Sprintf("(setup) requested staging of %d file(s) so far for %v", total, pathInfo.OriginalUserPath),
		})
		batch = batch[:0]
		return nil
	}

	// The scoutfs mount is assumed to be directly reachable from the FTA host, so we can walk it
	// locally to enumerate the candidate files without any ScoutAM API calls.
	err = walkFilePaths(pathInfo.ResolvedFTAPath, ds, func(p string) error {
		batch = append(batch, p)
		if len(batch) >= batchSize {
			return submitBatch()
		}
		return nil
	})
	if err == nil {
		err = submitBatch() // flush any partial final batch
	}
	if err != nil {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					LeasePath:  pathInfo.OriginalUserPath,
					PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
					ErrMessage: fmt.Sprintf("failed to request staging for %s: %v", pathInfo.OriginalUserPath, err),
				},
			},
		}, nil
	}

	// nothing to stage (e.g. an empty directory)
	if total == 0 {
		return &proto.FTAPluginErrors{}, pathInfo
	}

	var stageErrs []string
	timeout := time.After(scoutAMConfig.StageTimeout)
	for remaining := total; remaining > 0; remaining-- {
		evt, ok := queue.pop(timeout)
		if !ok {
			return &proto.FTAPluginErrors{
				Errors: []*proto.FTAPathError{
					{
						LeasePath:  pathInfo.OriginalUserPath,
						PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
						ErrMessage: fmt.Sprintf("timed out after %v waiting on stage notifications for %v (%d of %d files still pending)", scoutAMConfig.StageTimeout, pathInfo.OriginalUserPath, remaining, total),
					},
				},
			}, nil
		}
		if evt.Error != "" {
			stageErrs = append(stageErrs, fmt.Sprintf("%s: %s", evt.Filename, evt.Error))
		}
	}

	if len(stageErrs) > 0 {
		return &proto.FTAPluginErrors{
			Errors: []*proto.FTAPathError{
				{
					LeasePath:  pathInfo.OriginalUserPath,
					PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
					ErrMessage: fmt.Sprintf("ScoutAM reported stage errors: %s", strings.Join(stageErrs, "; ")),
				},
			},
		}, nil
	}

	updateTransferProgress(&proto.ETCDStatusDetails{
		PluginStatus: fmt.Sprintf("(setup) data staging for %v complete", pathInfo.OriginalUserPath),
	})

	return &proto.FTAPluginErrors{}, pathInfo
}

// walkFilePaths calls fn once with path itself if it's a regular file, or once for every regular
// file below path (recursively) if it's a directory. This relies on the scoutfs mount being
// directly reachable from the FTA host rather than any ScoutAM API/CLI directory listing.
func walkFilePaths(path string, ds os.FileInfo, fn func(string) error) error {
	if !ds.IsDir() {
		return fn(path)
	}

	return filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			return fn(p)
		}
		return nil
	})
}
