// Copyright 2026. Triad National Security, LLC. All rights reserved.

package scoutam

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/plugin"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	maxReportedErrors = 20
	maxRsyncOutput    = 4096
)

var rsyncPreserveArgs = []string{"--links", "--perms", "--times", "--group", "--owner", "--specials"}

// copyGroup is one transfer source and where it lands on the destination.
type copyGroup struct {
	userPath string
	srcRoot  string
	destRoot string
	isDir    bool
	// stage is false for sources on filesystems that don't list scoutam in transfer-src
	stage bool
	fsc   *plugin.FileSystemConfig
}

type stager struct {
	client      *apiClient
	token       string
	topic       string
	filesystems map[string]filesystemInfo // keyed by apiRoot
}

func (s *stager) filesystem(ctx context.Context, apiRoot string) (filesystemInfo, error) {
	if fsInfo, ok := s.filesystems[apiRoot]; ok {
		return fsInfo, nil
	}
	fsInfo, err := s.client.resolveFilesystem(ctx, s.token, apiRoot)
	if err != nil {
		return filesystemInfo{}, fmt.Errorf("failed to resolve ScoutAM filesystem: %w", err)
	}
	s.filesystems[apiRoot] = fsInfo
	return fsInfo, nil
}

// Transfer stages and copies in a pipeline: while sources are walked, files are requested for
// staging in batches, and each file is copied with rsync as soon as ScoutAM reports it staged
// instead of waiting for the whole source to come online. Use with setup-src: posix so Setup
// doesn't stage everything up front.
func (p *ScoutAMPlugin) Transfer(transferID uuid.UUID, pluginData *plugin.PluginData, destInfo proto.DestInfo, action string, options map[string]*anypb.Any, updateTransferProgress plugin.UpdateTransferProgress, updateAction plugin.UpdateAction) *proto.FTAPluginErrors {
	cfg := DefaultScoutAMPluginConfig()
	if err := plugin.GetPluginConfigsFromViper(ScoutAMPluginKey, &cfg); err != nil {
		return transferError(proto.Error_ERROR_INVALID_CONDUIT_CONFIG, fmt.Sprintf("failed to get scoutam config: %v", err))
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultScoutAMBatchSize
	}

	groups, pathErr := buildCopyGroups(pluginData, destInfo)
	if pathErr != nil {
		return &proto.FTAPluginErrors{Errors: []*proto.FTAPathError{pathErr}}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pl := newStagePipeline()

	var st *stager
	if slices.ContainsFunc(groups, func(g copyGroup) bool { return g.stage }) {
		client := newAPIClient(cfg.APIBaseURL, cfg.APIInsecureSkipVerify)
		token, err := client.login(ctx, cfg.APIUsername, cfg.APIPassword)
		if err != nil {
			return transferError(proto.Error_ERROR_FTA_PLUGIN_FAILED, fmt.Sprintf("failed to authenticate to ScoutAM API: %v", err))
		}

		nc, err := nats.Connect(strings.Join(cfg.NatsServers, ","))
		if err != nil {
			return transferError(proto.Error_ERROR_FTA_PLUGIN_FAILED, fmt.Sprintf("failed to connect to ScoutAM stage notification bus: %v", err))
		}
		defer nc.Close()

		topic := fmt.Sprintf("%s.%s", cfg.NatsStageTopicPrefix, uuid.New().String())
		sub, err := nc.Subscribe(topic, func(msg *nats.Msg) {
			var evt stageEvent
			if jsonErr := json.Unmarshal(msg.Data, &evt); jsonErr != nil {
				p.log.Errorf("Failed to parse ScoutAM stage notification: %v", jsonErr)
				return
			}
			if !pl.onStageEvent(evt) {
				p.log.Warnf("Ignoring ScoutAM stage notification for unrequested file %q", evt.Filename)
			}
		})
		if err != nil {
			return transferError(proto.Error_ERROR_FTA_PLUGIN_FAILED, fmt.Sprintf("failed to subscribe to ScoutAM stage notifications: %v", err))
		}
		defer sub.Unsubscribe()

		st = &stager{client: client, token: token, topic: topic, filesystems: make(map[string]filesystemInfo)}
	}

	go func() {
		pl.finishWalk(walkSources(ctx, groups, st, pl, batchSize, updateTransferProgress))
	}()

	var copyErrs []string
	var files uint32
	var copiedBytes int64
	for {
		batch, done, err := pl.next(batchSize, cfg.StageTimeout)
		if err != nil {
			return transferError(proto.Error_ERROR_FTA_PLUGIN_FAILED, err.Error())
		}
		if done {
			break
		}

		byGroup := make(map[int][]copyItem)
		for _, item := range batch {
			byGroup[item.group] = append(byGroup[item.group], item)
		}
		for _, gi := range slices.Sorted(maps.Keys(byGroup)) {
			items := byGroup[gi]
			if err := rsyncItems(ctx, cfg.RsyncPath, groups[gi], items); err != nil {
				copyErrs = append(copyErrs, fmt.Sprintf("%s: %v", groups[gi].userPath, err))
				continue
			}
			for _, item := range items {
				if !item.isDir {
					files++
					copiedBytes += item.size
				}
			}
		}

		requested, staged, _, _ := pl.status()
		if uErr := updateTransferProgress(&proto.ETCDStatusDetails{
			Files:        files,
			Data:         fmt.Sprintf("%dB", copiedBytes),
			PluginStatus: fmt.Sprintf("staged %d of %d requested file(s), copied %d file(s)", staged, requested, files),
		}); uErr != nil {
			p.log.Errorf("failed to update scoutam transfer progress: %v", uErr)
		}
	}

	_, _, stageErrs, walkErr := pl.status()
	if walkErr != nil {
		return transferError(proto.Error_ERROR_FTA_PLUGIN_FAILED, fmt.Sprintf("failed to request staging: %v", walkErr))
	}

	for _, g := range groups {
		if !g.isDir {
			continue
		}
		if err := syncRootDir(ctx, cfg.RsyncPath, g); err != nil {
			copyErrs = append(copyErrs, fmt.Sprintf("%s: %v", g.userPath, err))
		}
	}

	pluginErrs := &proto.FTAPluginErrors{}
	if len(stageErrs) > 0 {
		pluginErrs.Errors = append(pluginErrs.Errors, &proto.FTAPathError{
			PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
			ErrMessage: "ScoutAM reported stage errors: " + summarizeErrors(stageErrs),
		})
	}
	if len(copyErrs) > 0 {
		pluginErrs.Errors = append(pluginErrs.Errors, &proto.FTAPathError{
			PErr:       proto.Error_ERROR_FTA_PLUGIN_FAILED,
			ErrMessage: "failed to copy staged files: " + summarizeErrors(copyErrs),
		})
	}
	return pluginErrs
}

func buildCopyGroups(pluginData *plugin.PluginData, destInfo proto.DestInfo) ([]copyGroup, *proto.FTAPathError) {
	destRoot := pluginData.DestinationPluginInfo.TransferPath
	multipleSources := len(pluginData.SourcePluginInfo) > 1

	groups := make([]copyGroup, 0, len(pluginData.SourcePluginInfo))
	for _, sppi := range pluginData.SourcePluginInfo {
		info, err := os.Lstat(sppi.TransferPath)
		if err != nil {
			return nil, &proto.FTAPathError{
				LeasePath:  sppi.OriginalUserPath,
				PErr:       proto.Error_ERROR_STAT_FAILED,
				ErrMessage: fmt.Sprintf("failed to stat source %s: %v", sppi.OriginalUserPath, err),
			}
		}
		groups = append(groups, copyGroup{
			userPath: sppi.OriginalUserPath,
			srcRoot:  sppi.TransferPath,
			destRoot: sourceDestination(destRoot, sppi.OriginalUserPath, destInfo, multipleSources),
			isDir:    info.IsDir(),
			stage:    sppi.FSC != nil && slices.Contains(sppi.FSC.PluginStages.TransferSrc, ScoutAMPluginKey),
			fsc:      sppi.FSC,
		})
	}
	slices.SortFunc(groups, func(a, b copyGroup) int { return strings.Compare(a.userPath, b.userPath) })
	return groups, nil
}

// sourceDestination mirrors posix ValidateDestination: a source lands inside an existing
// destination directory, otherwise the destination itself becomes the copy.
func sourceDestination(destRoot string, userSrc string, destInfo proto.DestInfo, multipleSources bool) string {
	inside := destInfo == proto.DestInfo_DEST_IS_DIR
	if destInfo == proto.DestInfo_DEST_NONE {
		// destination wasn't validated by posix - fall back to rsync's semantics
		info, err := os.Stat(destRoot)
		inside = multipleSources || (err == nil && info.IsDir())
	}
	if inside {
		return filepath.Join(destRoot, filepath.Base(userSrc))
	}
	return destRoot
}

func walkSources(ctx context.Context, groups []copyGroup, st *stager, pl *stagePipeline, batchSize int, updateTransferProgress plugin.UpdateTransferProgress) error {
	var requested int
	for gi, g := range groups {
		if err := walkSource(ctx, gi, g, st, pl, batchSize, &requested, updateTransferProgress); err != nil {
			return fmt.Errorf("%s: %w", g.userPath, err)
		}
	}
	return nil
}

// walkSource queues every entry under g: regular files on ScoutFS are requested for staging (and
// become ready when their notification arrives), everything else is ready to copy immediately.
func walkSource(ctx context.Context, gi int, g copyGroup, st *stager, pl *stagePipeline, batchSize int, requested *int, updateTransferProgress plugin.UpdateTransferProgress) error {
	var apiRoot string
	var fsInfo filesystemInfo
	if g.stage {
		var err error
		if apiRoot, err = getAPIRoot(g.fsc); err != nil {
			return err
		}
		if fsInfo, err = st.filesystem(ctx, apiRoot); err != nil {
			return err
		}
	}

	batch := make([]string, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := st.client.batchStage(ctx, st.token, batch, fsInfo.FSID, st.topic); err != nil {
			return err
		}
		*requested += len(batch)
		batch = batch[:0]
		_ = updateTransferProgress(&proto.ETCDStatusDetails{
			PluginStatus: fmt.Sprintf("requested staging of %d file(s) so far", *requested),
		})
		return nil
	}

	visit := func(path string, rel string, d fs.DirEntry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		item := copyItem{group: gi, rel: rel, isDir: d.IsDir()}
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			item.size = info.Size()
		}
		if !g.stage || !d.Type().IsRegular() {
			pl.addReady(item)
			return nil
		}

		apiPath, err := serverAPIPath(path, g.fsc.FTARootFSPathSub, apiRoot)
		if err != nil {
			return err
		}
		if err := pl.addPending(fsInfo.Mount, apiPath, item); err != nil {
			return err
		}
		batch = append(batch, apiPath)
		if len(batch) >= batchSize {
			return flush()
		}
		return nil
	}

	var err error
	if !g.isDir {
		info, lerr := os.Lstat(g.srcRoot)
		if lerr != nil {
			return lerr
		}
		err = visit(g.srcRoot, "", fs.FileInfoToDirEntry(info))
	} else {
		err = filepath.WalkDir(g.srcRoot, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if path == g.srcRoot {
				return nil // root attributes are synced after everything else is copied
			}
			rel, relErr := filepath.Rel(g.srcRoot, path)
			if relErr != nil {
				return relErr
			}
			return visit(path, rel, d)
		})
	}
	if err != nil {
		return err
	}
	return flush()
}

func rsyncItems(ctx context.Context, rsyncPath string, g copyGroup, items []copyItem) error {
	args := slices.Clone(rsyncPreserveArgs)
	var stdin io.Reader
	if g.isDir {
		var list bytes.Buffer
		for _, item := range items {
			list.WriteString(item.rel)
			list.WriteByte(0)
		}
		// --files-from implies --relative and --dirs, so listed directories are created without their contents
		args = append(args, "--from0", "--files-from=-", g.srcRoot+"/", g.destRoot+"/")
		stdin = &list
	} else {
		args = append(args, g.srcRoot, g.destRoot)
	}
	return runRsync(ctx, rsyncPath, args, stdin)
}

// syncRootDir creates the destination directory if needed and copies the source directory's own
// attributes onto it; the exclude keeps rsync from touching contents that were already copied.
func syncRootDir(ctx context.Context, rsyncPath string, g copyGroup) error {
	args := append(slices.Clone(rsyncPreserveArgs), "--dirs", "--exclude=*", g.srcRoot+"/", g.destRoot+"/")
	return runRsync(ctx, rsyncPath, args, nil)
}

func runRsync(ctx context.Context, rsyncPath string, args []string, stdin io.Reader) error {
	cmd := exec.CommandContext(ctx, rsyncPath, args...)
	cmd.Stdin = stdin
	out, err := cmd.CombinedOutput()
	if err != nil {
		output := strings.TrimSpace(string(out))
		if len(output) > maxRsyncOutput {
			output = output[:maxRsyncOutput] + " ...(truncated)"
		}
		return fmt.Errorf("%v failed: %v: %s", cmd.Args, err, output)
	}
	return nil
}

func summarizeErrors(errs []string) string {
	if len(errs) <= maxReportedErrors {
		return strings.Join(errs, "; ")
	}
	return fmt.Sprintf("%s; ... and %d more", strings.Join(errs[:maxReportedErrors], "; "), len(errs)-maxReportedErrors)
}

func transferError(pErr proto.Error, msg string) *proto.FTAPluginErrors {
	return &proto.FTAPluginErrors{
		Errors: []*proto.FTAPathError{{PErr: pErr, ErrMessage: msg}},
	}
}
