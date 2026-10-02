// Copyright 2026. Triad National Security, LLC. All rights reserved.

package scoutam

import (
	"time"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/logger"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	ScoutAMPluginKey                = "scoutam"
	CustomPluginConfigAPIRootKey    = "scoutam-api-root"
	DefaultScoutAMAPIBaseURL        = "https://127.0.0.1:8080"
	DefaultScoutAMNatsStageTopicPre = "conduit.stage"
	DefaultScoutAMStageTimeout      = 30 * time.Minute
	DefaultScoutAMBatchSize         = 1000
	DefaultScoutAMRsyncPath         = "rsync"
)

var _ plugin.ConduitFTAPlugin = (*ScoutAMPlugin)(nil)

type ViperScoutAMPluginConfig struct {
	// APIBaseURL is the base URL of the ScoutAM REST API (FTA hosts are not expected to have samcli/scoutfs tooling installed)
	APIBaseURL string `mapstructure:"api-base-url" yaml:"api-base-url"`
	// APIUsername is the ScoutAM account used to authenticate to the REST API
	APIUsername string `mapstructure:"api-username" yaml:"api-username"`
	// APIPassword is the password for APIUsername
	APIPassword string `mapstructure:"api-password" yaml:"api-password"`
	// APIInsecureSkipVerify skips TLS certificate verification for the ScoutAM API
	APIInsecureSkipVerify bool `mapstructure:"api-insecure-skip-verify" yaml:"api-insecure-skip-verify"`
	// NatsServers is the list of NATS server addresses ScoutAM is configured to publish stage notifications to (see `samcli notify nats --stage`)
	NatsServers []string `mapstructure:"nats-servers" yaml:"nats-servers"`
	// NatsStageTopicPrefix prefixes the unique per-transfer NATS topic requested for stage completion notifications
	NatsStageTopicPrefix string `mapstructure:"nats-stage-topic-prefix" yaml:"nats-stage-topic-prefix"`
	// StageTimeout is the max amount of time to wait for all requested files to report as staged before failing setup
	StageTimeout time.Duration `mapstructure:"stage-timeout" yaml:"stage-timeout"`
	// BatchSize is the max number of files sent in a single batchstage API request. Directories are
	// split into multiple requests of this size so ScoutAM can start staging early files while later
	// ones are still being enumerated, and so no single request body gets too large.
	BatchSize int `mapstructure:"batch-size" yaml:"batch-size"`
	// RsyncPath is the rsync binary used by Transfer to copy files as soon as they're staged
	RsyncPath string `mapstructure:"rsync-path" yaml:"rsync-path"`
}

type ScoutAMPlugin struct {
	log        *logger.ConduitLogger
	transferID uuid.UUID
}

func (p *ScoutAMPlugin) Initialize(transferID uuid.UUID, log *logger.ConduitLogger) []plugin.PluginCapability {
	p.log = log
	p.transferID = transferID

	return []plugin.PluginCapability{
		plugin.SETUP,
		plugin.TRANSFER,
	}
}

// no op
func (p *ScoutAMPlugin) GetResolvedPath(userPath string, pathType proto.LeaseType, fsc *plugin.FileSystemConfig) (resolvedFTAPath string, foundSymlink string, _ *proto.FTAPathError) {
	return "", "", nil
}

// no op
func (p *ScoutAMPlugin) ValidateSource(pluginPathInfo *plugin.PluginPathInfo, action string, options map[string]*anypb.Any) (pluginErrors *proto.FTAPluginErrors, pluginPathData *string, omit bool) {
	return &proto.FTAPluginErrors{}, nil, false
}

// no op
func (p *ScoutAMPlugin) ValidateDestination(sourceBases []string, userDestination string, ftaDestination string, fsConfig *plugin.FileSystemConfig) (pluginErrors *proto.FTAPluginErrors, userDestinations []string, resolvedFTADestinations []string, destInfo proto.DestInfo, pluginPathData map[string]*string) {
	return &proto.FTAPluginErrors{}, []string{}, []string{}, proto.DestInfo_DEST_NONE, make(map[string]*string)
}

// no op
func (p *ScoutAMPlugin) Teardown(transferID uuid.UUID, transferDetails *proto.TransferDetails, pathInfo *plugin.PluginPathInfo, pathType proto.LeaseType, action string, options map[string]*anypb.Any, baseDest bool, updateTransferProgress plugin.UpdateTransferProgress) (_ *proto.FTAPluginErrors) {
	return &proto.FTAPluginErrors{}
}

func (p *ScoutAMPlugin) GetDefaultConfig() any {
	return DefaultScoutAMPluginConfig()
}

func DefaultScoutAMPluginConfig() ViperScoutAMPluginConfig {
	return ViperScoutAMPluginConfig{
		APIBaseURL:           DefaultScoutAMAPIBaseURL,
		NatsStageTopicPrefix: DefaultScoutAMNatsStageTopicPre,
		StageTimeout:         DefaultScoutAMStageTimeout,
		BatchSize:            DefaultScoutAMBatchSize,
		RsyncPath:            DefaultScoutAMRsyncPath,
	}
}
