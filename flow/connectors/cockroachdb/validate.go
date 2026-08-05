package conncockroachdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func (c *CockroachDBConnector) ValidateCheck(ctx context.Context) error {
	majorVersion, err := c.GetMajorVersion(ctx)
	if err != nil {
		return err
	}

	if majorVersion < 22 {
		return fmt.Errorf("CockroachDB must be version 22.1 or above. Current version: %d.x", majorVersion)
	}
	return nil
}

func settingValueIsTrue(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return v == "true" || v == "t" || v == "on"
	default:
		return false
	}
}

// isUnknownSettingError reports whether err is CockroachDB rejecting a SHOW
// CLUSTER SETTING probe for a setting the cluster does not know about. That is
// the expected negative outcome when probing variant-specific settings, unlike
// network or privilege errors. CockroachDB raises these as "unknown setting:
// ..." with the uncategorized SQLSTATE XXUUU (verified on v25.4.13), so the
// message is the only reliable discriminator.
func isUnknownSettingError(err error) bool {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return strings.Contains(pgErr.Message, "unknown setting")
	}
	return false
}

func (c *CockroachDBConnector) GetDatabaseVariant(ctx context.Context) (protos.DatabaseVariant, error) {
	// SHOW CLUSTER SETTING works without allow_unsafe_internals, unlike
	// crdb_internal.cluster_settings. cluster.organization exists on every
	// CockroachDB flavor: self-hosted clusters return an empty string, managed
	// CockroachDB Cloud clusters always have it set. An error here therefore
	// means the probe itself failed (privileges, network), not a negative
	// answer; the variant is telemetry only, so log and report unknown.
	var clusterOrg string
	if err := c.conn.QueryRow(ctx, "SHOW CLUSTER SETTING cluster.organization").Scan(&clusterOrg); err != nil {
		if !isUnknownSettingError(err) {
			c.logger.Warn("[cockroachdb] failed to probe cluster.organization for variant detection",
				slog.Any("error", err))
		}
		return protos.DatabaseVariant_VARIANT_UNKNOWN, nil
	}

	// server.serverless.enabled only exists on serverless hosts; everywhere
	// else the probe fails with an unknown-setting error, which is the
	// expected negative answer
	var serverless any
	if err := c.conn.QueryRow(ctx, "SHOW CLUSTER SETTING server.serverless.enabled").Scan(&serverless); err != nil {
		if !isUnknownSettingError(err) {
			c.logger.Warn("[cockroachdb] failed to probe server.serverless.enabled for variant detection",
				slog.Any("error", err))
		}
	} else if settingValueIsTrue(serverless) {
		return protos.DatabaseVariant_COCKROACHDB_SERVERLESS, nil
	}

	// managed CockroachDB Cloud clusters always have cluster.organization set
	if clusterOrg != "" {
		return protos.DatabaseVariant_COCKROACHDB_CLOUD, nil
	}

	return protos.DatabaseVariant_VARIANT_UNKNOWN, nil
}
