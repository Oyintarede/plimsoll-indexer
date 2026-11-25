package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/config"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/worker"
)

func runBackfillCommand(args []string) int {
	fs := flag.NewFlagSet("backfill", flag.ContinueOnError)
	from := fs.Uint64("from", 0, "first ledger to backfill (inclusive)")
	to := fs.Uint64("to", 0, "last ledger to backfill (inclusive)")
	corePath := fs.String("stellar-core", envOrDefault("STELLAR_CORE_PATH", "stellar-core"), "path to Stellar Core v23+")
	timeout := fs.Duration("timeout", 24*time.Hour, "maximum backfill duration")
	var archiveURLs stringListFlag
	fs.Var(&archiveURLs, "archive-url", "HTTPS history archive URL (repeat for multiple archives)")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *from < 2 || *from > uint64(^uint32(0)) || *to < *from || *to > uint64(^uint32(0)) {
		fmt.Fprintln(os.Stderr, "backfill requires --from >= 2 and --to >= --from within the uint32 ledger range")
		fs.Usage()
		return 1
	}
	if *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "--timeout must be positive")
		return 1
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "backfill configuration: %v\n", err)
		return 1
	}
	log := newLogger(cfg.LogLevel)
	archives := archiveURLs
	if len(archives) == 0 {
		archives, err = defaultArchiveURLs(cfg.NetworkPassphrase)
		if err != nil {
			log.Error("backfill archive configuration", "err", err)
			return 1
		}
	}
	if err := validateArchiveURLs(archives); err != nil {
		log.Error("backfill archive configuration", "err", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	db, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("open database", "err", err)
		return 1
	}
	defer db.Close()

	coreTOML, err := ledgerbackend.NewCaptiveCoreToml(ledgerbackend.CaptiveCoreTomlParams{
		NetworkPassphrase:  cfg.NetworkPassphrase,
		HistoryArchiveURLs: archives,
	})
	if err != nil {
		log.Error("configure archive backend", "err", err)
		return 1
	}
	backend, err := ledgerbackend.NewCaptive(ledgerbackend.CaptiveCoreConfig{
		BinaryPath:         *corePath,
		NetworkPassphrase:  cfg.NetworkPassphrase,
		HistoryArchiveURLs: archives,
		Toml:               coreTOML,
		Context:            ctx,
	})
	if err != nil {
		log.Error("start archive backend", "err", err)
		return 1
	}

	backfiller := &worker.Backfiller{
		Backend:           backend,
		Store:             db,
		ContractIDs:       []string{cfg.CoverageLedgerID, cfg.ReporterRegistryID},
		NetworkPassphrase: cfg.NetworkPassphrase,
		Log:               log.With("job", "backfill"),
	}
	if err := backfiller.Run(ctx, uint32(*from), uint32(*to)); err != nil {
		log.Error("backfill failed", "err", err)
		return 1
	}
	return 0
}

func defaultArchiveURLs(passphrase string) ([]string, error) {
	switch passphrase {
	case network.TestNetworkPassphrase:
		return append([]string(nil), network.TestNetworkhistoryArchiveURLs...), nil
	case network.PublicNetworkPassphrase:
		return append([]string(nil), network.PublicNetworkhistoryArchiveURLs...), nil
	default:
		return nil, errors.New("custom network passphrase requires one or more --archive-url values")
	}
}

func validateArchiveURLs(archives []string) error {
	if len(archives) == 0 {
		return errors.New("at least one history archive URL is required")
	}
	for _, raw := range archives {
		parsed, err := url.ParseRequestURI(raw)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
			return fmt.Errorf("history archive URL must be an HTTPS URL without credentials: %q", raw)
		}
	}
	return nil
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

type stringListFlag []string

func (s *stringListFlag) String() string { return strings.Join(*s, ",") }

func (s *stringListFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}
