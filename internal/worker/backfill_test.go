package worker

import (
	"strings"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

func TestArchiveEventUsesLiveCursorAndExistingDecoder(t *testing.T) {
	rawContract, err := strkey.Decode(strkey.VersionByteContract, "CCHPT4TEJDPZQDSUVW3NP6A35ROGV4WEFZKT45SKWYCFZSUB7R5HIM7S")
	if err != nil {
		t.Fatal(err)
	}
	var contractID xdr.ContractId
	copy(contractID[:], rawContract)
	reporter := "GDACI55IMI5DDZWMZ2EAXWK57AV75VE4626YYBAHQJYDV7QFPNNFK6HJ"
	reporterVal, err := soroban.AddressVal(reporter)
	if err != nil {
		t.Fatal(err)
	}
	event := xdr.ContractEvent{
		Type:       xdr.ContractEventTypeContract,
		ContractId: &contractID,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{symVal(t, soroban.EventReporterRevoked), reporterVal},
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvVoid},
			},
		},
	}
	closedAt := time.Date(2026, 11, 25, 8, 0, 0, 0, time.UTC)
	info, ok, err := archiveEventInfo(
		event, 12345, 7, 2, 5, closedAt, bytesOf(0xab, 32),
		map[string]struct{}{"CCHPT4TEJDPZQDSUVW3NP6A35ROGV4WEFZKT45SKWYCFZSUB7R5HIM7S": {}},
	)
	if err != nil || !ok {
		t.Fatalf("archiveEventInfo: ok=%v err=%v", ok, err)
	}
	wantID := (protocol.Cursor{Ledger: 12345, Tx: 7, Op: 2, Event: 5}).String()
	if info.ID != wantID || info.TransactionHash != strings.Repeat("ab", 32) {
		t.Fatalf("metadata = id %q tx %q", info.ID, info.TransactionHash)
	}
	decoded, err := soroban.Decode(info)
	if err != nil {
		t.Fatal(err)
	}
	revoked, ok := decoded.(soroban.ReporterRevoked)
	if !ok || revoked.Reporter != reporter || revoked.Ledger != 12345 || !revoked.ClosedAt.Equal(closedAt) {
		t.Fatalf("decoded event = %#v", decoded)
	}
}

func TestArchiveEventSkipsUnconfiguredContract(t *testing.T) {
	var contractID xdr.ContractId
	info, ok, err := archiveEventInfo(
		xdr.ContractEvent{Type: xdr.ContractEventTypeContract, ContractId: &contractID},
		1, 1, 0, 0, time.Now(), nil, map[string]struct{}{},
	)
	if err != nil || ok || info.ID != "" {
		t.Fatalf("got info=%+v ok=%v err=%v", info, ok, err)
	}
}

func bytesOf(value byte, count int) []byte {
	out := make([]byte, count)
	for i := range out {
		out[i] = value
	}
	return out
}
