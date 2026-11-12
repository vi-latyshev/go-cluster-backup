package assembly

// The external test package sees the harness of harness_test.go through the
// identifiers below. They exist only in the test build; the production package
// exports none of it.

import (
	"log/slog"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

type (
	World            = world
	FakeSource       = fakeSource
	FakeProcessor    = fakeProcessor
	FakeDiscovery    = fakeDiscovery
	FakeNotifier     = fakeNotifier
	FakeCatalog      = fakeCatalog
	ChainSource      = chainSource
	CredentialSource = credentialSource
)

// NewWorld builds one harness world with the distribution's fakes registered.
func NewWorld(t *testing.T) *World { return newWorld(t) }

// NewCredentialSource builds a source whose bytes carry credentials, with the
// verdict its optional policy answers and an optional policy failure.
func NewCredentialSource(data []byte, verdict source.CredentialVerdict, policyErr error) *credentialSource {
	return &credentialSource{Source: fakeSource{Data: data}, Verdict: verdict, PolicyErr: policyErr}
}

// NewAssembly builds the assembly from one configuration, failing the test on
// a construction error.
func (w *world) NewAssembly(t *testing.T, cfg map[string]any) *Assembly {
	return w.newAssembly(t, cfg)
}

// NewAssemblyErr builds the assembly and hands its error back.
func (w *world) NewAssemblyErr(t *testing.T, cfg map[string]any) (*Assembly, error) {
	return w.newAssemblyErr(t, cfg)
}

// Storage answers the fake one storage instance built.
func (w *world) Storage(t *testing.T, name string) *storagetest.Fake { return w.storage(t, name) }

// SyncLoader declares every factory registered since NewWorld.
func (w *world) SyncLoader(t *testing.T) { w.syncLoader(t) }

// NewFakeSource builds a source with the bytes it dumps.
func NewFakeSource(data []byte) *fakeSource { return &fakeSource{Data: data} }

// NewFakeProcessor builds a processor with its extension and sealing flag.
func NewFakeProcessor(ext string, seals bool, checkErr error) *fakeProcessor {
	return &fakeProcessor{Ext: ext, Seals: seals, CheckErr: checkErr}
}

// NewChainSource builds a source that carries a recorded producer chain.
func NewChainSource(data []byte, ext string, chain []model.ProcessorStep) *chainSource {
	return &chainSource{Data: data, Ext: ext, Chain: chain}
}

// NewFakeDiscovery builds a discovery answering the given findings.
func NewFakeDiscovery(found []discovery.Found) *fakeDiscovery {
	return &fakeDiscovery{Found: found}
}

// The harness's pure helpers, reachable from the external test package.
func JSONMarshal(v any) (jsontext.Value, error) { return jsonMarshal(v) }
func JSONUnmarshal(b []byte, v any) error       { return jsonUnmarshal(b, v) }
func MustMarshal(t *testing.T, v any) []byte    { return mustMarshal(t, v) }
func ReadDirNames(dir string) ([]string, error) { return readDirNames(dir) }
func HarnessLogger() *slog.Logger               { return testLogger() }

// The configuration builders the external tests share with the internal ones.
func ConfigLayout() map[string]any { return configLayout() }

func Instance(typ string) map[string]any { return instance(typ) }

func SourceInstance(typ, group, name string, pipeline []string, importance, sensitivity string) map[string]any {
	return sourceInstance(typ, group, name, pipeline, importance, sensitivity)
}

func StorageInstance(typ, retentionType string) map[string]any {
	return storageInstance(typ, retentionType)
}

func KeepLastCfg(count int) map[string]any { return keepLastCfg(count) }

func BaseConfig(t *testing.T, storages map[string]any) map[string]any {
	t.Helper()
	return baseConfig(t, storages)
}

// ConfigRun is the smallest complete run section a test config starts from.
func ConfigRun(spoolDir string) map[string]any { return configRun(spoolDir) }
