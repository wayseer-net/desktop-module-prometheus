package prometheus_test

import (
	"os/exec"
	"strings"
	"testing"
)

// TestImportsOnlyTheSDK keeps the module free of the app: it links only the SDK, the standard
// library and third-party modules.
func TestImportsOnlyTheSDK(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "-test", "wayseer.dev/modules/prometheus/...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, pkg := range strings.Fields(string(out)) {
		if strings.HasPrefix(pkg, "wayseer/") || (strings.HasPrefix(pkg, "wayseer.dev/modules/") && !strings.HasPrefix(pkg, "wayseer.dev/modules/prometheus")) {
			t.Errorf("the module imports %s", pkg)
		}
	}
}
