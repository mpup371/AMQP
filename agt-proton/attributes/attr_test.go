package attributes

import (
	"bytes"
	"os/exec"
	"testing"

	"github.com/pkg/xattr"
)

func TestSet(t *testing.T) {
	exec.Command("sh", "-c", "date > /tmp/testSet").Run()
	if err := xattr.Set("/tmp/testSet", "user.agt.routage.file", ([]byte)("testtesttest")); err != nil {
		t.Error(err)
	}

	if v, err := xattr.Get("/tmp/testSet", "user.agt.routage.file"); err != nil {
		t.Error(err)
	} else {
		if bytes.Compare(v, ([]byte)("testtesttest")) != 0 {
			t.Error("attribute not equal")
		}
	}
}
