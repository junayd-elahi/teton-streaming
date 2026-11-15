package integration

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/junayd-elahi/teton-streaming/internal/store"
	"github.com/junayd-elahi/teton-streaming/migrations"
)

func TestPublishedAlarmIdentitySurvivesSchemaUpgrade(t *testing.T) {
	ctx := context.Background()
	old, err := store.Open(ctx, isolatedDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	legacy := fstest.MapFS{}
	names, err := fs.Glob(migrations.Files, "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if name >= "006_" {
			continue
		}
		data, err := fs.ReadFile(migrations.Files, name)
		if err != nil {
			t.Fatal(err)
		}
		legacy[name] = &fstest.MapFile{Data: data}
	}
	if err := old.Migrate(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	body := []byte(fallAt("dev_upgrade", "room_upgrade", 1, "2026-11-15T11:59:00.000Z", 0.9))
	var object map[string]any
	if err := json.Unmarshal(body, &object); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(canonical)
	if _, err := old.AppendBatch(ctx, []store.Event{{
		DeviceID: "dev_upgrade", RoomID: "room_upgrade", Seq: 1, Type: "fall_warn",
		TS: parseTS(t, "2026-11-15T11:59:00.000Z"), ReceivedAt: fixedNow,
		Payload: canonical, PayloadHash: hash[:],
	}}); err != nil {
		t.Fatal(err)
	}
	old.Close()

	svc := startService(t)
	t.Cleanup(func() { svc.stop() })
	page := alarmsFrom(t, svc, "after_id=0")
	if len(page.Alarms) != 1 || page.Alarms[0].EventID != "dev_upgrade:1" {
		t.Fatalf("upgrade changed a published identity: %+v", page)
	}
	if reply := postReply(t, svc, string(body)); reply.status != 200 || !reply.duplicate {
		t.Fatalf("HTTP retry of a legacy event changed identity: %+v", reply)
	}
	mustPost(t, svc, fallAt("dev_upgrade", "room_upgrade", 1, "2026-11-15T11:59:30.000Z", 0.9))
	page = alarmsFrom(t, svc, "after_id=0")
	if len(page.Alarms) != 2 || !strings.HasPrefix(page.Alarms[1].EventID, "alarm:") {
		t.Fatalf("new sequence epoch lacks a unique identity: %+v", page)
	}
	newID := page.Alarms[1].EventID
	svc.stop()
	svc = startService(t)
	after := alarmsFrom(t, svc, "after_id=0")
	if len(after.Alarms) != 2 || after.Alarms[0].EventID != "dev_upgrade:1" || after.Alarms[1].EventID != newID {
		t.Fatalf("restart changed public identities: %+v", after)
	}
}
