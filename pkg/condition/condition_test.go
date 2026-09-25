package condition

import (
	"strings"
	"testing"
	"time"
)

func TestSetRaiseClear(t *testing.T) {
	s := NewSet("test")
	now := time.Unix(100, 0)
	s.now = func() time.Time { return now }

	s.Raise("b_type", "r1", "first")
	s.Raise("a_type", "r1", "other")
	now = now.Add(time.Minute)
	s.Raise("b_type", "r1", "second")

	list := s.List()
	if len(list) != 2 || list[0].Type != "a_type" || list[1].Type != "b_type" {
		t.Fatalf("List = %+v; want a_type, b_type", list)
	}
	if b := list[1]; b.Message != "second" || !b.Since.Equal(time.Unix(100, 0)) {
		t.Errorf("re-raising with the same reason should keep Since and update Message: %+v", b)
	}

	s.Raise("b_type", "r2", "changed")
	if b := s.List()[1]; !b.Since.Equal(now) {
		t.Errorf("a new reason should restart Since: %+v", b)
	}

	s.Clear("b_type")
	if s.Active("b_type") || !s.Active("a_type") || !Has(s.List(), "a_type") {
		t.Errorf("after Clear: %+v", s.List())
	}
}

func TestRedact(t *testing.T) {
	msg := `PUT https://acct.blob.core.windows.net/c/x?sv=2020&sig=SECRET: 403; also "http://h/p?X-Amz-Signature=SECRET2"`
	got := Redact(msg)
	if strings.Contains(got, "SECRET") {
		t.Errorf("Redact left a query string: %s", got)
	}
	if !strings.Contains(got, "https://acct.blob.core.windows.net/c/x?REDACTED") {
		t.Errorf("Redact dropped the URL path: %s", got)
	}
}
