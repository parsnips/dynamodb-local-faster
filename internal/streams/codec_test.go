package streams

import (
	"testing"
)

func TestEncodeDecodeVirtualToken(t *testing.T) {
	tests := []struct {
		name      string
		backendID int
		realToken string
	}{
		{"backend 0", 0, "shardId-000000000001"},
		{"backend 1", 1, "shardId-000000000002"},
		{"backend 99", 99, "some-iterator-token"},
		{"token with colons", 2, "arn:aws:dynamodb:ddblocal:000:table/foo/stream/123"},
		{"long token", 5, "AAAAAAAAAAGDiBjBlYjFkLWQ3ZDctNDY1OS04OGQy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeVirtualToken(tt.backendID, tt.realToken)
			gotID, gotToken, err := DecodeVirtualToken(encoded)
			if err != nil {
				t.Fatalf("DecodeVirtualToken(%q) error = %v", encoded, err)
			}
			if gotID != tt.backendID {
				t.Errorf("backendID = %d, want %d", gotID, tt.backendID)
			}
			if gotToken != tt.realToken {
				t.Errorf("realToken = %q, want %q", gotToken, tt.realToken)
			}
		})
	}
}

func TestDecodeVirtualTokenErrors(t *testing.T) {
	tests := []struct {
		name  string
		token string
	}{
		{"missing prefix", "shardId-000000000001"},
		{"wrong prefix", "xyz0:shardId-000000000001"},
		{"no separator", "dlfb42"},
		{"non-numeric backend ID", "dlfbabc:shardId-000000000001"},
		{"empty real token", "dlfb0:"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := DecodeVirtualToken(tt.token)
			if err == nil {
				t.Fatalf("DecodeVirtualToken(%q) expected error", tt.token)
			}
		})
	}
}

func TestParseTableNameFromStreamARN(t *testing.T) {
	tests := []struct {
		name      string
		arn       string
		wantTable string
	}{
		{
			"valid ARN",
			"arn:aws:dynamodb:ddblocal:000000000000:table/MyTable/stream/2024-01-01T00:00:00.000",
			"MyTable",
		},
		{
			"table with hyphens",
			"arn:aws:dynamodb:ddblocal:000000000000:table/my-test-table/stream/2024-01-01",
			"my-test-table",
		},
		{
			"table with underscores",
			"arn:aws:dynamodb:ddblocal:000000000000:table/my_table_v2/stream/123456",
			"my_table_v2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTableNameFromStreamARN(tt.arn)
			if err != nil {
				t.Fatalf("ParseTableNameFromStreamARN(%q) error = %v", tt.arn, err)
			}
			if got != tt.wantTable {
				t.Errorf("ParseTableNameFromStreamARN(%q) = %q, want %q", tt.arn, got, tt.wantTable)
			}
		})
	}
}

func TestParseTableNameFromStreamARNErrors(t *testing.T) {
	tests := []struct {
		name string
		arn  string
	}{
		{"empty", ""},
		{"no table segment", "arn:aws:dynamodb:ddblocal:000000000000"},
		{"no stream segment", "arn:aws:dynamodb:ddblocal:000000000000:table/MyTable"},
		{"empty table name", "arn:aws:dynamodb:ddblocal:000000000000:table//stream/123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseTableNameFromStreamARN(tt.arn)
			if err == nil {
				t.Fatalf("ParseTableNameFromStreamARN(%q) expected error", tt.arn)
			}
		})
	}
}
