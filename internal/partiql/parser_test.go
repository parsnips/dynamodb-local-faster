package partiql

import "testing"

func TestParseInsertStatementExtractsTableAndPartitionKey(t *testing.T) {
	parser := NewNoopParser()

	statement, err := parser.Parse(`INSERT INTO "users" VALUE {'pk':'user-1','payload':'value-1'}`)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if got, want := statement.TableName, "users"; got != want {
		t.Fatalf("TableName = %q, want %q", got, want)
	}
	if statement.ReadOnly {
		t.Fatal("ReadOnly = true, want false")
	}
	if !statement.HasPartitionKey {
		t.Fatal("HasPartitionKey = false, want true")
	}
	if got, want := statement.PartitionKeyAttribute, "pk"; got != want {
		t.Fatalf("PartitionKeyAttribute = %q, want %q", got, want)
	}
	if got, want := string(statement.PartitionKey), `{"S":"user-1"}`; got != want {
		t.Fatalf("PartitionKey = %q, want %q", got, want)
	}
}

func TestParseSelectStatementExtractsWherePartitionKey(t *testing.T) {
	parser := NewNoopParser()

	statement, err := parser.Parse(`SELECT * FROM users WHERE pk = 'user-2'`)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if got, want := statement.TableName, "users"; got != want {
		t.Fatalf("TableName = %q, want %q", got, want)
	}
	if !statement.ReadOnly {
		t.Fatal("ReadOnly = false, want true")
	}
	if !statement.HasPartitionKey {
		t.Fatal("HasPartitionKey = false, want true")
	}
	if got, want := statement.PartitionKeyAttribute, "pk"; got != want {
		t.Fatalf("PartitionKeyAttribute = %q, want %q", got, want)
	}
	if got, want := string(statement.PartitionKey), `{"S":"user-2"}`; got != want {
		t.Fatalf("PartitionKey = %q, want %q", got, want)
	}
}

func TestParseUpdateWithoutWhereDoesNotInferPartitionKey(t *testing.T) {
	parser := NewNoopParser()

	statement, err := parser.Parse(`UPDATE users SET payload = 'x'`)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if got, want := statement.TableName, "users"; got != want {
		t.Fatalf("TableName = %q, want %q", got, want)
	}
	if statement.ReadOnly {
		t.Fatal("ReadOnly = true, want false")
	}
	if statement.HasPartitionKey {
		t.Fatal("HasPartitionKey = true, want false")
	}
}

func TestParseDeleteStatementWithWhere(t *testing.T) {
	parser := NewNoopParser()

	statement, err := parser.Parse(`DELETE FROM users WHERE pk = 42`)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if got, want := statement.TableName, "users"; got != want {
		t.Fatalf("TableName = %q, want %q", got, want)
	}
	if statement.ReadOnly {
		t.Fatal("ReadOnly = true, want false")
	}
	if !statement.HasPartitionKey {
		t.Fatal("HasPartitionKey = false, want true")
	}
	if got, want := string(statement.PartitionKey), `{"N":"42"}`; got != want {
		t.Fatalf("PartitionKey = %q, want %q", got, want)
	}
}
