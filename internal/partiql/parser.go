package partiql

import (
	"fmt"
	"strings"
)

type Statement struct {
	Raw             string
	TableName       string
	PartitionKey    []byte
	HasPartitionKey bool
	ReadOnly        bool
}

type Parser interface {
	Parse(statement string) (Statement, error)
}

type NoopParser struct{}

func NewNoopParser() *NoopParser {
	return &NoopParser{}
}

func (p *NoopParser) Parse(statement string) (Statement, error) {
	_ = p

	statement = strings.TrimSpace(statement)
	if statement == "" {
		return Statement{}, fmt.Errorf("statement is required")
	}

	// Real PartiQL parsing is implemented in a later milestone.
	return Statement{
		Raw:      statement,
		ReadOnly: strings.HasPrefix(strings.ToUpper(statement), "SELECT"),
	}, nil
}
