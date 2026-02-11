package streams

import (
	"fmt"
	"strconv"
	"strings"
)

const virtualTokenPrefix = "dlfb"

// EncodeVirtualToken encodes a backend ID and real token into a virtual token.
// Format: dlfb{backendID}:{realToken}
func EncodeVirtualToken(backendID int, realToken string) string {
	return virtualTokenPrefix + strconv.Itoa(backendID) + ":" + realToken
}

// DecodeVirtualToken decodes a virtual token into a backend ID and real token.
func DecodeVirtualToken(virtualToken string) (backendID int, realToken string, err error) {
	if !strings.HasPrefix(virtualToken, virtualTokenPrefix) {
		return 0, "", fmt.Errorf("virtual token missing %q prefix", virtualTokenPrefix)
	}
	rest := virtualToken[len(virtualTokenPrefix):]

	colonIdx := strings.IndexByte(rest, ':')
	if colonIdx < 0 {
		return 0, "", fmt.Errorf("virtual token missing separator")
	}

	idStr := rest[:colonIdx]
	backendID, err = strconv.Atoi(idStr)
	if err != nil {
		return 0, "", fmt.Errorf("virtual token has non-numeric backend ID: %w", err)
	}

	realToken = rest[colonIdx+1:]
	if realToken == "" {
		return 0, "", fmt.Errorf("virtual token has empty real token")
	}

	return backendID, realToken, nil
}

// ParseTableNameFromStreamARN extracts the table name from a DynamoDB Local stream ARN.
// Expected format: arn:aws:dynamodb:ddblocal:000000000000:table/{tableName}/stream/{timestamp}
func ParseTableNameFromStreamARN(arn string) (string, error) {
	const tableMarker = ":table/"
	idx := strings.Index(arn, tableMarker)
	if idx < 0 {
		return "", fmt.Errorf("stream ARN missing table segment: %s", arn)
	}
	rest := arn[idx+len(tableMarker):]

	slashIdx := strings.IndexByte(rest, '/')
	if slashIdx < 0 {
		return "", fmt.Errorf("stream ARN missing stream segment: %s", arn)
	}

	tableName := rest[:slashIdx]
	if tableName == "" {
		return "", fmt.Errorf("stream ARN has empty table name: %s", arn)
	}
	return tableName, nil
}
