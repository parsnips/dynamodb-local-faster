package partiql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

type Statement struct {
	Raw                   string
	TableName             string
	PartitionKey          []byte
	PartitionKeyAttribute string
	HasPartitionKey       bool
	ReadOnly              bool
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

	rootKeyword := readLeadingKeyword(statement)
	switch rootKeyword {
	case "SELECT":
		return parseSelectStatement(statement)
	case "INSERT":
		return parseInsertStatement(statement)
	case "UPDATE":
		return parseUpdateStatement(statement)
	case "DELETE":
		return parseDeleteStatement(statement)
	default:
		// Keep fallback permissive so unsupported read-only statements can still fan out.
		return Statement{
			Raw:      statement,
			ReadOnly: rootKeyword == "SELECT",
		}, nil
	}
}

func parseSelectStatement(statement string) (Statement, error) {
	parsed := Statement{
		Raw:      statement,
		ReadOnly: true,
	}

	fromIdx := findKeywordOutsideQuotes(statement, "FROM")
	if fromIdx < 0 {
		return parsed, nil
	}

	tableName, _, ok := parseIdentifier(statement[fromIdx+len("FROM"):])
	if !ok {
		return Statement{}, fmt.Errorf("SELECT is missing table name after FROM")
	}
	parsed.TableName = tableName

	whereIdx := findKeywordOutsideQuotes(statement, "WHERE")
	if whereIdx < 0 {
		return parsed, nil
	}

	attributeName, literal, ok := parseSimpleWhereEquality(statement[whereIdx+len("WHERE"):])
	if !ok {
		return parsed, nil
	}

	partitionKey, err := literalToDynamoAttributeJSON(literal)
	if err != nil {
		return Statement{}, err
	}

	parsed.PartitionKey = partitionKey
	parsed.PartitionKeyAttribute = attributeName
	parsed.HasPartitionKey = true
	return parsed, nil
}

func parseInsertStatement(statement string) (Statement, error) {
	cursor := strings.TrimSpace(statement)
	var ok bool
	cursor, ok = consumeKeyword(cursor, "INSERT")
	if !ok {
		return Statement{}, fmt.Errorf("invalid INSERT statement")
	}
	cursor, ok = consumeKeyword(cursor, "INTO")
	if !ok {
		return Statement{}, fmt.Errorf("INSERT must include INTO")
	}

	tableName, rest, ok := parseIdentifier(cursor)
	if !ok {
		return Statement{}, fmt.Errorf("INSERT is missing table name")
	}

	rest = strings.TrimSpace(rest)
	rest, ok = consumeKeyword(rest, "VALUE")
	if !ok {
		return Statement{}, fmt.Errorf("INSERT must include VALUE")
	}

	objectRaw, ok := extractEnclosedBlock(strings.TrimSpace(rest), '{', '}')
	if !ok {
		return Statement{}, fmt.Errorf("INSERT VALUE must be an object literal")
	}

	attributes, err := parseTopLevelObjectLiteral(objectRaw)
	if err != nil {
		return Statement{}, err
	}

	partitionKeyAttribute, partitionKeyLiteral, ok := pickLikelyPartitionKey(attributes)
	if !ok {
		return Statement{
			Raw:       statement,
			TableName: tableName,
			ReadOnly:  false,
		}, nil
	}

	partitionKey, err := literalToDynamoAttributeJSON(partitionKeyLiteral)
	if err != nil {
		return Statement{}, err
	}

	return Statement{
		Raw:                   statement,
		TableName:             tableName,
		PartitionKey:          partitionKey,
		PartitionKeyAttribute: partitionKeyAttribute,
		HasPartitionKey:       true,
		ReadOnly:              false,
	}, nil
}

func parseUpdateStatement(statement string) (Statement, error) {
	cursor := strings.TrimSpace(statement)
	var ok bool
	cursor, ok = consumeKeyword(cursor, "UPDATE")
	if !ok {
		return Statement{}, fmt.Errorf("invalid UPDATE statement")
	}

	tableName, _, ok := parseIdentifier(cursor)
	if !ok {
		return Statement{}, fmt.Errorf("UPDATE is missing table name")
	}

	parsed := Statement{
		Raw:       statement,
		TableName: tableName,
		ReadOnly:  false,
	}

	whereIdx := findKeywordOutsideQuotes(statement, "WHERE")
	if whereIdx < 0 {
		return parsed, nil
	}

	attributeName, literal, ok := parseSimpleWhereEquality(statement[whereIdx+len("WHERE"):])
	if !ok {
		return parsed, nil
	}

	partitionKey, err := literalToDynamoAttributeJSON(literal)
	if err != nil {
		return Statement{}, err
	}

	parsed.PartitionKey = partitionKey
	parsed.PartitionKeyAttribute = attributeName
	parsed.HasPartitionKey = true
	return parsed, nil
}

func parseDeleteStatement(statement string) (Statement, error) {
	cursor := strings.TrimSpace(statement)
	var ok bool
	cursor, ok = consumeKeyword(cursor, "DELETE")
	if !ok {
		return Statement{}, fmt.Errorf("invalid DELETE statement")
	}
	cursor, ok = consumeKeyword(cursor, "FROM")
	if !ok {
		return Statement{}, fmt.Errorf("DELETE must include FROM")
	}

	tableName, _, ok := parseIdentifier(cursor)
	if !ok {
		return Statement{}, fmt.Errorf("DELETE is missing table name")
	}

	parsed := Statement{
		Raw:       statement,
		TableName: tableName,
		ReadOnly:  false,
	}

	whereIdx := findKeywordOutsideQuotes(statement, "WHERE")
	if whereIdx < 0 {
		return parsed, nil
	}

	attributeName, literal, ok := parseSimpleWhereEquality(statement[whereIdx+len("WHERE"):])
	if !ok {
		return parsed, nil
	}

	partitionKey, err := literalToDynamoAttributeJSON(literal)
	if err != nil {
		return Statement{}, err
	}

	parsed.PartitionKey = partitionKey
	parsed.PartitionKeyAttribute = attributeName
	parsed.HasPartitionKey = true
	return parsed, nil
}

type literalKind string

const (
	literalString  literalKind = "string"
	literalNumber  literalKind = "number"
	literalBoolean literalKind = "boolean"
	literalNull    literalKind = "null"
)

type partiqlLiteral struct {
	kind       literalKind
	textValue  string
	boolValue  bool
	isNullable bool
}

func literalToDynamoAttributeJSON(literal partiqlLiteral) ([]byte, error) {
	switch literal.kind {
	case literalString:
		return json.Marshal(map[string]string{"S": literal.textValue})
	case literalNumber:
		return json.Marshal(map[string]string{"N": literal.textValue})
	case literalBoolean:
		return json.Marshal(map[string]bool{"BOOL": literal.boolValue})
	case literalNull:
		return json.Marshal(map[string]bool{"NULL": true})
	default:
		return nil, fmt.Errorf("unsupported PartiQL literal kind %q", literal.kind)
	}
}

func readLeadingKeyword(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return ""
	}

	end := 0
	for end < len(input) {
		r := rune(input[end])
		if !unicode.IsLetter(r) {
			break
		}
		end++
	}
	if end == 0 {
		return ""
	}
	return strings.ToUpper(input[:end])
}

func consumeKeyword(input string, keyword string) (string, bool) {
	input = strings.TrimSpace(input)
	if len(input) < len(keyword) {
		return "", false
	}
	if !strings.EqualFold(input[:len(keyword)], keyword) {
		return "", false
	}
	if len(input) > len(keyword) && isIdentifierRune(rune(input[len(keyword)])) {
		return "", false
	}
	return strings.TrimSpace(input[len(keyword):]), true
}

func parseIdentifier(input string) (string, string, bool) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", "", false
	}

	if input[0] == '"' {
		value, rest, ok := parseQuotedString(input, '"')
		if !ok {
			return "", "", false
		}
		return value, rest, true
	}

	end := 0
	for end < len(input) {
		if !isIdentifierRune(rune(input[end])) {
			break
		}
		end++
	}
	if end == 0 {
		return "", "", false
	}
	return input[:end], strings.TrimSpace(input[end:]), true
}

func parseQuotedString(input string, quote byte) (string, string, bool) {
	if len(input) == 0 || input[0] != quote {
		return "", "", false
	}

	var builder strings.Builder
	for i := 1; i < len(input); i++ {
		ch := input[i]
		if ch == quote {
			if i+1 < len(input) && input[i+1] == quote {
				builder.WriteByte(quote)
				i++
				continue
			}
			return builder.String(), strings.TrimSpace(input[i+1:]), true
		}
		builder.WriteByte(ch)
	}

	return "", "", false
}

func findKeywordOutsideQuotes(input string, keyword string) int {
	keyword = strings.ToUpper(keyword)
	if keyword == "" {
		return -1
	}

	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i <= len(input)-len(keyword); i++ {
		ch := input[i]
		if inSingleQuote {
			if ch == '\'' {
				if i+1 < len(input) && input[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		}
		if inDoubleQuote {
			if ch == '"' {
				if i+1 < len(input) && input[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		}

		if ch == '\'' {
			inSingleQuote = true
			continue
		}
		if ch == '"' {
			inDoubleQuote = true
			continue
		}

		if !strings.EqualFold(input[i:i+len(keyword)], keyword) {
			continue
		}
		beforeOK := i == 0 || !isIdentifierRune(rune(input[i-1]))
		afterPos := i + len(keyword)
		afterOK := afterPos >= len(input) || !isIdentifierRune(rune(input[afterPos]))
		if beforeOK && afterOK {
			return i
		}
	}

	return -1
}

func parseSimpleWhereEquality(whereClause string) (string, partiqlLiteral, bool) {
	whereClause = strings.TrimSpace(whereClause)
	if whereClause == "" {
		return "", partiqlLiteral{}, false
	}

	andIdx := findKeywordOutsideQuotes(whereClause, "AND")
	if andIdx >= 0 {
		whereClause = strings.TrimSpace(whereClause[:andIdx])
	}

	eqIdx := findEqualityOutsideQuotes(whereClause)
	if eqIdx <= 0 {
		return "", partiqlLiteral{}, false
	}

	left := strings.TrimSpace(whereClause[:eqIdx])
	right := strings.TrimSpace(whereClause[eqIdx+1:])
	if left == "" || right == "" {
		return "", partiqlLiteral{}, false
	}

	identifier, ok := parseIdentifierPathTail(left)
	if !ok {
		return "", partiqlLiteral{}, false
	}
	literal, rest, ok := parseLiteral(right)
	if !ok {
		return "", partiqlLiteral{}, false
	}
	if strings.TrimSpace(rest) != "" {
		return "", partiqlLiteral{}, false
	}

	return identifier, literal, true
}

func findEqualityOutsideQuotes(input string) int {
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(input); i++ {
		ch := input[i]
		if inSingleQuote {
			if ch == '\'' {
				if i+1 < len(input) && input[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		}
		if inDoubleQuote {
			if ch == '"' {
				if i+1 < len(input) && input[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		}

		switch ch {
		case '\'':
			inSingleQuote = true
		case '"':
			inDoubleQuote = true
		case '=':
			return i
		}
	}

	return -1
}

func parseIdentifierPathTail(input string) (string, bool) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", false
	}

	parts := strings.Split(input, ".")
	last := strings.TrimSpace(parts[len(parts)-1])
	if last == "" {
		return "", false
	}

	if len(last) >= 2 && last[0] == '"' && last[len(last)-1] == '"' {
		unescaped, _, ok := parseQuotedString(last, '"')
		return unescaped, ok
	}

	if len(last) >= 2 && last[0] == '\'' && last[len(last)-1] == '\'' {
		unescaped, _, ok := parseQuotedString(last, '\'')
		return unescaped, ok
	}

	for _, r := range last {
		if !isIdentifierRune(r) {
			return "", false
		}
	}
	return last, true
}

func parseLiteral(input string) (partiqlLiteral, string, bool) {
	input = strings.TrimSpace(input)
	if input == "" {
		return partiqlLiteral{}, "", false
	}

	if input[0] == '\'' {
		value, rest, ok := parseQuotedString(input, '\'')
		if !ok {
			return partiqlLiteral{}, "", false
		}
		return partiqlLiteral{kind: literalString, textValue: value}, rest, true
	}

	if input[0] == '"' {
		value, rest, ok := parseQuotedString(input, '"')
		if !ok {
			return partiqlLiteral{}, "", false
		}
		return partiqlLiteral{kind: literalString, textValue: value}, rest, true
	}

	if strings.HasPrefix(strings.ToUpper(input), "TRUE") && boundaryAfterToken(input, 4) {
		return partiqlLiteral{kind: literalBoolean, boolValue: true}, strings.TrimSpace(input[4:]), true
	}
	if strings.HasPrefix(strings.ToUpper(input), "FALSE") && boundaryAfterToken(input, 5) {
		return partiqlLiteral{kind: literalBoolean, boolValue: false}, strings.TrimSpace(input[5:]), true
	}
	if strings.HasPrefix(strings.ToUpper(input), "NULL") && boundaryAfterToken(input, 4) {
		return partiqlLiteral{kind: literalNull, isNullable: true}, strings.TrimSpace(input[4:]), true
	}
	if input[0] == '?' {
		return partiqlLiteral{}, "", false
	}

	numberToken := scanNumberToken(input)
	if numberToken == "" {
		return partiqlLiteral{}, "", false
	}
	if _, err := strconv.ParseFloat(numberToken, 64); err != nil {
		return partiqlLiteral{}, "", false
	}
	return partiqlLiteral{
		kind:      literalNumber,
		textValue: numberToken,
	}, strings.TrimSpace(input[len(numberToken):]), true
}

func scanNumberToken(input string) string {
	if input == "" {
		return ""
	}

	end := 0
	for end < len(input) {
		ch := input[end]
		if (ch >= '0' && ch <= '9') || ch == '+' || ch == '-' || ch == '.' || ch == 'e' || ch == 'E' {
			end++
			continue
		}
		break
	}
	if end == 0 {
		return ""
	}
	return input[:end]
}

func boundaryAfterToken(input string, tokenLen int) bool {
	if tokenLen >= len(input) {
		return true
	}
	return !isIdentifierRune(rune(input[tokenLen]))
}

func parseTopLevelObjectLiteral(objectRaw string) (map[string]partiqlLiteral, error) {
	objectRaw = strings.TrimSpace(objectRaw)
	if objectRaw == "" {
		return map[string]partiqlLiteral{}, nil
	}

	inner := objectRaw
	if inner[0] == '{' && inner[len(inner)-1] == '}' {
		inner = strings.TrimSpace(inner[1 : len(inner)-1])
	}

	attributes := make(map[string]partiqlLiteral)
	for len(inner) > 0 {
		key, rest, ok := parseObjectKey(inner)
		if !ok {
			return nil, fmt.Errorf("invalid PartiQL object key")
		}
		rest = strings.TrimSpace(rest)
		if rest == "" || rest[0] != ':' {
			return nil, fmt.Errorf("invalid PartiQL object key/value separator")
		}
		rest = strings.TrimSpace(rest[1:])

		value, remaining, ok := parseLiteral(rest)
		if !ok {
			return nil, fmt.Errorf("unsupported PartiQL literal in object")
		}
		attributes[key] = value

		remaining = strings.TrimSpace(remaining)
		if remaining == "" {
			break
		}
		if remaining[0] != ',' {
			return nil, fmt.Errorf("invalid PartiQL object field separator")
		}
		inner = strings.TrimSpace(remaining[1:])
	}

	return attributes, nil
}

func parseObjectKey(input string) (string, string, bool) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", "", false
	}

	if input[0] == '\'' {
		return parseQuotedString(input, '\'')
	}
	if input[0] == '"' {
		return parseQuotedString(input, '"')
	}
	return parseIdentifier(input)
}

func pickLikelyPartitionKey(attributes map[string]partiqlLiteral) (string, partiqlLiteral, bool) {
	if len(attributes) == 0 {
		return "", partiqlLiteral{}, false
	}

	for _, candidate := range []string{"pk", "id"} {
		for key, value := range attributes {
			if strings.EqualFold(key, candidate) {
				return key, value, true
			}
		}
	}

	if len(attributes) == 1 {
		for key, value := range attributes {
			return key, value, true
		}
	}

	return "", partiqlLiteral{}, false
}

func extractEnclosedBlock(input string, open byte, close byte) (string, bool) {
	input = strings.TrimSpace(input)
	if input == "" || input[0] != open {
		return "", false
	}

	depth := 0
	inSingleQuote := false
	inDoubleQuote := false
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if inSingleQuote {
			if ch == '\'' {
				if i+1 < len(input) && input[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		}
		if inDoubleQuote {
			if ch == '"' {
				if i+1 < len(input) && input[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		}

		switch ch {
		case '\'':
			inSingleQuote = true
		case '"':
			inDoubleQuote = true
		case open:
			depth++
		case close:
			depth--
			if depth == 0 {
				return strings.TrimSpace(input[:i+1]), true
			}
		}
	}

	return "", false
}

func isIdentifierRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' || r == '.'
}
