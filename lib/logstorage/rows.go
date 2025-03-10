package logstorage

import (
	"fmt"
	"slices"

	"github.com/valyala/quicktemplate"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/bytesutil"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/encoding"
)

// Field is a single field for the log entry.
type Field struct {
	// Name is the name of the field
	Name string

	// Value is the value of the field
	Value string
}

// Reset resets f for future re-use.
func (f *Field) Reset() {
	f.Name = ""
	f.Value = ""
}

// String returns string representation of f.
func (f *Field) String() string {
	x := f.marshalToJSON(nil)
	return string(x)
}

func (f *Field) marshal(dst []byte, marshalFieldName bool) []byte {
	if marshalFieldName {
		dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(f.Name))
	}
	dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(f.Value))
	return dst
}

func (f *Field) unmarshalNoArena(src []byte, unmarshalFieldName bool) ([]byte, error) {
	srcOrig := src

	// Unmarshal field name
	if unmarshalFieldName {
		name, nSize := encoding.UnmarshalBytes(src)
		if nSize <= 0 {
			return srcOrig, fmt.Errorf("cannot unmarshal field name")
		}
		src = src[nSize:]
		f.Name = bytesutil.ToUnsafeString(name)
	}

	// Unmarshal field value
	value, nSize := encoding.UnmarshalBytes(src)
	if nSize <= 0 {
		return srcOrig, fmt.Errorf("cannot unmarshal field value")
	}
	src = src[nSize:]
	f.Value = bytesutil.ToUnsafeString(value)

	return src, nil
}

func (f *Field) marshalToJSON(dst []byte) []byte {
	name := f.Name
	if name == "" {
		name = "_msg"
	}
	dst = quicktemplate.AppendJSONString(dst, name, true)
	dst = append(dst, ':')
	dst = quicktemplate.AppendJSONString(dst, f.Value, true)
	return dst
}

func (f *Field) marshalToLogfmt(dst []byte) []byte {
	name := f.Name
	if name == "" {
		name = "_msg"
	}
	dst = append(dst, name...)
	dst = append(dst, '=')
	if needLogfmtQuoting(f.Value) {
		dst = quicktemplate.AppendJSONString(dst, f.Value, true)
	} else {
		dst = append(dst, f.Value...)
	}
	return dst
}

func getFieldValue(fields []Field, name string) string {
	for _, f := range fields {
		if f.Name == name {
			return f.Value
		}
	}
	return ""
}

func needLogfmtQuoting(s string) bool {
	for _, c := range s {
		if isLogfmtSpecialChar(c) {
			return true
		}
	}
	return false
}

func isLogfmtSpecialChar(c rune) bool {
	if c <= 0x20 {
		return true
	}
	switch c {
	case '"', '\\':
		return true
	default:
		return false
	}
}

// RenameField renames the first non-empty field with the name from oldNames list to newName in Fields
func RenameField(fields []Field, oldNames []string, newName string) {
	if len(oldNames) == 0 {
		// Nothing to rename
		return
	}
	for i := range fields {
		f := &fields[i]
		if f.Value != "" && slices.Contains(oldNames, f.Name) {
			f.Name = newName
			return
		}
	}
}

// MarshalFieldsToJSON appends JSON-marshaled fields to dst and returns the result.
func MarshalFieldsToJSON(dst []byte, fields []Field) []byte {
	fields = SkipLeadingFieldsWithoutValues(fields)
	dst = append(dst, '{')
	if len(fields) > 0 {
		dst = fields[0].marshalToJSON(dst)
		fields = fields[1:]
		for i := range fields {
			f := &fields[i]
			if f.Value == "" {
				// Skip fields without values
				continue
			}
			dst = append(dst, ',')
			dst = f.marshalToJSON(dst)
		}
	}
	dst = append(dst, '}')
	return dst
}

// MarshalFieldsToLogfmt appends logfmt-marshaled fields to dst and returns the result.
func MarshalFieldsToLogfmt(dst []byte, fields []Field) []byte {
	if len(fields) == 0 {
		return dst
	}
	dst = fields[0].marshalToLogfmt(dst)
	fields = fields[1:]
	for i := range fields {
		dst = append(dst, ' ')
		dst = fields[i].marshalToLogfmt(dst)
	}
	return dst
}

// SkipLeadingFieldsWithoutValues skips leading fields without values.
func SkipLeadingFieldsWithoutValues(fields []Field) []Field {
	i := 0
	for i < len(fields) && fields[i].Value == "" {
		i++
	}
	return fields[i:]
}

func appendFields(a *arena, dst, src []Field) []Field {
	for _, f := range src {
		dst = append(dst, Field{
			Name:  a.copyString(f.Name),
			Value: a.copyString(f.Value),
		})
	}
	return dst
}

// rows is an aux structure used during rows merge
type rows struct {
	fieldsBuf []Field

	timestamps []int64

	rows [][]Field
}

// reset resets rs
func (rs *rows) reset() {
	fb := rs.fieldsBuf
	for i := range fb {
		fb[i].Reset()
	}
	rs.fieldsBuf = fb[:0]

	rs.timestamps = rs.timestamps[:0]

	rows := rs.rows
	for i := range rows {
		rows[i] = nil
	}
	rs.rows = rows[:0]
}

// appendRows appends rows with the given timestamps to rs.
func (rs *rows) appendRows(timestamps []int64, rows [][]Field) {
	rs.timestamps = append(rs.timestamps, timestamps...)

	fieldsBuf := rs.fieldsBuf
	for _, fields := range rows {
		fieldsLen := len(fieldsBuf)
		fieldsBuf = append(fieldsBuf, fields...)
		rs.rows = append(rs.rows, fieldsBuf[fieldsLen:])
	}
	rs.fieldsBuf = fieldsBuf
}

// mergeRows merges the args and appends them to rs.
func (rs *rows) mergeRows(timestampsA, timestampsB []int64, fieldsA, fieldsB [][]Field) {
	for len(timestampsA) > 0 && len(timestampsB) > 0 {
		i := 0
		minTimestamp := timestampsB[0]
		for i < len(timestampsA) && timestampsA[i] <= minTimestamp {
			i++
		}
		rs.appendRows(timestampsA[:i], fieldsA[:i])
		fieldsA = fieldsA[i:]
		timestampsA = timestampsA[i:]

		fieldsA, fieldsB = fieldsB, fieldsA
		timestampsA, timestampsB = timestampsB, timestampsA
	}
	if len(timestampsA) == 0 {
		rs.appendRows(timestampsB, fieldsB)
	} else {
		rs.appendRows(timestampsA, fieldsA)
	}
}
