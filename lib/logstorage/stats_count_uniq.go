package logstorage

import (
	"fmt"
	"strconv"
	"unsafe"

	"github.com/cespare/xxhash/v2"

	"github.com/zzylol/VictoriaMetrics-cluster/lib/bytesutil"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/encoding"
)

type statsCountUniq struct {
	fields []string
	limit  uint64
}

func (su *statsCountUniq) String() string {
	s := "count_uniq(" + statsFuncFieldsToString(su.fields) + ")"
	if su.limit > 0 {
		s += fmt.Sprintf(" limit %d", su.limit)
	}
	return s
}

func (su *statsCountUniq) updateNeededFields(neededFields fieldsSet) {
	updateNeededFieldsForStatsFunc(neededFields, su.fields)
}

func (su *statsCountUniq) newStatsProcessor() (statsProcessor, int) {
	sup := &statsCountUniqProcessor{
		su: su,

		m: make(map[string]struct{}),
	}
	return sup, int(unsafe.Sizeof(*sup))
}

type statsCountUniqProcessor struct {
	su *statsCountUniq

	// m holds unique values if it is non-nil
	m map[string]struct{}

	// mHash holds unique value hashes if it is non-nil
	mHash map[uint64]struct{}

	columnValues [][]string
	keyBuf       []byte
	tmpNum       int
}

func (sup *statsCountUniqProcessor) updateStatsForAllRows(br *blockResult) int {
	if sup.limitReached() {
		return 0
	}

	fields := sup.su.fields

	stateSizeIncrease := 0
	if len(fields) == 0 {
		// Count unique rows
		cs := br.getColumns()

		columnValues := sup.columnValues[:0]
		for _, c := range cs {
			values := c.getValues(br)
			columnValues = append(columnValues, values)
		}
		sup.columnValues = columnValues

		keyBuf := sup.keyBuf[:0]
		for i := 0; i < br.rowsLen; i++ {
			seenKey := true
			for _, values := range columnValues {
				if i == 0 || values[i-1] != values[i] {
					seenKey = false
					break
				}
			}
			if seenKey {
				// This key has been already counted.
				continue
			}

			allEmptyValues := true
			keyBuf = keyBuf[:0]
			for j, values := range columnValues {
				v := values[i]
				if v != "" {
					allEmptyValues = false
				}
				// Put column name into key, since every block can contain different set of columns for '*' selector.
				keyBuf = encoding.MarshalBytes(keyBuf, bytesutil.ToUnsafeBytes(cs[j].name))
				keyBuf = encoding.MarshalBytes(keyBuf, bytesutil.ToUnsafeBytes(v))
			}
			if allEmptyValues {
				// Do not count empty values
				continue
			}
			stateSizeIncrease += sup.updateState(keyBuf)
		}
		sup.keyBuf = keyBuf
		return stateSizeIncrease
	}
	if len(fields) == 1 {
		// Fast path for a single column.
		// The unique key is formed as "<is_time> <value>",
		// This guarantees that keys do not clash for different column types across blocks.
		c := br.getColumnByName(fields[0])
		if c.isTime {
			// Count unique timestamps
			timestamps := br.getTimestamps()
			keyBuf := sup.keyBuf[:0]
			for i, timestamp := range timestamps {
				if i > 0 && timestamps[i-1] == timestamps[i] {
					// This timestamp has been already counted.
					continue
				}
				keyBuf = append(keyBuf[:0], 1)
				keyBuf = encoding.MarshalInt64(keyBuf, timestamp)
				stateSizeIncrease += sup.updateState(keyBuf)
			}
			sup.keyBuf = keyBuf
			return stateSizeIncrease
		}
		if c.isConst {
			// count unique const values
			v := c.valuesEncoded[0]
			if v == "" {
				// Do not count empty values
				return stateSizeIncrease
			}
			keyBuf := sup.keyBuf[:0]
			keyBuf = append(keyBuf[:0], 0)
			keyBuf = append(keyBuf, v...)
			stateSizeIncrease += sup.updateState(keyBuf)
			sup.keyBuf = keyBuf
			return stateSizeIncrease
		}
		if c.valueType == valueTypeDict {
			// count unique non-zero dict values for the selected logs
			sup.tmpNum = 0
			c.forEachDictValue(br, func(v string) {
				if v == "" {
					// Do not count empty values
					return
				}
				keyBuf := append(sup.keyBuf[:0], 0)
				keyBuf = append(keyBuf, v...)
				sup.tmpNum += sup.updateState(keyBuf)
				sup.keyBuf = keyBuf
			})
			stateSizeIncrease += sup.tmpNum
			return stateSizeIncrease
		}

		// Count unique values across values
		values := c.getValues(br)
		keyBuf := sup.keyBuf[:0]
		for i, v := range values {
			if v == "" {
				// Do not count empty values
				continue
			}
			if i > 0 && values[i-1] == v {
				// This value has been already counted.
				continue
			}
			keyBuf = append(keyBuf[:0], 0)
			keyBuf = append(keyBuf, v...)
			stateSizeIncrease += sup.updateState(keyBuf)
		}
		sup.keyBuf = keyBuf
		return stateSizeIncrease
	}

	// Slow path for multiple columns.

	// Pre-calculate column values for byFields in order to speed up building group key in the loop below.
	columnValues := sup.columnValues[:0]
	for _, f := range fields {
		c := br.getColumnByName(f)
		values := c.getValues(br)
		columnValues = append(columnValues, values)
	}
	sup.columnValues = columnValues

	keyBuf := sup.keyBuf[:0]
	for i := 0; i < br.rowsLen; i++ {
		seenKey := true
		for _, values := range columnValues {
			if i == 0 || values[i-1] != values[i] {
				seenKey = false
				break
			}
		}
		if seenKey {
			continue
		}

		allEmptyValues := true
		keyBuf = keyBuf[:0]
		for _, values := range columnValues {
			v := values[i]
			if v != "" {
				allEmptyValues = false
			}
			keyBuf = encoding.MarshalBytes(keyBuf, bytesutil.ToUnsafeBytes(v))
		}
		if allEmptyValues {
			// Do not count empty values
			continue
		}
		stateSizeIncrease += sup.updateState(keyBuf)
	}
	sup.keyBuf = keyBuf
	return stateSizeIncrease
}

func (sup *statsCountUniqProcessor) updateStatsForRow(br *blockResult, rowIdx int) int {
	if sup.limitReached() {
		return 0
	}

	fields := sup.su.fields

	stateSizeIncrease := 0
	if len(fields) == 0 {
		// Count unique rows
		allEmptyValues := true
		keyBuf := sup.keyBuf[:0]
		for _, c := range br.getColumns() {
			v := c.getValueAtRow(br, rowIdx)
			if v != "" {
				allEmptyValues = false
			}
			// Put column name into key, since every block can contain different set of columns for '*' selector.
			keyBuf = encoding.MarshalBytes(keyBuf, bytesutil.ToUnsafeBytes(c.name))
			keyBuf = encoding.MarshalBytes(keyBuf, bytesutil.ToUnsafeBytes(v))
		}
		sup.keyBuf = keyBuf

		if allEmptyValues {
			// Do not count empty values
			return stateSizeIncrease
		}
		stateSizeIncrease += sup.updateState(keyBuf)
		return stateSizeIncrease
	}
	if len(fields) == 1 {
		// Fast path for a single column.
		// The unique key is formed as "<is_time> <value>",
		// This guarantees that keys do not clash for different column types across blocks.
		c := br.getColumnByName(fields[0])
		if c.isTime {
			// Count unique timestamps
			timestamps := br.getTimestamps()
			keyBuf := sup.keyBuf[:0]
			keyBuf = append(keyBuf[:0], 1)
			keyBuf = encoding.MarshalInt64(keyBuf, timestamps[rowIdx])
			stateSizeIncrease += sup.updateState(keyBuf)
			sup.keyBuf = keyBuf
			return stateSizeIncrease
		}
		if c.isConst {
			// count unique const values
			v := c.valuesEncoded[0]
			if v == "" {
				// Do not count empty values
				return stateSizeIncrease
			}
			keyBuf := sup.keyBuf[:0]
			keyBuf = append(keyBuf[:0], 0)
			keyBuf = append(keyBuf, v...)
			stateSizeIncrease += sup.updateState(keyBuf)
			sup.keyBuf = keyBuf
			return stateSizeIncrease
		}
		if c.valueType == valueTypeDict {
			// count unique non-zero c.dictValues
			valuesEncoded := c.getValuesEncoded(br)
			dictIdx := valuesEncoded[rowIdx][0]
			v := c.dictValues[dictIdx]
			if v == "" {
				// Do not count empty values
				return stateSizeIncrease
			}
			keyBuf := sup.keyBuf[:0]
			keyBuf = append(keyBuf[:0], 0)
			keyBuf = append(keyBuf, v...)
			stateSizeIncrease += sup.updateState(keyBuf)
			sup.keyBuf = keyBuf
			return stateSizeIncrease
		}

		// Count unique values for the given rowIdx
		v := c.getValueAtRow(br, rowIdx)
		if v == "" {
			// Do not count empty values
			return stateSizeIncrease
		}
		keyBuf := sup.keyBuf[:0]
		keyBuf = append(keyBuf[:0], 0)
		keyBuf = append(keyBuf, v...)
		stateSizeIncrease += sup.updateState(keyBuf)
		sup.keyBuf = keyBuf
		return stateSizeIncrease
	}

	// Slow path for multiple columns.
	allEmptyValues := true
	keyBuf := sup.keyBuf[:0]
	for _, f := range fields {
		c := br.getColumnByName(f)
		v := c.getValueAtRow(br, rowIdx)
		if v != "" {
			allEmptyValues = false
		}
		keyBuf = encoding.MarshalBytes(keyBuf, bytesutil.ToUnsafeBytes(v))
	}
	sup.keyBuf = keyBuf

	if allEmptyValues {
		// Do not count empty values
		return stateSizeIncrease
	}
	stateSizeIncrease += sup.updateState(keyBuf)
	return stateSizeIncrease
}

func (sup *statsCountUniqProcessor) mergeState(sfp statsProcessor) {
	if sup.limitReached() {
		return
	}

	src := sfp.(*statsCountUniqProcessor)

	if sup.mHash != nil {
		m := sup.mHash
		for h := range src.mHash {
			if _, ok := m[h]; !ok {
				m[h] = struct{}{}
			}
		}
	} else {
		m := sup.m
		for k := range src.m {
			if _, ok := m[k]; !ok {
				m[k] = struct{}{}
			}
		}
	}
}

func (sup *statsCountUniqProcessor) finalizeStats(dst []byte) []byte {
	var n uint64
	if sup.mHash != nil {
		n = uint64(len(sup.mHash))
	} else {
		n = uint64(len(sup.m))
	}
	if limit := sup.su.limit; limit > 0 && n > limit {
		n = limit
	}
	return strconv.AppendUint(dst, n, 10)
}

func (sup *statsCountUniqProcessor) updateState(v []byte) int {
	stateSizeIncrease := 0

	if sup.mHash != nil {
		h := xxhash.Sum64(v)
		if _, ok := sup.mHash[h]; !ok {
			sup.mHash[h] = struct{}{}
			stateSizeIncrease += int(unsafe.Sizeof(h))
		}
	} else {
		if _, ok := sup.m[string(v)]; !ok {
			sup.m[string(v)] = struct{}{}
			stateSizeIncrease += len(v) + int(unsafe.Sizeof(""))
		}
	}

	return stateSizeIncrease
}

func (sup *statsCountUniqProcessor) limitReached() bool {
	limit := sup.su.limit
	if limit <= 0 {
		return false
	}
	if sup.mHash != nil {
		return uint64(len(sup.mHash)) > limit
	}
	return uint64(len(sup.m)) > limit
}

func parseStatsCountUniq(lex *lexer) (*statsCountUniq, error) {
	fields, err := parseStatsFuncFields(lex, "count_uniq")
	if err != nil {
		return nil, err
	}
	su := &statsCountUniq{
		fields: fields,
	}
	if lex.isKeyword("limit") {
		lex.nextToken()
		n, ok := tryParseUint64(lex.token)
		if !ok {
			return nil, fmt.Errorf("cannot parse 'limit %s' for 'count_uniq': %w", lex.token, err)
		}
		lex.nextToken()
		su.limit = n
	}
	return su, nil
}
