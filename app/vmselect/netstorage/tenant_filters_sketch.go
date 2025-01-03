package netstorage

import (
	"fmt"
	"strings"

	"github.com/zzylol/VictoriaMetrics-cluster/app/vmselect/searchutils"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/prompbmarshal"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/promrelabel"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/querytracer"
	"github.com/zzylol/VictoriaMetrics-cluster/lib/sketch"
)

// GetTenantTokensFromFilters returns the list of tenant tokens and the list of filters without tenant filters.
func GetTenantTokensFromFiltersSketch(qt *querytracer.Tracer, tr sketch.TimeRange, tfs [][]sketch.TagFilter, deadline searchutils.Deadline) ([]sketch.TenantToken, [][]sketch.TagFilter, error) {
	tenants, err := TenantsCachedSketch(qt, tr, deadline)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot obtain tenants: %w", err)
	}

	tenantFilters, otherFilters := splitFiltersByTypeSketch(tfs)

	tts, err := applyFiltersToTenantsSketch(tenants, tenantFilters)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot apply filters to tenants: %w", err)
	}

	return tts, otherFilters, nil
}

func splitFiltersByTypeSketch(tfs [][]sketch.TagFilter) ([][]sketch.TagFilter, [][]sketch.TagFilter) {
	if len(tfs) == 0 {
		return nil, tfs
	}

	tenantFilters := make([][]sketch.TagFilter, 0, len(tfs))
	otherFilters := make([][]sketch.TagFilter, 0, len(tfs))
	for _, f := range tfs {
		ffs := make([]sketch.TagFilter, 0, len(f))
		offs := make([]sketch.TagFilter, 0, len(f))
		for _, tf := range f {
			if !isTenancyLabel(string(tf.Key)) {
				offs = append(offs, tf)
				continue
			}
			ffs = append(ffs, tf)
		}

		if len(ffs) > 0 {
			tenantFilters = append(tenantFilters, ffs)
		}
		if len(offs) > 0 {
			otherFilters = append(otherFilters, offs)
		}
	}
	return tenantFilters, otherFilters
}

// ApplyTenantFiltersToTagFilters applies the given tenant filters to the given tag filters.
func ApplyTenantFiltersToTagFiltersSketch(tts []sketch.TenantToken, tfs [][]sketch.TagFilter) ([]sketch.TenantToken, [][]sketch.TagFilter) {
	tenantFilters, otherFilters := splitFiltersByTypeSketch(tfs)
	if len(tenantFilters) == 0 {
		return tts, otherFilters
	}

	tts, err := applyFiltersToTenantsSketch(tts, tenantFilters)
	if err != nil {
		return nil, nil
	}
	return tts, otherFilters
}

func tagFiltersToStringSketch(tfs []sketch.TagFilter) string {
	a := make([]string, len(tfs))
	for i, tf := range tfs {
		a[i] = tf.String()
	}
	return "{" + strings.Join(a, ",") + "}"
}

// applyFiltersToTenants applies the given filters to the given tenants.
// It returns the filtered tenants.
func applyFiltersToTenantsSketch(tenants []sketch.TenantToken, filters [][]sketch.TagFilter) ([]sketch.TenantToken, error) {
	// fast path - return all tenants if no filters are given
	if len(filters) == 0 {
		return tenants, nil
	}

	resultingTokens := make([]sketch.TenantToken, 0, len(tenants))
	lbs := make([][]prompbmarshal.Label, 0, len(filters))
	lbsAux := make([]prompbmarshal.Label, 0, len(filters))
	for _, token := range tenants {
		lbsAuxLen := len(lbsAux)
		lbsAux = append(lbsAux, prompbmarshal.Label{
			Name:  "vm_account_id",
			Value: fmt.Sprintf("%d", token.AccountID),
		}, prompbmarshal.Label{
			Name:  "vm_project_id",
			Value: fmt.Sprintf("%d", token.ProjectID),
		})

		lbs = append(lbs, lbsAux[lbsAuxLen:])
	}

	promIfs := make([]promrelabel.IfExpression, len(filters))
	for i, tags := range filters {
		filter := tagFiltersToStringSketch(tags)
		err := promIfs[i].Parse(filter)
		if err != nil {
			return nil, fmt.Errorf("cannot parse if expression from filters %v: %s", filter, err)
		}
	}

	for i, lb := range lbs {
		for _, promIf := range promIfs {
			if promIf.Match(lb) {
				resultingTokens = append(resultingTokens, tenants[i])
				break
			}
		}
	}

	return resultingTokens, nil
}
