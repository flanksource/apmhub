package logs

import "testing"

func TestSearchRoute_Match(t *testing.T) {
	type fields struct {
		Type       string
		IdPrefix   string
		Labels     map[string]string
		IsAdditive bool
	}

	tests := []struct {
		name   string
		fields fields
		args   *SearchParams
		want   bool
	}{
		{
			name: "match",
			fields: fields{
				Type:     "pod",
				IdPrefix: "search",
				Labels: map[string]string{
					"app": "search",
				},
			},
			args: &SearchParams{Type: "pod", Id: "search-1234", Labels: map[string]string{"app": "search"}},
			want: true,
		},
		{
			name: "not match - label",
			fields: fields{
				Type:     "pod",
				IdPrefix: "search",
				Labels: map[string]string{
					"app": "backend",
				},
			},
			args: &SearchParams{Type: "pod", Id: "search-1234", Labels: map[string]string{"app": "search"}},
			want: false,
		},
		{
			name: "not match - type",
			fields: fields{
				Type:     "pod",
				IdPrefix: "search",
				Labels: map[string]string{
					"app": "search",
				},
			},
			args: &SearchParams{Type: "node", Id: "search-1234", Labels: map[string]string{"app": "search"}},
			want: false,
		},
		{
			name: "not match - prefix",
			fields: fields{
				Type:     "pod",
				IdPrefix: "search",
				Labels: map[string]string{
					"app": "search",
				},
			},
			args: &SearchParams{Type: "pod", Id: "pod-1234", Labels: map[string]string{"app": "search"}},
			want: false,
		},
		{
			name: "not match - all labels required",
			fields: fields{
				Type:     "node",
				IdPrefix: "search",
				Labels: map[string]string{
					"app":  "backend",
					"host": "flanksource.com",
				},
			},
			args: &SearchParams{Type: "node", Id: "search-1234", Labels: map[string]string{"app": "backend"}},
			want: false,
		},
		{
			name: "match - query has additional labels",
			fields: fields{
				Type:     "node",
				IdPrefix: "search",
				Labels: map[string]string{
					"app":  "backend",
					"host": "flanksource.com",
				},
			},
			args: &SearchParams{Type: "node",
				Id: "search-1234",
				Labels: map[string]string{
					"app":    "backend",
					"host":   "flanksource.com",
					"system": "linux",
				},
			},
			want: true,
		},
		{
			name: "match - labels matching - one label matching",
			fields: fields{
				Type:     "node",
				IdPrefix: "search",
				Labels: map[string]string{
					"app": "backend,frontend",
				},
			},
			args: &SearchParams{Type: "node",
				Id: "search-1234",
				Labels: map[string]string{
					"app":    "backend",
					"host":   "flanksource.com",
					"system": "linux",
				},
			},
			want: true,
		},
		{
			name: "match - labels matching - last label matching",
			fields: fields{
				Type:     "node",
				IdPrefix: "search",
				Labels: map[string]string{
					"app": "backend,frontend",
				},
			},
			args: &SearchParams{Type: "node",
				Id: "search-1234",
				Labels: map[string]string{
					"app":    "frontend",
					"host":   "flanksource.com",
					"system": "linux",
				},
			},
			want: true,
		},
		{
			name: "match - labels matching - no labels matching",
			fields: fields{
				Type:     "node",
				IdPrefix: "search",
				Labels: map[string]string{
					"app": "backend,frontend",
				},
			},
			args: &SearchParams{Type: "node",
				Id: "search-1234",
				Labels: map[string]string{
					"app":    "search",
					"host":   "flanksource.com",
					"system": "linux",
				},
			},
			want: false,
		},
		{
			name: "match - labels matching - wildcard",
			fields: fields{
				Type:     "node",
				IdPrefix: "search",
				Labels: map[string]string{
					"app": "*",
				},
			},
			args: &SearchParams{Type: "node",
				Id: "search-1234",
				Labels: map[string]string{
					"app":    "frontend",
					"host":   "flanksource.com",
					"system": "linux",
				},
			},
			want: true,
		},
		{
			name: "match - labels matching - negate",
			fields: fields{
				Type:     "node",
				IdPrefix: "search",
				Labels: map[string]string{
					"app": "!frontend",
				},
			},
			args: &SearchParams{Type: "node",
				Id: "search-1234",
				Labels: map[string]string{
					"app":    "frontend",
					"host":   "flanksource.com",
					"system": "linux",
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &SearchRoute{
				Type:       tt.fields.Type,
				IdPrefix:   tt.fields.IdPrefix,
				Labels:     tt.fields.Labels,
				IsAdditive: tt.fields.IsAdditive,
			}
			if got := tr.Match(tt.args); got != tt.want {
				t.Errorf("SearchRoute.Match() = %v, want %v", got, tt.want)
			}
		})
	}
}
