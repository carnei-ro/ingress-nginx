/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package globalratelimit

import (
	"strings"
	"time"

	"github.com/pkg/errors"
	networking "k8s.io/api/networking/v1beta1"

	"k8s.io/ingress-nginx/internal/ingress/annotations/parser"
	ing_errors "k8s.io/ingress-nginx/internal/ingress/errors"
	"k8s.io/ingress-nginx/internal/ingress/resolver"
	"k8s.io/ingress-nginx/internal/sets"
)

const defaultKey = "$remote_addr"

// Config encapsulates all global rate limit attributes
type Config struct {
	Namespace     string   `json:"namespace"`
	Limit         int      `json:"limit"`
	WindowSize    int      `json:"window-size"`
	Key           string   `json:"key"`
	IgnoredHeader []string `json:"ignored-header"`
}

// Equal tests for equality between two Config types
func (l *Config) Equal(r *Config) bool {
	if l.Namespace != r.Namespace {
		return false
	}
	if l.Limit != r.Limit {
		return false
	}
	if l.WindowSize != r.WindowSize {
		return false
	}
	if l.Key != r.Key {
		return false
	}
	if len(l.IgnoredHeader) != len(r.IgnoredHeader) || !sets.StringElementsMatch(l.IgnoredHeader, r.IgnoredHeader) {
		return false
	}

	return true
}

type globalratelimit struct {
	r resolver.Resolver
}

// NewParser creates a new globalratelimit annotation parser
func NewParser(r resolver.Resolver) parser.IngressAnnotation {
	return globalratelimit{r}
}

// Parse extracts globalratelimit annotations from the given ingress
// and returns them structured as Config type
func (a globalratelimit) Parse(ing *networking.Ingress) (interface{}, error) {
	config := &Config{}

	limit, _ := parser.GetIntAnnotation("global-rate-limit", ing)
	rawWindowSize, _ := parser.GetStringAnnotation("global-rate-limit-window", ing)

	if limit == 0 || len(rawWindowSize) == 0 {
		return config, nil
	}

	windowSize, err := time.ParseDuration(rawWindowSize)
	if err != nil {
		return config, ing_errors.LocationDenied{
			Reason: errors.Wrap(err, "failed to parse 'global-rate-limit-window' value"),
		}
	}

	key, _ := parser.GetStringAnnotation("global-rate-limit-key", ing)
	if len(key) == 0 {
		key = defaultKey
	}

	rawIgnoredHeader, _ := parser.GetStringAnnotation("global-rate-limit-ignored-header", ing)
	ignoredHeader := strings.Split(rawIgnoredHeader, ",")
	for i := range ignoredHeader {
		ignoredHeader[i] = strings.TrimSpace(ignoredHeader[i])
	}

	config.Namespace = strings.Replace(string(ing.UID), "-", "", -1)
	config.Limit = limit
	config.WindowSize = int(windowSize.Seconds())
	config.Key = key
	config.IgnoredHeader = ignoredHeader

	return config, nil
}
