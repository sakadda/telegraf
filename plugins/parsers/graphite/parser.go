package graphite

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal/templating"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/parsers"
)

// Minimum and maximum supported dates for timestamps.
var (
	MinDate = time.Date(1901, 12, 13, 0, 0, 0, 0, time.UTC)
	MaxDate = time.Date(2038, 1, 19, 0, 0, 0, 0, time.UTC)
)

type Parser struct {
	Separator   string            `toml:"separator"`
	Templates   []string          `toml:"templates"`
	DefaultTags map[string]string ` toml:"-"`

	templateEngine *templating.Engine
}

func (p *Parser) Init() error {
	// Set defaults
	if p.Separator == "" {
		p.Separator = DefaultSeparator
	}

	defaultTemplate, err := templating.NewDefaultTemplateWithPattern("measurement*")
	if err != nil {
		return fmt.Errorf("creating template failed: %w", err)
	}

	p.templateEngine, err = templating.NewEngine(p.Separator, defaultTemplate, p.Templates)
	if err != nil {
		return fmt.Errorf("creating template engine failed: %w ", err)
	}

	return nil
}

func (p *Parser) Parse(buf []byte) ([]telegraf.Metric, error) {
	// parse even if the buffer begins with a newline
	if len(buf) != 0 && buf[0] == '\n' {
		buf = buf[1:]
	}

	var metrics []telegraf.Metric
	var errs []string

	for {
		n := bytes.IndexByte(buf, '\n')
		var line []byte
		if n >= 0 {
			line = bytes.TrimSpace(buf[:n:n])
		} else {
			line = bytes.TrimSpace(buf) // last line
		}
		if len(line) != 0 {
			m, err := p.ParseLine(string(line))
			if err == nil {
				metrics = append(metrics, m)
			} else {
				errs = append(errs, err.Error())
			}
		}
		if n < 0 {
			break
		}
		buf = buf[n+1:]
	}
	if len(errs) != 0 {
		return metrics, errors.New(strings.Join(errs, "\n"))
	}
	return metrics, nil
}

// ParseLine performs Graphite parsing of a single line.
func (p *Parser) ParseLine(line string) (telegraf.Metric, error) {
	// Break into 3 fields (name, value, timestamp).
	fields := strings.Fields(line)
	if len(fields) != 2 && len(fields) != 3 {
		return nil, fmt.Errorf("received %q which doesn't have required fields", line)
	}

	parts := strings.Split(fields[0], ";")

	// decode the name and tags
	measurement, tags, field, err := p.templateEngine.Apply(parts[0])
	if err != nil {
		return nil, err
	}

	// Could not extract measurement, use the raw value
	if measurement == "" {
		measurement = parts[0]
	}

	// Parse value.
	v, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return nil, fmt.Errorf(`field %q value: %w`, fields[0], err)
	}

	fieldValues := make(map[string]interface{}, 1)
	if field != "" {
		fieldValues[field] = v
	} else {
		fieldValues["value"] = v
	}

	// If no 3rd field, use now as timestamp
	timestamp := time.Now().UTC()

	if len(fields) == 3 {
		// Parse timestamp.
		unixTime, err := strconv.ParseFloat(fields[2], 64)
		if err != nil {
			return nil, fmt.Errorf(`field %q time: %w`, fields[0], err)
		}

		// -1 is a special value that gets converted to current UTC time
		// See https://github.com/graphite-project/carbon/issues/54
		if unixTime != float64(-1) {
			// Check if we have fractional seconds
			timestamp = time.Unix(int64(unixTime), int64((unixTime-math.Floor(unixTime))*float64(time.Second)))
			if timestamp.Before(MinDate) || timestamp.After(MaxDate) {
				return nil, errors.New("timestamp out of range")
			}
		}
	}

	// Split name and tags
	if len(parts) >= 2 {
		for _, tag := range parts[1:] {
			tagValue := strings.Split(tag, "=")
			if len(tagValue) != 2 || len(tagValue[0]) == 0 || len(tagValue[1]) == 0 {
				continue
			}
			if strings.ContainsAny(tagValue[0], "!^") {
				continue
			}
			if strings.Index(tagValue[1], "~") == 0 {
				continue
			}
			tags[tagValue[0]] = tagValue[1]
		}
	}

	// Set the default tags on the point if they are not already set
	for k, v := range p.DefaultTags {
		if _, ok := tags[k]; !ok {
			tags[k] = v
		}
	}

	return metric.New(measurement, tags, fieldValues, timestamp), nil
}

// ApplyTemplate extracts the template fields from the given line and
// returns the measurement name and tags.
//
//nolint:revive // This function should be eliminated anyway
func (p *Parser) ApplyTemplate(line string) (string, map[string]string, string, error) {
	// Break line into fields (name, value, timestamp), only name is used
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return "", make(map[string]string), "", nil
	}
	// decode the name and tags
	name, tags, field, err := p.templateEngine.Apply(fields[0])

	// Set the default tags on the point if they are not already set
	for k, v := range p.DefaultTags {
		if _, ok := tags[k]; !ok {
			tags[k] = v
		}
	}

	return name, tags, field, err
}

func (p *Parser) SetDefaultTags(tags map[string]string) {
	p.DefaultTags = tags
}

func init() {
	parsers.Add("graphite", func(string) telegraf.Parser { return &Parser{} })
}
