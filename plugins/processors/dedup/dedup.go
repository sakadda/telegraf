//go:generate ../../../tools/readme_config_includer/generator
package dedup

import (
	_ "embed"
	"fmt"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/parsers/influx"
	"github.com/influxdata/telegraf/plugins/processors"
	serializers_influx "github.com/influxdata/telegraf/plugins/serializers/influx"
)

//go:embed sample.conf
var sampleConfig string

type Dedup struct {
	DedupInterval config.Duration `toml:"dedup_interval"`
	FlushTime     time.Time
	Cache         map[uint64]telegraf.Metric
	Log           telegraf.Logger `toml:"-"`
}

// Remove expired items from cache
func (d *Dedup) cleanup() {
	// No need to cleanup cache too often. Lets save some CPU
	if time.Since(d.FlushTime) < time.Duration(d.DedupInterval) {
		return
	}
	d.FlushTime = time.Now()
	keep := make(map[uint64]telegraf.Metric)
	for id, metric := range d.Cache {
		if time.Since(metric.Time()) < time.Duration(d.DedupInterval) {
			keep[id] = metric
		}
	}
	d.Cache = keep
}

// Save item to cache
func (d *Dedup) save(metric telegraf.Metric, id uint64) {
	d.Cache[id] = metric.Copy()
	d.Cache[id].Accept()
}

func (*Dedup) SampleConfig() string {
	return sampleConfig
}

// main processing method
func (d *Dedup) Apply(metrics ...telegraf.Metric) []telegraf.Metric {
	idx := 0
	for _, metric := range metrics {
		id := metric.HashID()
		m, ok := d.Cache[id]

		if !ok {
			d.save(metric, id)
			metrics[idx] = metric
			idx++
			continue
		}

		if time.Since(m.Time()) >= time.Duration(d.DedupInterval) {
			// Интервал истёк, отправляем метрику полностью
			d.save(metric, id)
			metrics[idx] = metric
			idx++
			continue
		} else {
			// Интервал не истёк, проверяем изменившиеся поля
			// Список полей для удаления (неизменившиеся поля)
			fieldsToRemove := []string{}
			for _, f := range metric.FieldList() {
				if value, ok := m.GetField(f.Key); ok && value == f.Value {
					// Значение поля не изменилось, помечаем для удаления
					fieldsToRemove = append(fieldsToRemove, f.Key)
				} else {
					// Значение поля изменилось или новое поле, обновляем кэш
					m.AddField(f.Key, f.Value)
				}
			}

			// Удаляем неизменившиеся поля из метрики
			for _, fieldKey := range fieldsToRemove {
				metric.RemoveField(fieldKey)
			}

			if len(metric.FieldList()) > 0 {
				// Есть изменившиеся поля, обновляем время в кэше и сохраняем метрику
				m.SetTime(metric.Time())
				d.Cache[id] = m
				metrics[idx] = metric
				idx++
			} else {
				// Нет изменившихся полей, удаляем метрику
				metric.Drop()
			}
		}
	}
	metrics = metrics[:idx]
	d.cleanup()
	return metrics
}

func (d *Dedup) GetState() interface{} {
	s := &serializers_influx.Serializer{}
	v := make([]telegraf.Metric, 0, len(d.Cache))
	for _, value := range d.Cache {
		v = append(v, value)
	}
	state, err := s.SerializeBatch(v)
	if err != nil {
		d.Log.Errorf("dedup processor failed to serialize metric batch: %v", err)
	}
	return state
}

func (d *Dedup) SetState(state interface{}) error {
	p := &influx.Parser{}
	if err := p.Init(); err != nil {
		return err
	}
	data, ok := state.([]byte)
	if !ok {
		return fmt.Errorf("state has wrong type %T", state)
	}
	metrics, err := p.Parse(data)
	if err == nil {
		d.Apply(metrics...)
	}
	return nil
}

func init() {
	processors.Add("dedup", func() telegraf.Processor {
		return &Dedup{
			DedupInterval: config.Duration(10 * time.Minute),
			FlushTime:     time.Now(),
			Cache:         make(map[uint64]telegraf.Metric),
		}
	})
}
