package transfer

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	client "github.com/influxdata/influxdb1-client"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

func NewTrans(database string, start, end time.Time, step time.Duration, p v1.API, i *client.Client, c, retry int, monitorLabel string, precision string, metrics string) *Trans {
	return &Trans{
		Database:     database,
		Start:        start,
		End:          end,
		Step:         step,
		p:            p,
		i:            i,
		C:            c,
		Retry:        retry,
		MonitorLabel: monitorLabel,
		Precision:    precision,
		Metrics:      metrics,
	}
}

type Trans struct {
	Database     string
	Start        time.Time
	End          time.Time
	Step         time.Duration
	p            v1.API
	i            *client.Client
	C            int
	Retry        int
	MonitorLabel string
	Precision    string
	Metrics      string
}

func (t *Trans) Run(ctx context.Context) error {

	var names model.LabelValues
	var err error
	if t.Metrics == "" {
		names = model.LabelValues{model.LabelValue(t.Metrics)}

	} else {
		for _, l := range strings.Split(t.Metrics, ",") {
			names = append(names, model.LabelValue(l))
		}
		if err != nil {
			return err
		}
	}

	if t.End.IsZero() {
		t.End = time.Now()
	}
	if t.Step == 0 {
		t.Step = time.Minute
	}
	if t.Start.IsZero() {
		flags, err := t.p.Flags(ctx)
		if err != nil {
			return err
		}
		v, ok := flags["storage.tsdb.retention"]
		if !ok {
			panic("storage.tsdb.retention not found")
		}
		if v[len(v)-1] == 'd' {
			a, err := strconv.Atoi(v[0 : len(v)-1])
			if err != nil {
				return err
			}
			v = strconv.Itoa(a*24) + "h"
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			return err
		}
		t.Start = time.Now().Add(-d)
	}
	if err != nil {
		return err
	}
	if t.C == 0 {
		t.C = 1
	}
	v, _ := json.Marshal(t)
	log.Println(string(v))
	c := make(chan struct{}, t.C)
	wg := sync.WaitGroup{}
	wg.Add(len(names))
	for _, i := range names {
		c <- struct{}{}
		go func(i string) {
			log.Println("start ", i)
			err := t.runOne(string(i))
			if err != nil {
				log.Fatal(err)
			}
			log.Println("done ", i)
			wg.Done()
			<-c
		}(string(i))
	}
	wg.Wait()
	return nil
}

func (t *Trans) runOne(name string) error {
	start := t.Start
	finish := t.End
	for start.Before(finish) {
		end := start.Add(t.Step * 60 * 1)
		log.Println(name+"...", start.Format(time.RFC3339), end.Format(time.RFC3339))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
		defer cancel()
		v, warn, err := t.p.QueryRange(ctx, name, v1.Range{
			Start: start,
			End:   end,
			Step:  t.Step,
		})
		_ = warn
		if err != nil {
			panic(err)
			return err
		}
		bps := t.valueToInfluxdb(name, v)
		for _, i := range bps {
			{
				var b bytes.Buffer
				for _, p := range i.Points {
					if p.Raw != "" {
						_, _ = b.WriteString(p.Raw)
					} else {
						for k, v := range i.Tags {
							if p.Tags == nil {
								p.Tags = make(map[string]string, len(i.Tags))
							}
							p.Tags[k] = v
						}

						b.WriteString(p.MarshalString())
					}

					b.WriteByte('\n')
				}
			}
			var err error
			for try := 0; try <= t.Retry; try++ {
				_, err = t.i.Write(i)
				if err == nil {
					break
				}
			}
			if err != nil {
				panic(err)
				return err
			}
		}
		start = end
	}
	return nil
}

func (t *Trans) valueToInfluxdb(name string, v model.Value) (bps []client.BatchPoints) {
	var externalLabels = map[string]string{"monitor": t.MonitorLabel}
	switch v.(type) {
	case model.Matrix:
		v := v.(model.Matrix)
		for _, i := range v {
			bp := client.BatchPoints{
				Database:  t.Database,
				Tags:      metricToTag(i.Metric),
				Precision: t.Precision,
			}
			for _, j := range i.Values {
				tt := j.Timestamp.Time()
				// fmt.Println(tt)
				// ts := j.Timestamp.Unix()
				// fmt.Println(ts)
				// i, err := strconv.ParseInt(tt, 10, 64)
				// if err != nil {
				//  	fmt.Println("Oops:", err)

				//  }
				// t := time.Unix(i, 0)
				bp.Points = append(bp.Points, client.Point{
					Tags:        externalLabels,
					Measurement: name,
					Time:        tt,
					Fields:      map[string]interface{}{"value": float64(j.Value)},
					Precision:   t.Precision,
				})
			}
			bps = append(bps, bp)
		}
	case *model.Scalar:
		v := v.(*model.Scalar)
		bps = append(bps, client.BatchPoints{
			Points: []client.Point{{
				Measurement: name,
				Tags:        externalLabels,
				Fields:      map[string]interface{}{"value": float64(v.Value)},
				Precision:   t.Precision,
			}},
			Database: t.Database,
			Time:     v.Timestamp.Time().Add(-(time.Hour * 24 * 15)),
		})
	case model.Vector:
		v := v.(model.Vector)
		bp := client.BatchPoints{
			Database:  t.Database,
			Precision: t.Precision,
		}
		for _, i := range v {
			tags := metricToTag(i.Metric)
			for k, v := range externalLabels {
				tags[k] = v
			}
			bp.Points = append(bp.Points, client.Point{
				Measurement: name,
				Tags:        tags,
				Time:        i.Timestamp.Time(),
				Fields:      map[string]interface{}{"value": i.Value},
			})
		}
		bps = append(bps, bp)
	case *model.String:
		v := v.(*model.String)
		bps = append(bps, client.BatchPoints{
			Points: []client.Point{{
				Measurement: name,
				Tags:        externalLabels,

				Fields:    map[string]interface{}{"value": string(v.Value)},
				Precision: t.Precision,
			}},
			Database: t.Database,
			Time:     v.Timestamp.Time(),
		})
	default:
		panic("unknown type")
	}
	return
}

func metricToTag(metric model.Metric) map[string]string {
	return *(*map[string]string)(unsafe.Pointer(&metric))
}
