package kube_state

import (
	"context"
	"time"

	"github.com/influxdata/telegraf"
	"k8s.io/api/core/v1"
)

var serviceMeasurement = "kube_service"

func registerServiceCollector(ctx context.Context, acc telegraf.Accumulator, ks *KubenetesState) {
	list, err := ks.client.getServices(ctx)
	if err != nil {
		acc.AddError(err)
		return
	}
	for _, s := range list.Items {
		if err = ks.gatherService(s, acc); err != nil {
			acc.AddError(err)
			return
		}
	}
}

func (ks *KubenetesState) gatherService(s v1.Service, acc telegraf.Accumulator) error {
	var creationTime time.Time
	if !s.CreationTimestamp.IsZero() {
		creationTime = s.CreationTimestamp.Time
	}
	fields := map[string]interface{}{
		"gauge": 1,
	}
	tags := map[string]string{
		"namespace":  s.Namespace,
		"service":    s.Name,
		"type":       string(s.Spec.Type),
		"cluster_ip": s.Spec.ClusterIP,
	}
	for k, v := range s.Labels {
		tags["label_"+sanitizeLabelName(k)] = v
	}
	acc.AddFields(serviceMeasurement, fields, tags, creationTime)

	return nil
}
