package kube_state

import (
	"context"
	"strconv"

	"github.com/influxdata/telegraf"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/util/node"
)

var (
	podMeasurement          = "kube_pod"
	podContainerMeasurement = "kube_pod_container"
	podVolumeMeasurement    = "kube_pod_spec_volumes"
)

func registerPodCollector(ctx context.Context, acc telegraf.Accumulator, ks *KubenetesState) {
	list, err := ks.client.getPods(ctx)
	if err != nil {
		acc.AddError(err)
		return
	}
	for _, p := range list.Items {
		if err = ks.gatherPod(p, acc); err != nil {
			acc.AddError(err)
			return
		}
	}
}

func (ks *KubenetesState) gatherPod(p v1.Pod, acc telegraf.Accumulator) error {
	nodeName := p.Spec.NodeName
	fields := make(map[string]interface{})
	tags := make(map[string]string)

	createdBy := metav1.GetControllerOf(&p)
	createdByKind := ""
	createdByName := ""
	if createdBy != nil {
		if createdBy.Kind != "" {
			createdByKind = createdBy.Kind
		}
		if createdBy.Name != "" {
			createdByName = createdBy.Name
		}
	}

	if p.Status.StartTime != nil {
		fields["start_time"] = p.Status.StartTime.UnixNano()
	}

	tags["namesapce"] = p.Namespace
	tags["name"] = p.Name
	tags["host_ip"] = p.Status.HostIP
	tags["pod_ip"] = p.Status.PodIP
	tags["node"] = nodeName
	tags["created_by_kind"] = createdByKind
	tags["created_by_name"] = createdByName
	tags["status_scheduled"] = "false"
	tags["status_ready"] = "false"

	owners := p.GetOwnerReferences()
	if len(owners) == 0 {
		tags["owner_kind"] = ""
		tags["owner_name"] = ""
		tags["owner_is_controller"] = ""
	} else {
		tags["owner_kind"] = owners[0].Kind
		tags["owner_name"] = owners[0].Name
		if owners[0].Controller != nil {
			tags["owner_is_controller"] = strconv.FormatBool(*owners[0].Controller)
		} else {
			tags["owner_is_controller"] = "false"
		}
	}

	for k, v := range p.Labels {
		tags["label_"+sanitizeLabelName(k)] = v
	}

	if phase := p.Status.Phase; phase != "" {
		tags["status_phase"] = string(phase)
		// This logic is directly copied from: https://github.com/kubernetes/kubernetes/blob/d39bfa0d138368bbe72b0eaf434501dcb4ec9908/pkg/printers/internalversion/printers.go#L597-L601
		// For more info, please go to: https://github.com/kubernetes/kube-state-metrics/issues/410
		if p.DeletionTimestamp != nil && p.Status.Reason == node.NodeUnreachablePodReason {
			tags["status_phase"] = string(v1.PodUnknown)
		}
	}

	if !p.CreationTimestamp.IsZero() {
		fields["created"] = p.CreationTimestamp.Unix()
	}

	for _, c := range p.Status.Conditions {
		switch c.Type {
		case v1.PodReady:
			tags["status_ready"] = "true"
		case v1.PodScheduled:
			tags["status_scheduled"] = "true"
			fields["status_scheduled_time"] = c.LastTransitionTime.Unix()
		}
	}

	var lastFinishTime int64

	for i, cs := range p.Status.ContainerStatuses {
		c := p.Spec.Containers[i]
		gatherPodContainer(nodeName, p, cs, c, &lastFinishTime, acc)
	}

	if lastFinishTime > 0 {
		fields["completion_time"] = lastFinishTime
	}

	for _, v := range p.Spec.Volumes {
		if v.PersistentVolumeClaim != nil {
			gatherPodVolume(v, p, acc)
		}
	}

	acc.AddFields(podMeasurement, fields, tags)
	return nil
}

func gatherPodVolume(v v1.Volume, p v1.Pod, acc telegraf.Accumulator) {
	fields := map[string]interface{}{
		"read_only": 0.0,
	}
	tags := map[string]string{
		"namespace":             p.Namespace,
		"pod":                   p.Name,
		"volume":                v.Name,
		"persistentvolumeclaim": v.PersistentVolumeClaim.ClaimName,
	}
	if v.PersistentVolumeClaim.ReadOnly {
		fields["read_only"] = 1.0
	}
	acc.AddFields(podVolumeMeasurement, fields, tags)
}

func gatherPodContainer(nodeName string, p v1.Pod, cs v1.ContainerStatus, c v1.Container, lastFinishTime *int64, acc telegraf.Accumulator) {

	fields := map[string]interface{}{
		"status_restarts_total": cs.RestartCount,
	}
	tags := map[string]string{
		"namespace":                p.Namespace,
		"pod_name":                 p.Name,
		"node_name":                nodeName,
		"container":                c.Name,
		"image":                    cs.Image,
		"image_id":                 cs.ImageID,
		"container_id":             cs.ContainerID,
		"status_waiting":           strconv.FormatBool(cs.State.Waiting != nil),
		"status_waiting_reason":    "",
		"status_running":           strconv.FormatBool(cs.State.Terminated != nil),
		"status_terminated":        strconv.FormatBool(cs.State.Running != nil),
		"status_terminated_reason": "",
		"container_status_ready":   strconv.FormatBool(cs.Ready),
	}

	if cs.State.Waiting != nil {
		tags["status_waiting_reason"] = cs.State.Waiting.Reason
	}

	if cs.State.Terminated != nil {
		tags["status_terminated_reason"] = cs.State.Terminated.Reason
		if *lastFinishTime == 0 || *lastFinishTime < cs.State.Terminated.FinishedAt.Unix() {
			*lastFinishTime = cs.State.Terminated.FinishedAt.Unix()
		}
	}
	req := c.Resources.Requests
	lim := c.Resources.Limits

	for resourceName, val := range req {
		switch resourceName {
		case v1.ResourceCPU:
			fields["resource_requests_cpu_cores"] = val.MilliValue() / 1000
		default:
			fields["resource_requests_"+sanitizeLabelName(string(resourceName))+"_bytes"] = val.Value()
		}
	}
	for resourceName, val := range lim {
		switch resourceName {
		case v1.ResourceCPU:
			fields["resource_limits_cpu_cores"] = val.MilliValue() / 1000
		default:
			fields["resource_limits_"+sanitizeLabelName(string(resourceName))+"_bytes"] = val.Value()
		}
	}

	acc.AddFields(podContainerMeasurement, fields, tags)
}
