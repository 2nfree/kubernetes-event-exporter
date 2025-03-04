package sinks

import (
	"fmt"
	"testing"
	"time"

	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildSendMessage(t *testing.T) {
	k := &KafkaSink{
		cfg: &KafkaConfig{
			Topic: "test-topic",
			CustomFields: map[string]interface{}{
				"test": "test",
			},
		},
	}

	ev := &kube.EnhancedEvent{}
	ev.ClusterName = "test-cluster"
	ev.Namespace = "default"
	ev.Type = "Warning"
	ev.InvolvedObject.Kind = "Pod"
	ev.InvolvedObject.Name = "nginx-server-123abc-456def"
	ev.Message = "Successfully pulled image \"nginx:latest\""
	ev.FirstTimestamp = v1.Time{Time: time.Now()}

	buf, err := k.buildSendMessage(ev)
	if err != nil {
		t.Fatalf("failed to build send message: %v", err)
	}

	fmt.Println(string(buf))
}
