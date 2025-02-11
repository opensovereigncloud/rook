/*
Copyright 2020 The Rook Authors. All rights reserved.

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

package mon

import (
	"context"
	"testing"
	"time"

	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/rook/rook/pkg/clusterd"
	"github.com/rook/rook/pkg/daemon/ceph/client"
	"github.com/rook/rook/pkg/operator/k8sutil"
	"github.com/rook/rook/pkg/operator/test"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
)

func TestCreateService(t *testing.T) {
	ctx := t.Context()
	clientset := test.New(t, 1)
	c := New(ctx, &clusterd.Context{Clientset: clientset}, "ns", cephv1.ClusterSpec{}, &k8sutil.OwnerInfo{})
	c.ClusterInfo = client.AdminTestClusterInfo("rook-ceph")
	m := &monConfig{ResourceName: "rook-ceph-mon-b", DaemonName: "b"}
	service, err := c.createService(m)
	assert.NoError(t, err)
	// the clusterIP will not be set in a mock service
	assert.Equal(t, "", service.Spec.ClusterIP)

	m.PublicIP = "1.2.3.4"
	service, err = c.createService(m)
	assert.NoError(t, err)
	// the clusterIP will not be set in the mock because the service already exists
	assert.Equal(t, "", service.Spec.ClusterIP)

	// delete the service to mock a disaster recovery scenario
	err = clientset.CoreV1().Services(c.Namespace).Delete(ctx, m.ResourceName, metav1.DeleteOptions{})
	assert.NoError(t, err)

	service, err = c.createService(m)
	assert.NoError(t, err)
	// the clusterIP will now be set to the expected value
	assert.Equal(t, m.PublicIP, service.Spec.ClusterIP)
}

func TestCreateServiceSelector(t *testing.T) {
	ctx := context.TODO()
	clientset := test.New(t, 1)
	c := New(ctx, &clusterd.Context{Clientset: clientset}, "ns", cephv1.ClusterSpec{}, &k8sutil.OwnerInfo{})
	c.ClusterInfo = client.AdminTestClusterInfo("rook-ceph")

	t.Run("normal mon service has rook-ceph-mon selector", func(t *testing.T) {
		m := &monConfig{ResourceName: "rook-ceph-mon-a", DaemonName: "a"}
		service, err := c.createService(m)
		assert.NoError(t, err)
		assert.Equal(t, AppName, service.Spec.Selector["app"])
		assert.Equal(t, "a", service.Spec.Selector["mon"])
	})

	t.Run("floating mon service has rook-ceph-floating-mon selector", func(t *testing.T) {
		c.spec.Mon.FloatingMon.Name = "c"
		m := &monConfig{ResourceName: "rook-ceph-mon-c", DaemonName: "c"}
		service, err := c.createService(m)
		assert.NoError(t, err)
		assert.Equal(t, FloatingMonAppName, service.Spec.Selector["app"])
		assert.Equal(t, "c", service.Spec.Selector["mon"])
	})
}

func TestCreateLoadBalancerService(t *testing.T) {
	ctx := t.Context()
	clientset := test.New(t, 1)
	c := New(ctx, &clusterd.Context{Clientset: clientset}, "ns", cephv1.ClusterSpec{}, &k8sutil.OwnerInfo{})
	c.ClusterInfo = client.AdminTestClusterInfo("rook-ceph")
	c.ClusterInfo.NetworkSpec.InternalLoadBalancerService = cephv1.InternalLoadBalancerServiceSpec{
		Enabled: true,
		Labels:  map[string]string{"example.com/internal": "true"},
	}
	m := &monConfig{ResourceName: "rook-ceph-mon-a", DaemonName: "a", PublicIP: "10.0.0.5"}

	// no address is assigned to the load balancer
	loadBalancerIPTimeout = time.Millisecond
	_, err := c.createLoadBalancerService(m)
	assert.Error(t, err)

	service, err := clientset.CoreV1().Services(c.Namespace).Get(ctx, m.ResourceName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, v1.ServiceTypeLoadBalancer, service.Spec.Type)
	assert.Equal(t, "true", service.Annotations["example.com/internal"])
	// the mon address is the load balancer address and must not be requested as clusterIP
	assert.Equal(t, "", service.Spec.ClusterIP)

	// the cloud provider assigns an address. the fake clientset drops the status when the
	// service is updated, so the status is set on every get.
	clientset.PrependReactor("get", "services", func(action k8stesting.Action) (bool, runtime.Object, error) {
		obj, err := clientset.Tracker().Get(action.GetResource(), action.GetNamespace(), action.(k8stesting.GetAction).GetName())
		if err != nil {
			return true, nil, err
		}
		svc := obj.(*v1.Service).DeepCopy()
		svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{IP: "10.0.0.5"}}
		return true, svc, nil
	})
	loadBalancerIPTimeout = time.Minute
	ip, err := c.createLoadBalancerService(m)
	assert.NoError(t, err)
	assert.Equal(t, "10.0.0.5", ip)
}
