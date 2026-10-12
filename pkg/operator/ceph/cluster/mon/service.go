/*
Copyright 2016 The Rook Authors. All rights reserved.

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
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/rook/rook/pkg/operator/k8sutil"
	"github.com/rook/rook/pkg/util/log"
	v1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

// how long to wait for the cloud provider to assign an address to a mon load balancer service,
// and how often to check for it
var (
	loadBalancerIPTimeout      = 2 * time.Minute
	loadBalancerIPPollInterval = 2 * time.Second
)

// monsUseLoadBalancer returns whether each mon advertises the address of its load balancer service
// instead of the address of the node it runs on.
func (c *Cluster) monsUseLoadBalancer() bool {
	return c.spec.Network.IsHost() && c.spec.Network.InternalLoadBalancerService.Enabled
}

// monIsPortable returns whether a mon can move to another node and keep its identity: its address
// belongs to a load balancer and its store is on a PVC.
func (c *Cluster) monIsPortable(mon *monConfig) bool {
	return c.monsUseLoadBalancer() && c.monVolumeClaimTemplate(mon) != nil
}

func (c *Cluster) createService(mon *monConfig) (*v1.Service, error) {
	svcDef := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mon.ResourceName,
			Namespace: c.Namespace,
			Labels:    c.getLabels(mon, false, true),
		},
		Spec: v1.ServiceSpec{
			Selector: c.getLabels(mon, false, false),
		},
	}

	if c.monsUseLoadBalancer() {
		svcDef.Spec.Type = v1.ServiceTypeLoadBalancer
		svcDef.Annotations = c.spec.Network.InternalLoadBalancerService.Labels
		// The update below replaces the whole object. Keep the finalizers, or the cloud provider
		// loses the one that makes it clean up the load balancer when the service is deleted.
		existing, err := c.context.Clientset.CoreV1().Services(c.Namespace).Get(c.ClusterInfo.Context, svcDef.Name, metav1.GetOptions{})
		if err == nil {
			svcDef.Finalizers = existing.Finalizers
		} else if !kerrors.IsNotFound(err) {
			return nil, errors.Wrapf(err, "failed to get mon service %q", svcDef.Name)
		}
	}

	err := c.ownerInfo.SetOwnerReference(svcDef)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to set owner reference to mon service %q", svcDef.Name)
	}

	// If the mon port was not msgr2, add the msgr1 port
	if mon.Port != DefaultMsgr2Port {
		addServicePort(svcDef, DefaultMsgr1PortName, mon.Port)
	}
	addServicePort(svcDef, DefaultMsgr2PortName, DefaultMsgr2Port)

	// Set the ClusterIP if the service does not exist and we expect a certain cluster IP
	// For example, in disaster recovery the service might have been deleted accidentally, but we have the
	// expected endpoint from the mon configmap.
	if !c.monsUseLoadBalancer() && mon.PublicIP != "" {
		_, err := c.context.Clientset.CoreV1().Services(c.Namespace).Get(c.ClusterInfo.Context, svcDef.Name, metav1.GetOptions{})
		if err != nil && kerrors.IsNotFound(err) {
			log.NamespacedInfo(c.Namespace, logger, "ensuring the clusterIP for mon %q is %q", mon.DaemonName, mon.PublicIP)
			svcDef.Spec.ClusterIP = mon.PublicIP
		}
	}

	s, err := k8sutil.CreateOrUpdateService(c.ClusterInfo.Context, c.context.Clientset, c.Namespace, svcDef)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create service for mon %s", mon.DaemonName)
	}

	if s == nil {
		logger.Errorf("service ip not found for mon %q. if this is not a unit test, this is an error", mon.ResourceName)
		return nil, nil
	}

	log.NamespacedInfo(c.Namespace, logger, "mon %q cluster IP is %s", mon.DaemonName, s.Spec.ClusterIP)
	return s, nil
}

// createLoadBalancerService creates the mon service as a load balancer and returns the IP address
// assigned to it, which is the address the mon advertises instead of the node address. Load
// balancers that are only assigned a hostname are not supported.
func (c *Cluster) createLoadBalancerService(mon *monConfig) (string, error) {
	if _, err := c.createService(mon); err != nil {
		return "", errors.Wrap(err, "failed to create mon service")
	}

	var ip string
	err := wait.PollUntilContextTimeout(c.ClusterInfo.Context, loadBalancerIPPollInterval, loadBalancerIPTimeout, true, func(ctx context.Context) (bool, error) {
		svc, err := c.context.Clientset.CoreV1().Services(c.Namespace).Get(ctx, mon.ResourceName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if len(svc.Status.LoadBalancer.Ingress) > 0 {
			ip = svc.Status.LoadBalancer.Ingress[0].IP
		}
		return ip != "", nil
	})
	if err != nil {
		return "", errors.Wrapf(err, "failed to get load balancer IP for mon %q", mon.DaemonName)
	}

	log.NamespacedInfo(c.Namespace, logger, "mon %q load balancer IP is %s", mon.DaemonName, ip)
	return ip, nil
}

func (c *Cluster) exportService(service *v1.Service, monDaemon string) (string, error) {
	// defer removing the mon canary deployment to after the service is exported because DNS
	// query on <service>.<ns>.svc.clusterset.local requires the mon canary pod to be running
	defer c.removeCanaryDeployments(monCanaryLabelSelector + fmt.Sprintf(",mon=%s", monDaemon))

	log.NamespacedInfo(c.Namespace, logger, "exporting service %q", service.Name)
	exportedIP, err := k8sutil.ExportService(c.ClusterInfo.Context, c.context, service, c.spec.Network.MultiClusterService.ClusterID)
	if err != nil {
		return "", errors.Wrapf(err, "failed to export service %q", service.Name)
	}
	log.NamespacedInfo(c.Namespace, logger, "mon %q exported IP is %s", service.Name, exportedIP)

	return exportedIP, nil
}
