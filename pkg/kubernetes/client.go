package kubernetes

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/flanksource/commons/logger"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/flanksource/apm-hub/api/logs"
	"github.com/flanksource/kommons"
)

type Client struct {
	*kommons.Client
}

func GetKubeClient(kommonsClient *kommons.Client, kubernetesSeachBackend *logs.KubernetesSearchBackend) (*Client, error) {
	if kubernetesSeachBackend.Kubeconfig != nil {
		if kommonsClient != nil {
			_, value, err := kommonsClient.GetEnvValue(*kubernetesSeachBackend.Kubeconfig, kubernetesSeachBackend.Namespace)
			if err != nil {
				return nil, err
			}
			kommonsClient, err = kommons.NewClientFromBytes([]byte(value))
			return &Client{kommonsClient}, err
		}
		return nil, fmt.Errorf("default client is nil and kubeconfig is not set")
	}
	return &Client{kommonsClient}, nil
}

func (c *Client) GetAllPodsForNode(nodeName string, labels map[string]string) (pods *v1.PodList, err error) {
	client, err := c.GetClientset()
	if err != nil {
		return nil, err
	}
	labelsString := GetLabelString(labels)

	if nodeName != "" {
		pods, err = client.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{
			FieldSelector: "spec.nodeName=" + nodeName,
			LabelSelector: labelsString,
		})
	} else {
		pods, err = client.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{
			LabelSelector: labelsString,
		})
	}
	if err != nil {
		return nil, err
	}
	if len(pods.Items) != 0 {
		return pods, nil
	}
	return nil, nil
}

// empty name will fetch all pods with the specified labels and if labels are nil will fetch the pods with the specified name
func (c *Client) GetPodsWithNameAndLabels(name, namespace string, labels map[string]string) (pods *v1.PodList, err error) {
	client, err := c.GetClientset()
	if err != nil {
		return nil, err
	}
	labelsString := GetLabelString(labels)
	if name != "" {
		pods, err = client.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
			FieldSelector: "metadata.name=" + name,
			LabelSelector: labelsString,
		})
	} else {
		pods, err = client.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labelsString,
		})
	}
	if err != nil {
		return nil, err
	}
	if len(pods.Items) != 0 {
		return pods, nil
	}
	return nil, nil
}

func (c *Client) GetPodsForDeployment(name, namespace string, labels map[string]string) (pods *v1.PodList, err error) {
	client, err := c.GetClientset()
	if err != nil {
		return nil, err
	}

	labelsString := GetLabelString(labels)

	var deployments *appsv1.DeploymentList
	if name != "" {
		deployments, err = client.AppsV1().Deployments(namespace).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labelsString,
			FieldSelector: "metadata.name=" + name,
		})
	} else {
		deployments, err = client.AppsV1().Deployments(namespace).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labelsString,
		})
	}
	if err != nil {
		return nil, err
	}
	var deploymentPod *v1.PodList
	pods = &v1.PodList{
		Items: []v1.Pod{},
	}
	for _, deployment := range deployments.Items {
		deploymentPod, err = client.CoreV1().Pods(deployment.GetNamespace()).List(context.TODO(), metav1.ListOptions{
			LabelSelector: GetLabelString(deployment.Spec.Template.Labels),
		})
		if err != nil {
			logger.Errorf("error fetching pod for deployment: %v; error: %v", deployment.Name, err)
			continue
		}
		pods.Items = append(pods.Items, deploymentPod.Items...)
	}
	return
}

func (c *Client) GetPodsForService(name, namespace string, labels map[string]string) (pods *v1.PodList, err error) {
	client, err := c.GetClientset()
	if err != nil {
		return nil, err
	}

	labelsString := GetLabelString(labels)

	var services *v1.ServiceList
	if name != "" {
		services, err = client.CoreV1().Services(namespace).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labelsString,
			FieldSelector: "metadata.name=" + name,
		})
	} else {
		services, err = client.CoreV1().Services(namespace).List(context.TODO(), metav1.ListOptions{
			LabelSelector: labelsString,
		})
	}
	if err != nil {
		return nil, err
	}
	pods = &v1.PodList{
		Items: []v1.Pod{},
	}
	for _, service := range services.Items {
		servicePods, err := client.CoreV1().Pods(service.GetNamespace()).List(context.TODO(), metav1.ListOptions{
			LabelSelector: GetLabelString(service.Spec.Selector),
		})
		if err != nil {
			logger.Errorf("error fetching pod for deployment: %v; error: %v", service.Name, err)
			continue
		}
		pods.Items = append(pods.Items, servicePods.Items...)
	}
	return
}

func getLogResult(line string) logs.Result {
	timestamp := strings.Split(line, " ")[0]
	return logs.Result{
		Time:    timestamp,
		Message: strings.TrimPrefix(line, timestamp),
	}
}

func (c *Client) GetLogsForPod(q *logs.SearchParams, pod v1.Pod) (map[string][]logs.Result, error) {
	containerLogs := make(map[string][]logs.Result)
	client, err := c.GetClientset()
	if err != nil {
		return nil, err
	}
	pods := client.CoreV1().Pods(pod.Namespace)

	for _, container := range append(pod.Spec.Containers, pod.Spec.InitContainers...) {
		options := &v1.PodLogOptions{
			Container:  container.Name,
			Follow:     false,
			Timestamps: true,
		}

		if q.LimitPerItem > 0 {
			options.TailLines = &q.LimitPerItem
		} else if q.Limit > 0 {
			options.TailLines = &q.Limit
		}
		if q.LimitBytesPerItem > 0 {
			options.LimitBytes = &q.LimitBytesPerItem
		} else if q.LimitBytes > 0 {
			options.LimitBytes = &q.LimitBytes
		}
		start := q.GetStart()
		if start != nil {
			options.SinceTime = &metav1.Time{Time: *start}
		}

		podLogs, err := pods.GetLogs(pod.Name, options).Do(context.TODO()).Raw()
		if err != nil {
			logger.Tracef("failed to begin streaming %s/%s: %s", pod.Name, container.Name, err)
			continue
		}
		scanner := bufio.NewScanner(bytes.NewReader(podLogs))
		var lines []logs.Result
		for scanner.Scan() {
			lines = append(lines, getLogResult(scanner.Text()))
		}
		containerLogs[container.Name] = lines
	}
	return containerLogs, nil
}
