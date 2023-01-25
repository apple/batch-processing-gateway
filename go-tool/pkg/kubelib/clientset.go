/*
 *
 * This source file is part of the Batch Processing Gateway open source project
 *
 * Copyright 2022 Apple Inc. and the Batch Processing Gateway project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kubelib

import (
	"fmt"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/tools/clientcmd"
	//
	// Uncomment to load all auth plugins
	// _ "k8s.io/client-go/plugin/pkg/client/auth"
	//
	// Or uncomment to load specific auth plugins
	// _ "k8s.io/client-go/plugin/pkg/client/auth/azure"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
)

type KubernetesClientConfig struct {
	Host   string
	CAData []byte
}

func GetClientset(kubeConfigContextName string) (*kubernetes.Clientset, KubernetesClientConfig, error) {
	pathOpts := clientcmd.NewDefaultPathOptions()
	config, err := pathOpts.GetStartingConfig()
	if err != nil {
		return nil, KubernetesClientConfig{}, fmt.Errorf("failed to run pathOpts.GetStartingConfig(): %w", err)
	}
	var overrides *clientcmd.ConfigOverrides = nil
	if kubeConfigContextName != "" {
		configContext := config.Contexts[kubeConfigContextName]
		if configContext == nil {
			return nil, KubernetesClientConfig{}, fmt.Errorf("context %s not exist in kubeconfig", kubeConfigContextName)
		}
		overrides = &clientcmd.ConfigOverrides{
			Context: *configContext,
		}
	}
	clientConfig := clientcmd.NewDefaultClientConfig(*config, overrides)
	restclientConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return nil, KubernetesClientConfig{}, fmt.Errorf("failed to run clientConfig.ClientConfig(): %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restclientConfig)
	if err != nil {
		return nil, KubernetesClientConfig{}, fmt.Errorf("failed to run kubernetes.NewForConfig(): %w", err)
	}
	kubernetesClientConfig := KubernetesClientConfig{
		Host:   restclientConfig.Host,
		CAData: restclientConfig.CAData,
	}
	return clientset, kubernetesClientConfig, nil
}
