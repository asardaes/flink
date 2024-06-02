/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.kubernetes.configuration.KubernetesResourceManagerDriverConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.factory.KubernetesTaskManagerFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesStatefulSet;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.active.AbstractResourceManagerDriver;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import io.fabric8.kubernetes.api.model.ObjectMeta;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class KubernetesStatefulSetResourceManagerDriver
        extends AbstractResourceManagerDriver<KubernetesWorkerNode> {

    private static final String STATEFUL_SET_NAME_TEMPLATE = "%s-taskmanager-%d";

    private final String clusterId;

    private final String webInterfaceUrl;

    private final FlinkKubeClient flinkKubeClient;

    private final Set<TaskExecutorProcessSpec> taskExecutorProcessSpecs;

    private final Set<String> recoveredExecutorProcessSpecHashes;

    private volatile boolean running;

    private FlinkPod taskManagerPodTemplate;

    public KubernetesStatefulSetResourceManagerDriver(
            Configuration flinkConfig,
            FlinkKubeClient flinkKubeClient,
            KubernetesResourceManagerDriverConfiguration configuration) {
        super(flinkConfig, GlobalConfiguration.loadConfiguration());
        this.clusterId = Preconditions.checkNotNull(configuration.getClusterId());
        this.webInterfaceUrl = configuration.getWebInterfaceUrl();
        this.flinkKubeClient = Preconditions.checkNotNull(flinkKubeClient);
        this.taskExecutorProcessSpecs = new HashSet<>();
        this.recoveredExecutorProcessSpecHashes = new HashSet<>();
        this.running = false;
    }

    @Override
    protected void initializeInternal() throws Exception {
        taskManagerPodTemplate =
                KubernetesUtils.getTaskManagerPodFromTemplateInPod(flinkKubeClient);
        KubernetesUtils.updateKubernetesServiceTargetPortIfNecessary(
                flinkKubeClient, clusterId, flinkConfig, webInterfaceUrl);
        recoverWorkerNodesFromPreviousAttempts();
        this.running = true;
    }

    private void recoverWorkerNodesFromPreviousAttempts() {
        final List<KubernetesWorkerNode> workerNodes = new ArrayList<>();
        final List<KubernetesStatefulSet> statefulSets =
                flinkKubeClient.getStatefulSetsWithLabels(
                        KubernetesUtils.getTaskManagerSelectors(clusterId));

        for (KubernetesStatefulSet statefulSet : statefulSets) {
            final ObjectMeta metadata = statefulSet.getInternalResource().getMetadata();
            final String processSpecHash =
                    metadata.getAnnotations()
                            .get(KubernetesTaskManagerFactory.STATEFUL_SET_ANNOTATION_KEY);

            if (processSpecHash == null) {
                log.warn(
                        "Found stateful set for {} with missing annotation {}.",
                        clusterId,
                        KubernetesTaskManagerFactory.STATEFUL_SET_ANNOTATION_KEY);
            } else {
                recoveredExecutorProcessSpecHashes.add(processSpecHash);
            }

            for (int i = 0; i < statefulSet.getInternalResource().getSpec().getReplicas(); i++) {
                workerNodes.add(
                        new KubernetesWorkerNode(new ResourceID(metadata.getName() + "-" + i)));
            }
        }

        getResourceEventHandler().onPreviousAttemptWorkersRecovered(workerNodes);
    }

    @Override
    public void terminate() {
        if (!running) {
            return;
        }

        try {
            flinkKubeClient.close();
        } finally {
            running = false;
        }
    }

    @Override
    public void deregisterApplication(
            ApplicationStatus finalStatus, @Nullable String optionalDiagnostics) {
        log.info(
                "Deregistering Flink Kubernetes cluster, clusterId: {}, diagnostics: {}",
                clusterId,
                optionalDiagnostics == null ? "" : optionalDiagnostics);
        flinkKubeClient.stopAndCleanupCluster(clusterId);
    }

    @Override
    public CompletableFuture<KubernetesWorkerNode> requestResource(
            TaskExecutorProcessSpec taskExecutorProcessSpec) {
        String processSpecHash = String.valueOf(taskExecutorProcessSpec.hashCode());
        if (recoveredExecutorProcessSpecHashes.contains(processSpecHash)) {
            taskExecutorProcessSpecs.add(taskExecutorProcessSpec);
        }

        if (taskExecutorProcessSpecs.add(taskExecutorProcessSpec)) {
            final String name =
                    String.format(
                            STATEFUL_SET_NAME_TEMPLATE, clusterId, taskExecutorProcessSpecs.size());
            return createNewTaskManagerStatefulSet(taskExecutorProcessSpec, name);
        } else {
            final String name =
                    String.format(
                            STATEFUL_SET_NAME_TEMPLATE, clusterId, taskExecutorProcessSpecs.size());
            return scaleExistingTaskManagerStatefulSet(name, 1);
        }
    }

    private CompletableFuture<KubernetesWorkerNode> createNewTaskManagerStatefulSet(
            TaskExecutorProcessSpec taskExecutorProcessSpec, String name) {
        final KubernetesTaskManagerParameters parameters =
                KubernetesUtils.createKubernetesTaskManagerParameters(
                        flinkConfig,
                        flinkClientConfig,
                        name,
                        taskExecutorProcessSpec,
                        getBlockedNodeRetriever().getAllBlockedNodeIds());
        final KubernetesStatefulSet taskManagerStatefulSet =
                KubernetesTaskManagerFactory.buildTaskManagerKubernetesStatefulSet(
                        taskManagerPodTemplate, taskExecutorProcessSpec, parameters);

        log.info(
                "Creating new TaskManager stateful set with name {} and resources <{},{}>.",
                name,
                parameters.getTaskManagerMemoryMB(),
                parameters.getTaskManagerCPU());

        final CompletableFuture<KubernetesWorkerNode> requestResourceFuture =
                new CompletableFuture<>();
        final CompletableFuture<Void> createStatefulSetFuture =
                flinkKubeClient.createTaskManagerStatefulSet(taskManagerStatefulSet);

        FutureUtils.assertNoException(
                createStatefulSetFuture.handleAsync(
                        (ignored, exception) -> {
                            if (exception == null) {
                                log.info("Stateful set {} created successfully", name);
                                requestResourceFuture.complete(
                                        new KubernetesWorkerNode(new ResourceID(name + "-0")));
                            } else {
                                log.error("Creation of stateful set {} failed", name, exception);
                                taskExecutorProcessSpecs.remove(taskExecutorProcessSpec);
                                requestResourceFuture.completeExceptionally(exception);
                            }
                            return null;
                        },
                        getMainThreadExecutor()));

        return requestResourceFuture;
    }

    private CompletableFuture<KubernetesWorkerNode> scaleExistingTaskManagerStatefulSet(
            String name, int delta) {

        log.info("Scaling TaskManager stateful set with name {} by {}", name, delta);

        final CompletableFuture<KubernetesWorkerNode> requestResourceFuture =
                new CompletableFuture<>();
        final CompletableFuture<Integer> createStatefulSetFuture =
                flinkKubeClient.scaleTaskManagerStatefulSet(name, delta);

        FutureUtils.assertNoException(
                createStatefulSetFuture.handleAsync(
                        (numReplicas, exception) -> {
                            if (exception == null) {
                                log.info("Stateful set {} scaled successfully", name);
                                final int nameSuffix = numReplicas - 1;
                                requestResourceFuture.complete(
                                        new KubernetesWorkerNode(
                                                new ResourceID(name + "-" + nameSuffix)));
                            } else {
                                log.error("Scaling of stateful set {} failed", name, exception);
                                requestResourceFuture.completeExceptionally(exception);
                            }
                            return null;
                        },
                        getMainThreadExecutor()));

        return requestResourceFuture;
    }

    @Override
    public void releaseResource(KubernetesWorkerNode worker) {
        // TODO if a pod dies externally, the manager still calls this, ignore scaling then...
        final String podName = worker.getResourceID().getResourceIdString();
        final String statefulSetName = podName.substring(0, podName.lastIndexOf('-'));
        scaleExistingTaskManagerStatefulSet(statefulSetName, -1)
                .whenComplete(
                        (ignored, exception) -> {
                            if (exception == null) {
                                getResourceEventHandler()
                                        .onWorkerTerminated(worker.getResourceID(), "");
                            } else {
                                log.error(
                                        "Could not downscale TaskManager stateful set {}",
                                        statefulSetName,
                                        exception);
                            }
                        });
    }
}
