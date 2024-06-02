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

package org.apache.flink.kubernetes.kubeclient.factory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.CmdTaskManagerDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.EnvSecretsDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkConfMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.HadoopConfMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.InitTaskManagerDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.KerberosMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.KubernetesStepDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.MountSecretsDecorator;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesStatefulSet;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.ObjectFieldSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.KUBERNETES_HADOOP_CONF_MOUNT_DECORATOR_ENABLED;
import static org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.KUBERNETES_KERBEROS_MOUNT_DECORATOR_ENABLED;

/** Utility class for constructing the TaskManager resource on the JobManager. */
public class KubernetesTaskManagerFactory {

    public static final String STATEFUL_SET_ANNOTATION_KEY = "flink.apache.org/process-spec-hash";

    public static final String RESOURCE_ID_ENV_VAR = "TASKMANAGER_RESOURCE_ID";

    private static final EnvVar RESOURCE_ID_ENV_VALUE;

    static {
        ObjectFieldSelector fieldRef = new ObjectFieldSelector();
        fieldRef.setFieldPath("metadata.name");
        RESOURCE_ID_ENV_VALUE =
                new EnvVarBuilder()
                        .withName(RESOURCE_ID_ENV_VAR)
                        .withValueFrom(new EnvVarSourceBuilder().withFieldRef(fieldRef).build())
                        .build();
    }

    public static KubernetesPod buildTaskManagerKubernetesPod(
            FlinkPod podTemplate, KubernetesTaskManagerParameters kubernetesTaskManagerParameters) {
        FlinkPod flinkPod = Preconditions.checkNotNull(podTemplate).copy();

        final List<KubernetesStepDecorator> stepDecorators =
                new ArrayList<>(
                        Arrays.asList(
                                new InitTaskManagerDecorator(kubernetesTaskManagerParameters),
                                new EnvSecretsDecorator(kubernetesTaskManagerParameters),
                                new MountSecretsDecorator(kubernetesTaskManagerParameters),
                                new CmdTaskManagerDecorator(kubernetesTaskManagerParameters)));

        Configuration configuration = kubernetesTaskManagerParameters.getFlinkConfiguration();
        if (configuration.get(KUBERNETES_HADOOP_CONF_MOUNT_DECORATOR_ENABLED)) {
            stepDecorators.add(new HadoopConfMountDecorator(kubernetesTaskManagerParameters));
        }
        if (configuration.get(KUBERNETES_KERBEROS_MOUNT_DECORATOR_ENABLED)) {
            stepDecorators.add(new KerberosMountDecorator(kubernetesTaskManagerParameters));
        }

        stepDecorators.add(new FlinkConfMountDecorator(kubernetesTaskManagerParameters));

        for (KubernetesStepDecorator stepDecorator : stepDecorators) {
            flinkPod = stepDecorator.decorateFlinkPod(flinkPod);
        }

        final Pod resolvedPod =
                new PodBuilder(flinkPod.getPodWithoutMainContainer())
                        .editOrNewSpec()
                        .addToContainers(flinkPod.getMainContainer())
                        .endSpec()
                        .build();

        return new KubernetesPod(resolvedPod);
    }

    public static KubernetesStatefulSet buildTaskManagerKubernetesStatefulSet(
            FlinkPod podTemplate,
            TaskExecutorProcessSpec taskExecutorProcessSpec,
            KubernetesTaskManagerParameters kubernetesTaskManagerParameters) {
        final KubernetesPod resolvedPod =
                buildTaskManagerKubernetesPod(podTemplate, kubernetesTaskManagerParameters);
        final StatefulSet statefulSet =
                new StatefulSetBuilder()
                        .withApiVersion(Constants.APPS_API_VERSION)
                        .editOrNewMetadata()
                        .withName(kubernetesTaskManagerParameters.getPodName())
                        .withLabels(resolvedPod.getInternalResource().getMetadata().getLabels())
                        .addToAnnotations(
                                STATEFUL_SET_ANNOTATION_KEY,
                                String.valueOf(taskExecutorProcessSpec.hashCode()))
                        .endMetadata()
                        .editOrNewSpec()
                        .withPodManagementPolicy("Parallel")
                        .withSelector(
                                new LabelSelectorBuilder()
                                        .withMatchLabels(
                                                resolvedPod
                                                        .getInternalResource()
                                                        .getMetadata()
                                                        .getLabels())
                                        .build())
                        .withTemplate(
                                new PodTemplateSpec(
                                        resolvedPod
                                                .getInternalResource()
                                                .getMetadata()
                                                .toBuilder()
                                                .withName(null)
                                                .build(),
                                        resolvedPod
                                                .getInternalResource()
                                                .getSpec()
                                                .toBuilder()
                                                .withRestartPolicy("Always")
                                                .editMatchingContainer(
                                                        KubernetesTaskManagerFactory
                                                                ::findMainContainerBuilder)
                                                .addToEnv(RESOURCE_ID_ENV_VALUE)
                                                .endContainer()
                                                .build()))
                        .endSpec()
                        .build();

        return new KubernetesStatefulSet(statefulSet);
    }

    private static boolean findMainContainerBuilder(ContainerBuilder containerBuilder) {
        return containerBuilder.getName().equals(Constants.MAIN_CONTAINER_NAME);
    }
}
