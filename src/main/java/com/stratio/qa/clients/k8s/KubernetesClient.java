/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.qa.clients.k8s;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.stratio.qa.specs.CommandExecutionSpec;
import com.stratio.qa.specs.CommonG;
import com.stratio.qa.utils.ThreadProperty;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinitionList;
import io.fabric8.kubernetes.api.model.apps.*;
import io.fabric8.kubernetes.api.model.rbac.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.extended.run.RunConfigBuilder;
import io.fabric8.kubernetes.client.internal.SerializationUtils;
import okhttp3.Response;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class KubernetesClient {

    private static KubernetesClient CLIENT;

    private static io.fabric8.kubernetes.client.KubernetesClient k8sClient;

    private static CountDownLatch execLatch;

    private static final Logger logger = LoggerFactory.getLogger(KubernetesClient.class);

    public static KubernetesClient getInstance() {
        if (CLIENT == null) {
            CLIENT = new KubernetesClient();
        }
        return CLIENT;
    }

    private KubernetesClient() {

    }

    public static Logger getLogger() {
        return logger;
    }

    public void connect(String kubeConfigPath) throws IOException {
        StringBuilder contentBuilder = new StringBuilder();
        try (Stream<String> stream = Files.lines(Paths.get(kubeConfigPath), StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append("\n"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        k8sClient = new DefaultKubernetesClient(Config.fromKubeconfig(contentBuilder.toString()));
    }

    public void getK8sConfigFromWorkspace(CommonG commonspec) throws Exception {
        String clusterName = System.getProperty("KEOS_CLUSTER_ID");
        if (clusterName == null) {
            commonspec.getLogger().info("Info cannot be retrieved from workspace without KEOS_CLUSTER_ID variable");
            return;
        }

        String daedalusSystem = "keos-workspaces.int.stratio.com";
        String workspaceName = "keos-workspace-" + clusterName;
        String workspaceURL = "http://" + daedalusSystem + "/" + workspaceName + ".tgz";

        // Download workspace
        String commandWget = "wget " + workspaceURL;
        commonspec.runLocalCommand(commandWget);

        // Untar workspace
        CommandExecutionSpec commandExecutionSpec = new CommandExecutionSpec(commonspec);
        String commandUntar = "tar -C target/test-classes/ -xvf " + workspaceName + ".tgz";
        commandExecutionSpec.executeLocalCommand(commandUntar, null, null);

        // Clean
        String commandRmTgz = "rm " + workspaceName + ".tgz";
        commandExecutionSpec.executeLocalCommand(commandRmTgz, null, null);

        // Obtain and export values
        String daedalusJson = commonspec.retrieveData(workspaceName + "/keos.json", "json");
        ThreadProperty.set("CLUSTER_SSH_USER", commonspec.getJSONPathString(daedalusJson, "$.infra.ssh_user", null).replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\"", ""));
        ThreadProperty.set("CLUSTER_SSH_PEM_PATH", "./target/test-classes/" + workspaceName + "/key");
        ThreadProperty.set("CLUSTER_KUBE_CONFIG_PATH", "./target/test-classes/" + workspaceName + "/.kube/config");
    }

    /**
     * kubectl get pods -n namespace
     * @param namespace
     */
    public String getNamespacePods(String namespace) {
        StringBuilder result = new StringBuilder();
        PodList podList = namespace != null ? k8sClient.pods().inNamespace(namespace).list() : k8sClient.pods().inAnyNamespace().list();
        for (Pod pod : podList.getItems()) {
            result.append(pod.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl get namespaces
     */
    public String getAllNamespaces() {
        StringBuilder result = new StringBuilder();
        for (Namespace namespace : k8sClient.namespaces().list().getItems()) {
            result.append(namespace.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();

    }

    /**
     * Return pod object
     *
     * @param podName Pod name
     * @param namespace Namespace
     * @return Pod object
     */
    public Pod getPod(String podName, String namespace) {
        return namespace != null ? k8sClient.pods().inNamespace(namespace).withName(podName).get() : k8sClient.pods().withName(podName).get();
    }

    /**
     * Return deployment object
     *
     * @param deploymentName Deployment name
     * @param namespace Namespace
     * @return Deployment object
     */
    public Deployment getDeployment(String deploymentName, String namespace) {
        return k8sClient.apps().deployments().inNamespace(namespace).withName(deploymentName).get();
    }

    /**
     * kubectl describe pod
     *
     * @param podName Pod name
     * @param namespace Namespace (optional)
     * @return String with pod yaml
     * @throws JsonProcessingException
     */
    public String describePodYaml(String podName, String namespace) throws JsonProcessingException {
        return SerializationUtils.dumpAsYaml(getPod(podName, namespace));
    }

    /**
     * kubectl describe service myservice
     *
     * @param serviceName Service
     * @param namespace Namespace
     * @return String with service yaml
     */
    public String describeServiceYaml(String serviceName, String namespace) throws Exception {
        if (namespace == null) {
            throw new Exception("Namespace is mandatory");
        }
        return SerializationUtils.dumpAsYaml(k8sClient.services().inNamespace(namespace).withName(serviceName).get());
    }

    /**
     * kubectl describe deployment myDeployment
     *
     * @param deploymentName Service
     * @param namespace Namespace
     * @return String with service yaml
     */
    public String describeDeploymentYaml(String deploymentName, String namespace) throws Exception {
        return SerializationUtils.dumpAsYaml(getDeployment(deploymentName, namespace));
    }

    /**
     * kubectl describe pgcluster
     *
     * @param kind kind custom resource (PgCluster)
     * @param nameItem pgcluster name
     * @param namespace Namespace (optional)
     * @return String with custom resource in json format
     * @throws JsonProcessingException
     */
    public String describeCustomResourceJson(String kind, String nameItem, String namespace, String version, String plural, String name, String scope, String group) throws JsonProcessingException {
        CustomResourceDefinitionContext customResourceDefinitionContext = new CustomResourceDefinitionContext.Builder()
                .withVersion(version)
                .withPlural(plural)
                .withKind(kind)
                .withName(name)
                .withScope(scope)
                .withGroup(group)
                .build();
        Map<String, Object> customResourceMap = k8sClient.customResource(customResourceDefinitionContext).list(namespace);
        for (Map<String, Object> itemMap : ((List<Map<String, Object>>) customResourceMap.get("items"))) {
            if (((Map<String, Object>)  itemMap.get("metadata")).get("name").equals(nameItem)) {
                return new JSONObject(itemMap).toString();
            }
        }
        return null;
    }

    /**
     * kubectl apply -f yamlOrJsonFile.yml
     *
     * @param file
     * @param namespace
     * @throws FileNotFoundException
     */
    public void createOrReplaceResource(String file, String namespace) throws FileNotFoundException {
        k8sClient.load(new FileInputStream(file))
                .inNamespace(namespace)
                .createOrReplace();
    }

    /**
     * kubectl apply -f yamlOrJsonFile.yml
     * Using a custom resource
     *
     * @param file
     * @param namespace
     * @throws FileNotFoundException
     */
    public void createOrReplaceCustomResource(String createOrModify, String file, String namespace, String version, String plural, String kind, String name, String scope, String group) throws IOException {
        CustomResourceDefinitionContext customResourceDefinitionContext = new CustomResourceDefinitionContext.Builder()
                .withVersion(version)
                .withPlural(plural)
                .withKind(kind)
                .withName(name)
                .withScope(scope)
                .withGroup(group)
                .build();
        Map<String, Object> myObject = k8sClient.customResource(customResourceDefinitionContext).load(new FileInputStream(file));
        if (createOrModify.equals("create")) {
            k8sClient.customResource(customResourceDefinitionContext).create(namespace, myObject);
        } else { //modify
            k8sClient.customResource(customResourceDefinitionContext).updateStatus(namespace, myObject);
        }
    }

    /**
     * kubectl get pgcluster -n xxxxx
     * Using a custom resource
     *
     * @param namespace
     */
    public String getCustomResource(String kind, String namespace, String version, String plural, String name, String scope, String group) throws IOException {
        CustomResourceDefinitionContext customResourceDefinitionContext = new CustomResourceDefinitionContext.Builder()
                .withVersion(version)
                .withPlural(plural)
                .withKind(kind)
                .withName(name)
                .withScope(scope)
                .withGroup(group)
                .build();
        Map<String, Object> customResourceMap = k8sClient.customResource(customResourceDefinitionContext).list(namespace);
        StringBuilder result = new StringBuilder();
        for (Map<String, Object> itemMap : ((List<Map<String, Object>>) customResourceMap.get("items"))) {
            result.append(((Map<String, Object>)  itemMap.get("metadata")).get("name")).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * Using a custom resource
     *
     * @param namespace
     */
    public Integer getReadyReplicasCustomResource(String kind, String nameItem, String namespace, String version, String plural, String name, String scope, String group) throws IOException {
        CustomResourceDefinitionContext customResourceDefinitionContext = new CustomResourceDefinitionContext.Builder()
                .withVersion(version)
                .withPlural(plural)
                .withKind(kind)
                .withName(name)
                .withScope(scope)
                .withGroup(group)
                .build();
        Map<String, Object> customResourceMap = k8sClient.customResource(customResourceDefinitionContext).list(namespace);
        StringBuilder result = new StringBuilder();
        Integer replicas = 0;
        for (Map<String, Object> itemMap : ((List<Map<String, Object>>) customResourceMap.get("items"))) {
            if (((Map<String, Object>)  itemMap.get("metadata")).get("name").equals(nameItem)) {
                return Integer.valueOf(result.append(((Map<String, Object>)  itemMap.get("status")).get("ReadyInstances")).toString().split("/")[0]);
            }
        }
        return replicas;
    }

    /**
     * kubectl create deployment xxx -n namespace --image myimage
     *
     * @param deploymentName Deployment name
     * @param namespace Namespace
     * @param image Image
     * @param imagePullPolicy Image pull policy (IfNotPresent as default value)
     */
    public void createDeployment(String deploymentName, String namespace, String image, String imagePullPolicy) {
        imagePullPolicy = imagePullPolicy != null ? imagePullPolicy : "IfNotPresent";
        Deployment deployment = new DeploymentBuilder()
                .withNewMetadata()
                .withName(deploymentName)
                .endMetadata()
                .withNewSpec()
                .withReplicas(1)
                .withNewTemplate()
                .withNewMetadata()
                .addToLabels("app", deploymentName)
                .endMetadata()
                .withNewSpec()
                .addNewContainer()
                .withName(deploymentName)
                .withImage(image)
                .withImagePullPolicy(imagePullPolicy)
                .endContainer()
                .endSpec()
                .endTemplate()
                .withNewSelector()
                .addToMatchLabels("app", deploymentName)
                .endSelector()
                .endSpec()
                .build();
        k8sClient.apps().deployments().inNamespace(namespace).create(deployment);
    }

    /**
     * kubectl expose deployment xxx -n namespace --port=xxxx
     *
     * @param deploymentName Deployment to expose
     * @param namespace Namespace
     * @param port Port to expose
     */
    public void exposeDeployment(String deploymentName, String namespace, Integer port) {
        Service service = new ServiceBuilder()
                .withNewMetadata()
                .withName(deploymentName)
                .endMetadata()
                .withNewSpec()
                .withSelector(Collections.singletonMap("app", deploymentName))
                .addNewPort()
                .withProtocol("TCP")
                .withPort(port)
                .withTargetPort(new IntOrString(port))
                .endPort()
                .endSpec()
                .build();
        k8sClient.services().inNamespace(namespace).create(service);
    }

    /**
     * kubectl logs pod
     *
     * @param pod Pod name
     * @param namespace Namespace
     * @return pod log
     */
    public String getPodLog(String pod, String namespace) {
        return k8sClient.pods().inNamespace(namespace).withName(pod).getLog();
    }

    /**
     * kubectl exec mypod -- command
     * @param pod Pod
     * @param namespace Namespace
     * @param command Command to execute
     * @throws InterruptedException
     */
    public String execCommand(String pod, String namespace, String[] command) throws InterruptedException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        execLatch = new CountDownLatch(1);

        ExecWatch execWatch = k8sClient.pods().inNamespace(namespace).withName(pod)
                .writingOutput(out)
                .writingError(error)
                .usingListener(new MyPodExecListener())
                .exec(command);

        boolean latchTerminationStatus = execLatch.await(5, TimeUnit.SECONDS);
        if (!latchTerminationStatus) {
            logger.warn("Latch could not terminate within specified time");
        }
        logger.debug("Exec Output: {} ", out);
        execWatch.close();
        return out.toString();
    }

    /**
     * kubectl run xxx --image=xxx --restart=xxx --serviceaccount=xxx --namespace=xxx --command -- mycommand
     *
     * @param podName Pod name
     * @param namespace Namespace
     * @param image Image
     * @param imagePullPolicy Image pull policy (IfNotPresent as default value)
     * @param restartPolicy Restart policy
     * @param serviceAccount Service Account
     * @param command Command to execute
     * @param args Command arguments
     */
    public void runPod(String podName, String namespace, String image, String imagePullPolicy, String restartPolicy, String serviceAccount, String command, List<String> args) {
        imagePullPolicy = imagePullPolicy != null ? imagePullPolicy : "IfNotPresent";
        RunConfigBuilder runConfig = new RunConfigBuilder()
                .withName(podName)
                .withImage(image)
                .withImagePullPolicy(imagePullPolicy)
                .withRestartPolicy(restartPolicy)
                .withServiceAccount(serviceAccount)
                .withCommand(command)
                .withArgs(args);
        k8sClient.run().inNamespace(namespace).withName(podName).withImage(image).withRunConfig(runConfig.build()).done();
    }

    /**
     * kubectl delete pod mypod
     *
     * @param pod Pod to delete
     * @param namespace Namespace
     */
    public void deletePod(String pod, String namespace) {
        k8sClient.pods().inNamespace(namespace).withName(pod).delete();
    }

    /**
     * kubectl delete deployment mydeployment
     *
     * @param deployment Deployment to delete
     * @param namespace Namespace
     */
    public void deleteDeployment(String deployment, String namespace) {
        k8sClient.apps().deployments().inNamespace(namespace).withName(deployment).delete();
    }

    /**
     * kubectl delete service myservice
     *
     * @param service Service to delete
     * @param namespace Namespace
     */
    public void deleteService(String service, String namespace) {
        k8sClient.services().inNamespace(namespace).withName(service).delete();
    }

    /**
     * kubectl delete pgcluster mypgcluster
     * Using a custom resource
     *
     * @param nameItem
     * @param namespace
     */
    public void deleteCustomResourceItem(String kind, String nameItem, String namespace, String version, String plural, String name, String scope, String group) throws IOException {
        CustomResourceDefinitionContext customResourceDefinitionContext = new CustomResourceDefinitionContext.Builder()
                .withVersion(version)
                .withPlural(plural)
                .withKind(kind)
                .withName(name)
                .withScope(scope)
                .withGroup(group)
                .build();
        k8sClient.customResource(customResourceDefinitionContext).delete(namespace, nameItem);
    }

    /**
     * kubectl scale --replicas=4 -n namespace deploy/xxx
     *
     * @param deployment deployment to scale
     * @param namespace Namespace
     */
    public void scaleDeployment(String deployment, String namespace, Integer instances) {
        k8sClient.apps().deployments().inNamespace(namespace).withName(deployment).scale(instances);
    }

    /**
     * private function which return label selector
     *
     * @param selector Label filter (separated by comma)
     * @return LabelSelector
     */
    private LabelSelector getLabelSelector (String selector) {
        String[] arraySelector = selector.split(",");
        LabelSelector labelSelector = new LabelSelector();
        Map<String, String> expressions = new HashMap<>();
        for (String sel : arraySelector) {
            String labelKey = sel.split("=")[0];
            String labelValue = sel.split("=")[1];
            expressions.put(labelKey, labelValue);
        }
        labelSelector.setMatchLabels(expressions);
        return labelSelector;
    }

    /**
     * kubectl get pods --selector=version=v1 -o jsonpath='{.items[*].metadata.name}'
     *
     * @param selector Label filter (separated by comma)
     * @param namespace Namespace
     * @return Pods list filtered
     */
    public String getPodsFilteredByLabel(String selector, String namespace) {
        LabelSelector labelSelector;
        labelSelector = getLabelSelector(selector);
        StringBuilder result = new StringBuilder();
        PodList podList = namespace != null ?
                k8sClient.pods().inNamespace(namespace).withLabelSelector(labelSelector).list() :
                k8sClient.pods().inAnyNamespace().withLabelSelector(labelSelector).list();
        for (Pod pod : podList.getItems()) {
            result.append(pod.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl get deployments --selector=version=v1 -o jsonpath='{.items[*].metadata.name}'
     *
     * @param selector Label filter (separated by comma)
     * @param namespace Namespace
     * @return Deployments list filtered
     */
    public String getDeploymentsFilteredByLabel(String selector, String namespace) {
        LabelSelector labelSelector;
        labelSelector = getLabelSelector(selector);
        StringBuilder result = new StringBuilder();
        DeploymentList deploymentList = namespace != null ?
                k8sClient.apps().deployments().inNamespace(namespace).withLabelSelector(labelSelector).list() :
                k8sClient.apps().deployments().inAnyNamespace().withLabelSelector(labelSelector).list();
        for (Deployment deployment : deploymentList.getItems()) {
            result.append(deployment.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl get replicasets --selector=version=v1 -o jsonpath='{.items[*].metadata.name}'
     *
     * @param selector Label filter (separated by comma)
     * @param namespace Namespace
     * @return Replicasets list filtered
     */
    public String getReplicaSetsFilteredByLabel(String selector, String namespace) {
        LabelSelector labelSelector;
        labelSelector = getLabelSelector(selector);
        StringBuilder result = new StringBuilder();
        ReplicaSetList replicasetList = namespace != null ?
                k8sClient.apps().replicaSets().inNamespace(namespace).withLabelSelector(labelSelector).list() :
                k8sClient.apps().replicaSets().inAnyNamespace().withLabelSelector(labelSelector).list();
        for (ReplicaSet replicaset : replicasetList.getItems()) {
            result.append(replicaset.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl get services --selector=version=v1 -o jsonpath='{.items[*].metadata.name}'
     *
     * @param selector Label filter (separated by comma)
     * @param namespace Namespace
     * @return services list filtered
     */
    public String getServicesFilteredByLabel(String selector, String namespace) {
        LabelSelector labelSelector;
        labelSelector = getLabelSelector(selector);
        StringBuilder result = new StringBuilder();
        ServiceList serviceList = namespace != null ?
                k8sClient.services().inNamespace(namespace).withLabelSelector(labelSelector).list() :
                k8sClient.services().inAnyNamespace().withLabelSelector(labelSelector).list();
        for (Service service : serviceList.getItems()) {
            result.append(service.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl get statefulsets --selector=version=v1 -o jsonpath='{.items[*].metadata.name}'
     *
     * @param selector Label filter (separated by comma)
     * @param namespace Namespace
     * @return StateFulSets list filtered
     */
    public String getStateFulSetsFilteredByLabel(String selector, String namespace) {
        LabelSelector labelSelector;
        labelSelector = getLabelSelector(selector);
        StringBuilder result = new StringBuilder();
        StatefulSetList statefulSetList = namespace != null ?
                k8sClient.apps().statefulSets().inNamespace(namespace).withLabelSelector(labelSelector).list() :
                k8sClient.apps().statefulSets().inAnyNamespace().withLabelSelector(labelSelector).list();
        for (StatefulSet statefulSet : statefulSetList.getItems()) {
            result.append(statefulSet.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl get configmaps --selector=version=v1 -o jsonpath='{.items[*].metadata.name}'
     *
     * @param selector Label filter (separated by comma)
     * @param namespace Namespace
     * @return ConfigMaps list filtered
     */
    public String getConfigMapsFilteredByLabel(String selector, String namespace) {
        LabelSelector labelSelector;
        labelSelector = getLabelSelector(selector);
        StringBuilder result = new StringBuilder();
        ConfigMapList configMapList = namespace != null ?
                k8sClient.configMaps().inNamespace(namespace).withLabelSelector(labelSelector).list() :
                k8sClient.configMaps().inAnyNamespace().withLabelSelector(labelSelector).list();
        for (ConfigMap configMap : configMapList.getItems()) {
            result.append(configMap.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl get serviceaccounts --selector=version=v1 -o jsonpath='{.items[*].metadata.name}'
     *
     * @param selector Label filter (separated by comma)
     * @param namespace Namespace
     * @return ServiceAccounts list filtered
     */
    public String getServiceAccountsFilteredByLabel(String selector, String namespace) {
        LabelSelector labelSelector;
        labelSelector = getLabelSelector(selector);
        StringBuilder result = new StringBuilder();
        ServiceAccountList serviceAccountList = namespace != null ?
                k8sClient.serviceAccounts().inNamespace(namespace).withLabelSelector(labelSelector).list() :
                k8sClient.serviceAccounts().inAnyNamespace().withLabelSelector(labelSelector).list();
        for (ServiceAccount serviceAccount : serviceAccountList.getItems()) {
            result.append(serviceAccount.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl get roles --selector=version=v1 -o jsonpath='{.items[*].metadata.name}'
     *
     * @param selector Label filter (separated by comma)
     * @param namespace Namespace
     * @return Roles list filtered
     */
    public String getRolesFilteredByLabel(String selector, String namespace) {
        LabelSelector labelSelector;
        labelSelector = getLabelSelector(selector);
        StringBuilder result = new StringBuilder();
        RoleList roleList = namespace != null ?
                k8sClient.rbac().roles().inNamespace(namespace).withLabelSelector(labelSelector).list() :
                k8sClient.rbac().roles().inAnyNamespace().withLabelSelector(labelSelector).list();
        for (Role role : roleList.getItems()) {
            result.append(role.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl get rolebindings --selector=version=v1 -o jsonpath='{.items[*].metadata.name}'
     *
     * @param selector Label filter (separated by comma)
     * @param namespace Namespace
     * @return Role list filtered
     */
    public String getRolesBindingsFilteredByLabel(String selector, String namespace) {
        LabelSelector labelSelector;
        labelSelector = getLabelSelector(selector);
        StringBuilder result = new StringBuilder();
        RoleBindingList roleBindingList = namespace != null ?
                k8sClient.rbac().roleBindings().inNamespace(namespace).withLabelSelector(labelSelector).list() :
                k8sClient.rbac().roleBindings().inAnyNamespace().withLabelSelector(labelSelector).list();
        for (RoleBinding roleBinding : roleBindingList.getItems()) {
            result.append(roleBinding.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl get pods --field-selector=status.phase=Running
     *
     * @param selector Label filter (separated by comma)
     * @param namespace Namespace
     * @return Pods list filtered
     */
    public String getPodsFilteredByField(String selector, String namespace) {
        String[] arraySelector = selector.split(",");
        Map<String, String> fields = new HashMap<>();
        for (String sel : arraySelector) {
            String fieldKey = sel.split("=")[0];
            String fieldValue = sel.split("=")[1];
            fields.put(fieldKey, fieldValue);
        }
        StringBuilder result = new StringBuilder();
        PodList podList = namespace != null ?
                k8sClient.pods().inNamespace(namespace).withFields(fields).list() :
                k8sClient.pods().inAnyNamespace().withFields(fields).list();
        for (Pod pod : podList.getItems()) {
            result.append(pod.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * Get a configmap
     *
     * @param name Config map name
     * @param namespace Namespace
     */
    public ConfigMap getConfigMap(String name, String namespace) {
        return k8sClient.configMaps().inNamespace(namespace).withName(name).get();
    }

    /**
     * Get configmap list in selected namespace
     *
     * @param namespace Namespace
     * @return Configmap list
     */
    public String getConfigMapList(String namespace) {
        StringBuilder result = new StringBuilder();
        for (ConfigMap configMap : k8sClient.configMaps().inNamespace(namespace).list().getItems()) {
            result.append(configMap.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl describe configmap xxx -n namespace
     *
     * @param configMapName Config map name
     * @param namespace Namespace
     *
     * @return String with config map
     */
    public String describeConfigMap(String configMapName, String namespace)  {
        return getConfigMap(configMapName, namespace).getData().toString();
    }

    /**
     * Get a replicaset
     *
     * @param name Replicaset name
     * @param namespace Namespace
     */
    public ReplicaSet getReplicaSet(String name, String namespace) {
        return k8sClient.apps().replicaSets().inNamespace(namespace).withName(name).get();
    }

    /**
     * Get replicaset list in selected namespace
     *
     * @param namespace Namespace
     * @return Replicaset list
     */
    public String getReplicaSetList(String namespace) {
        StringBuilder result = new StringBuilder();
        for (ReplicaSet replicaSet : k8sClient.apps().replicaSets().inNamespace(namespace).list().getItems()) {
            result.append(replicaSet.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl describe replicaset xxx -n namespace
     *
     * @param replicaSetName Config map name
     * @param namespace Namespace
     *
     * @return String with replicaset
     */
    public String describeReplicaSet(String replicaSetName, String namespace) throws JsonProcessingException {
        return SerializationUtils.dumpAsYaml(getReplicaSet(replicaSetName, namespace));
    }

    /**
     * Get a serviceAccount
     *
     * @param name serviceAccount name
     * @param namespace Namespace
     */
    public ServiceAccount getServiceAccount(String name, String namespace) {
        return k8sClient.serviceAccounts().inNamespace(namespace).withName(name).get();
    }

    /**
     * Get ServiceAccount list in selected namespace
     *
     * @param namespace Namespace
     * @return ServiceAccount list
     */
    public String getServiceAccountList(String namespace) {
        StringBuilder result = new StringBuilder();
        for (ServiceAccount serviceAccount : k8sClient.serviceAccounts().inNamespace(namespace).list().getItems()) {
            result.append(serviceAccount.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl describe serviceaccount xxx -n namespace
     *
     * @param serviceAccountName serviceAccount name
     * @param namespace Namespace
     *
     * @return String with serviceAccount
     */
    public String describeServiceAccount(String serviceAccountName, String namespace) throws JsonProcessingException {
        return SerializationUtils.dumpAsYaml(getServiceAccount(serviceAccountName, namespace));
    }

    /**
     * Get secret
     *
     * @param name secret name
     * @param namespace Namespace
     */
    public Secret getSecret(String name, String namespace) {
        return k8sClient.secrets().inNamespace(namespace).withName(name).get();
    }

    /**
     * Get Secrets list in selected namespace
     *
     * @param namespace Namespace
     * @return Secrets list
     */
    public String getSecretsList(String namespace) {
        StringBuilder result = new StringBuilder();
        for (Secret secret : k8sClient.secrets().inNamespace(namespace).list().getItems()) {
            result.append(secret.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl describe secret xxx -n namespace
     *
     * @param secretName secret name
     * @param namespace Namespace
     *
     * @return String with secret
     */
    public String describeSecret(String secretName, String namespace) throws JsonProcessingException {
        return SerializationUtils.dumpAsYaml(getSecret(secretName, namespace));
    }

    /**
     * Get clusterrole
     *
     * @param name clusterrole name
     * @param namespace Namespace
     */
    public ClusterRole getClusterRole(String name, String namespace) {
        return k8sClient.rbac().clusterRoles().inNamespace(namespace).withName(name).get();
    }

    /**
     * Get clusterrole list in selected namespace
     *
     * @param namespace Namespace
     * @return clusterrole list
     */
    public String getClusterRoleList(String namespace) {
        StringBuilder result = new StringBuilder();
        for (ClusterRole clusterRole : k8sClient.rbac().clusterRoles().inNamespace(namespace).list().getItems()) {
            result.append(clusterRole.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl describe clusterrole xxx -n namespace
     *
     * @param crName clusterrole name
     * @param namespace Namespace
     *
     * @return String with clusterrole
     */
    public String describeClusterRole(String crName, String namespace) throws JsonProcessingException {
        return SerializationUtils.dumpAsYaml(getClusterRole(crName, namespace));
    }

    /**
     * Get clusterrolebinding
     *
     * @param name clusterrolebinding name
     * @param namespace Namespace
     */
    public ClusterRoleBinding getClusterRoleBinding(String name, String namespace) {
        return k8sClient.rbac().clusterRoleBindings().inNamespace(namespace).withName(name).get();
    }

    /**
     * Get clusterrolebinding list in selected namespace
     *
     * @param namespace Namespace
     * @return clusterrolebinding list
     */
    public String getClusterRoleBindingList(String namespace) {
        StringBuilder result = new StringBuilder();
        for (ClusterRoleBinding clusterRoleBinding : k8sClient.rbac().clusterRoleBindings().inNamespace(namespace).list().getItems()) {
            result.append(clusterRoleBinding.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl describe clusterrolebinding xxx -n namespace
     *
     * @param crName clusterrolebinding name
     * @param namespace Namespace
     *
     * @return String with clusterrolebinding
     */
    public String describeClusterRoleBinding(String crName, String namespace) throws JsonProcessingException {
        return SerializationUtils.dumpAsYaml(getClusterRoleBinding(crName, namespace));
    }

    /**
     * Get statefulset
     *
     * @param name statefulset name
     * @param namespace Namespace
     */
    public StatefulSet getStateFulSet(String name, String namespace) {
        return k8sClient.apps().statefulSets().inNamespace(namespace).withName(name).get();
    }

    /**
     * Get statefulset list in selected namespace
     *
     * @param namespace Namespace
     * @return statefulset list
     */
    public String getStateFulSetList(String namespace) {
        StringBuilder result = new StringBuilder();
        for (StatefulSet statefulSet : k8sClient.apps().statefulSets().inNamespace(namespace).list().getItems()) {
            result.append(statefulSet.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl describe statefulset xxx -n namespace
     *
     * @param stateFulSetName statefulset name
     * @param namespace Namespace
     *
     * @return String with statefulset
     */
    public String describeStateFulSet(String stateFulSetName, String namespace) throws JsonProcessingException {
        return SerializationUtils.dumpAsYaml(getStateFulSet(stateFulSetName, namespace));
    }

    /**
     * Get role
     *
     * @param name role name
     * @param namespace Namespace
     */
    public Role getRole(String name, String namespace) {
        return k8sClient.rbac().roles().inNamespace(namespace).withName(name).get();
    }

    /**
     * Get role list in selected namespace
     *
     * @param namespace Namespace
     * @return role list
     */
    public String getRoleList(String namespace) {
        StringBuilder result = new StringBuilder();
        for (Role role : k8sClient.rbac().roles().inNamespace(namespace).list().getItems()) {
            result.append(role.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl describe role xxx -n namespace
     *
     * @param roleName role name
     * @param namespace Namespace
     *
     * @return String with role
     */
    public String describeRole(String roleName, String namespace) throws JsonProcessingException {
        return SerializationUtils.dumpAsYaml(getRole(roleName, namespace));
    }

    /**
     * Get rolebinding
     *
     * @param name rolebinding name
     * @param namespace Namespace
     */
    public RoleBinding getRoleBinding(String name, String namespace) {
        return k8sClient.rbac().roleBindings().inNamespace(namespace).withName(name).get();
    }

    /**
     * Get rolebinding list in selected namespace
     *
     * @param namespace Namespace
     * @return rolebinding list
     */
    public String getRoleBindingList(String namespace) {
        StringBuilder result = new StringBuilder();
        for (RoleBinding roleBinding : k8sClient.rbac().roleBindings().inNamespace(namespace).list().getItems()) {
            result.append(roleBinding.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**

     /**
     * Get customresourcedefinition list
     *
     * @return customresourcedefinition list
     */

    public String getCustomResourceDefinitionList() {
        StringBuilder result = new StringBuilder();
        for (CustomResourceDefinition customResourceDefinition : k8sClient.customResourceDefinitions().list().getItems()) {
            result.append(customResourceDefinition.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * Get deployment list in selected namespace
     *
     * @param namespace Namespace
     * @return deployment list
     */

    public String getDeploymentList(String namespace) {
        StringBuilder result = new StringBuilder();
        for (Deployment deployment : k8sClient.apps().deployments().inNamespace(namespace).list().getItems()) {
            result.append(deployment.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * Get service list in selected namespace
     *
     * @param namespace Namespace
     * @return service list
     */

    public String getServiceList(String namespace) {
        StringBuilder result = new StringBuilder();
        for (Service service : k8sClient.services().inNamespace(namespace).list().getItems()) {
            result.append(service.getMetadata().getName()).append("\n");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : result.toString();
    }

    /**
     * kubectl describe rolebinding xxx -n namespace
     *
     * @param roleName rolebinding name
     * @param namespace Namespace
     *
     * @return String with rolebinding
     */
    public String describeRoleBinding(String roleName, String namespace) throws JsonProcessingException {
        return SerializationUtils.dumpAsYaml(getRoleBinding(roleName, namespace));
    }

    private static class MyPodExecListener implements ExecListener {
        @Override
        public void onOpen(Response response) {
            getLogger().debug("K8S Shell was opened");
        }

        @Override
        public void onFailure(Throwable throwable, Response response) {
            getLogger().warn("Some error encountered in K8S Shell");
            execLatch.countDown();
        }

        @Override
        public void onClose(int i, String s) {
            getLogger().debug("K8S Shell Closing");
            execLatch.countDown();
        }
    }
}
