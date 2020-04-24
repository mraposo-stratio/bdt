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

package com.stratio.qa.specs;

import com.auth0.jwt.JWTSigner;
import com.jayway.jsonpath.JsonPath;
import com.ning.http.client.Response;
import com.stratio.qa.utils.GosecSSOUtils;
import com.stratio.qa.utils.RemoteSSHConnection;
import com.stratio.qa.utils.RemoteSSHConnectionsUtil;
import com.stratio.qa.utils.ThreadProperty;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.assertj.core.api.Assertions;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.stratio.qa.assertions.Assertions.assertThat;
import static org.testng.Assert.fail;

/**
 * Generic DC/OS Specs.
 *
 * @see <a href="DcosSpec-annotations.html">DCOS Steps &amp; Matching Regex</a>
 */
public class DcosSpec extends BaseGSpec {

    private final Logger logger = LoggerFactory.getLogger(DcosSpec.class);

    String descriptorPath = "/stratio_volume/descriptor.json";

    String vaultResponsePath = "/stratio_volume/vault_response";

    /**
     * Generic constructor.
     *
     * @param spec object
     */
    public DcosSpec(CommonG spec) {
        this.commonspec = spec;
    }

    /**
     * Authenticate in a DCOS cluster
     *
     * @param remoteHost remote host
     * @param email      email for JWT singing
     * @param user       remote user
     * @param password   (required if pemFile null)
     * @throws Exception exception
     */
    @Given("^I authenticate to DCOS cluster '(.+?)' using email '(.+?)' with user '(.+?)' and password '(.+?)'( over SSH port '(.+?)')?$")
    public void authenticateDCOSpassword(String remoteHost, String email, String user, String password, String remotePort) throws Exception {
        authenticateDCOS(remoteHost, email, user, password, null, remotePort);
    }

    /**
     * Authenticate in a DCOS cluster
     *
     * @param remoteHost remote host
     * @param email      email for JWT singing
     * @param user       remote user
     * @param pemFile    (required if password null)
     * @throws Exception exception
     */
    @Given("^I authenticate to DCOS cluster '(.+?)' using email '(.+?)' with user '(.+?)' and pem file '(.+?)'( over SSH port '(.+?)')?$")
    public void authenticateDCOSpem(String remoteHost, String email, String user, String pemFile, String remotePort) throws Exception {
        authenticateDCOS(remoteHost, email, user, null, pemFile, remotePort);
    }

    /**
     * Authenticate in a DCOS cluster
     *
     * @param remoteHost remote host
     * @param email      email for JWT singing
     * @param user       remote user
     * @param password   (required if pemFile null)
     * @param pemFile    (required if password null)
     * @throws Exception exception
     */
    private void authenticateDCOS(String remoteHost, String email, String user, String password, String pemFile, String remotePort) throws Exception {
        commonspec.setRemoteSSHConnection(new RemoteSSHConnection(user, password, remoteHost, remotePort, pemFile), remoteHost + "_" + user);
        commonspec.getRemoteSSHConnection().runCommand("sudo cat /var/lib/dcos/dcos-oauth/auth-token-secret");
        String DCOSsecret = commonspec.getRemoteSSHConnection().getResult().trim();
        setDCOSCookie(DCOSsecret, email);
    }

    public void setDCOSCookie(String DCOSsecret, String email) throws Exception {
        final JWTSigner signer = new JWTSigner(DCOSsecret);
        final HashMap<String, Object> claims = new HashMap();
        claims.put("uid", email);
        final String jwt = signer.sign(claims);
        com.ning.http.client.cookie.Cookie cookie = new com.ning.http.client.cookie.Cookie("dcos-acs-auth-cookie", jwt, false, "", "", 99999, false, false);
        List<com.ning.http.client.cookie.Cookie> cookieList = new ArrayList<com.ning.http.client.cookie.Cookie>();
        cookieList.add(cookie);
        commonspec.setCookies(cookieList);
        ThreadProperty.set("dcosAuthCookie", jwt);
    }

    /**
     * Generate token to authenticate in gosec SSO
     *
     * @param ssoHost  current sso host
     * @param userName username
     * @param passWord password
     * @throws Exception exception
     */
    @Given("^I( do not)? set sso( governance)? token using host '(.+?)' with user '(.+?)' and password '(.+?)'( and tenant '(.+?)')?( without host name verification)?$")
    public void setGoSecSSOCookie(String set, String gov, String ssoHost, String userName, String passWord, String tenant, String hostVerifier) throws Exception {
        if (set == null) {
            GosecSSOUtils ssoUtils = new GosecSSOUtils(ssoHost, userName, passWord, tenant, gov);
            ssoUtils.setVerifyHost(hostVerifier == null);
            HashMap<String, String> ssoCookies = ssoUtils.ssoTokenGenerator();

            String[] tokenList = {"user", "dcos-acs-auth-cookie"};
            if (gov != null) {
                tokenList = new String[]{"user", "dcos-acs-auth-cookie", "stratio-governance-auth"};
            }
            List<com.ning.http.client.cookie.Cookie> cookiesAtributes = addSsoToken(ssoCookies, tokenList);

            this.commonspec.getLogger().debug("Cookies to set:");
            for (String cookie:tokenList) {
                this.commonspec.getLogger().debug("\t" + cookie + ":" + ssoCookies.get(cookie));
            }

            if (ssoCookies.get("dcos-acs-auth-cookie") != null) {
                ThreadProperty.set("dcosAuthCookie", ssoCookies.get("dcos-acs-auth-cookie"));
            }

            if (ssoCookies.get("stratio-governance-auth") != null) {
                ThreadProperty.set("dcosGovernanceAuthCookie", ssoCookies.get("stratio-governance-auth"));
            }
            commonspec.setCookies(cookiesAtributes);
        }
    }

    public List<com.ning.http.client.cookie.Cookie> addSsoToken(HashMap<String, String> ssoCookies, String[] tokenList) {
        List<com.ning.http.client.cookie.Cookie> cookiesAttributes = new ArrayList<>();

        for (String tokenKey : tokenList) {
            cookiesAttributes.add(new com.ning.http.client.cookie.Cookie(tokenKey, ssoCookies.get(tokenKey),
                    false, null,
                    null, 999999, false, false));
        }
        return cookiesAttributes;
    }

    /**
     * Checks if there are any unused nodes in the cluster and returns the IP of one of them.
     * REQUIRES A PREVIOUSLY-ESTABLISHED SSH CONNECTION TO DCOS-CLI TO WORK
     *
     * @param hosts:  list of IPs that will be investigated
     * @param envVar: environment variable name
     * @throws Exception
     */
    @Given("^I save the IP of an unused node in hosts '(.+?)' in the in environment variable '(.+?)'?$")
    public void getUnusedNode(String hosts, String envVar) throws Exception {
        Set<String> hostList = new HashSet(Arrays.asList(hosts.split(",")));

        //Get the list of currently used hosts
        commonspec.executeCommand("dcos task | awk '{print $2}'", null, 0, null);
        String results = commonspec.getRemoteSSHConnection().getResult();
        Set<String> usedHosts = new HashSet(Arrays.asList(results.replaceAll("\r", "").split("\n")));

        //We get the nodes not being used
        hostList.removeAll(usedHosts);

        if (hostList.size() == 0) {
            throw new IllegalStateException("No unused nodes in the cluster.");
        } else {
            //Pick the first available node
            ThreadProperty.set(envVar, hostList.iterator().next());
        }
    }


    /**
     * Check if all task of a service are correctly distributed in all datacenters of the cluster
     *
     * @param serviceList all task deployed in the cluster separated by a semicolumn.
     * @throws Exception
     */
    @Given("^services '(.*?)' are splitted correctly in datacenters$")
    public void checkServicesDistributionMultiDataCenter(String serviceList) throws Exception {
        commonspec.executeCommand("dcos node --json >> aux.txt", null, 0, null);
        commonspec.executeCommand("cat aux.txt", null, 0, null);
        checkDataCentersDistribution(serviceList.split(","), obtainsDataCenters(commonspec.getRemoteSSHConnection().getResult()).split(";"));
        commonspec.executeCommand("rm -rf aux.txt", null, 0, null);
    }

    /**
     * Check if all task of a service are correctly distributed in all datacenters of the cluster
     *
     * @param serviceList    all task deployed in the cluster separated by a semicolumn.
     * @param dataCentersIps all ips of the datacenters to be checked
     *                       Example: ip_1_dc1, ip_2_dc1;ip_3_dc2,ip_4_dc2
     * @throws Exception
     */
    @Given("^services '(.+?)' are splitted correctly in datacenters '(.+?)'$")
    public void checkServicesDistributionMultiDataCenterPram(String serviceList, String dataCentersIps) throws Exception {
        checkDataCentersDistribution(serviceList.split(","), dataCentersIps.split(";"));
    }

    public void checkDataCentersDistribution(String[] serviceListArray, String[] dataCentersIpsArray) throws Exception {
        int[] results = new int[dataCentersIpsArray.length];
        int div = serviceListArray.length / dataCentersIpsArray.length;
        int resto = serviceListArray.length % dataCentersIpsArray.length;

        for (int i = 0; i < serviceListArray.length; i++) {
            commonspec.executeCommand("dcos task | grep " + serviceListArray[i] + " | awk '{print $2}'", null, 0, null);
            String service_ip = commonspec.getRemoteSSHConnection().getResult();
            for (int x = 0; x < dataCentersIpsArray.length; x++) {
                if (dataCentersIpsArray[x].toLowerCase().contains(service_ip.toLowerCase())) {
                    results[x] = results[x] + 1;
                }
            }
        }

        int sum = 0;
        for (int i = 0; i < results.length; i++) {
            if (resto > 0) {
                assertThat(results[i]).as("Services in datacenter should be: " + div + " or " + (div + 1)).isBetween(div - 1, div + 2);
            } else {
                assertThat(results[i]).as("Services in datacenter should be: " + div + " and it is: " + results[i]).isEqualTo(div);
            }

            sum = sum + results[i];
        }

        assertThat(sum).as("There are less services: " + sum + " than expected: " + serviceListArray.length).isEqualTo(serviceListArray.length);
    }

    public String obtainsDataCenters(String jsonString) {
        Map<String, String> datacentersDistribution = new HashMap<String, String>();
        JSONArray jsonArray = new JSONArray(jsonString);
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject object = jsonArray.getJSONObject(i);
            String ip = object.getString("hostname");
            String datacenter = ((JSONObject) object.get("attributes")).getString("dc");
            String existValue = datacentersDistribution.get(datacenter);
            if (existValue == null) {
                datacentersDistribution.put(datacenter, ip);
            } else {
                datacentersDistribution.put(datacenter, datacentersDistribution.get(datacenter) + "," + ip);
            }
        }
        String result = "";
        for (String ips : datacentersDistribution.keySet()) {
            String key = ips;
            String value = datacentersDistribution.get(key);
            result = result + ";" + value;
        }
        return result.substring(1);
    }

    /**
     * Convert jsonSchema to json
     *
     * @param jsonSchema jsonSchema to be converted to json
     * @param envVar     environment variable where to store json
     * @throws Exception exception     *
     */
    @Given("^I convert jsonSchema '(.+?)' to json( and save it in variable '(.+?)')?( and save it in file '(.+?)')?")
    public void convertJSONSchemaToJSON(String jsonSchema, String envVar, String fileName) throws Exception {
        String json = commonspec.parseJSONSchema(new JSONObject(jsonSchema)).toString();
        if (envVar != null) {
            ThreadProperty.set(envVar, json);
        }
        if (fileName != null) {
            File tempDirectory = new File(System.getProperty("user.dir") + "/target/test-classes/");
            String absolutePathFile = tempDirectory.getAbsolutePath() + "/" + fileName;
            commonspec.getLogger().debug("Creating file {} in 'target/test-classes'", absolutePathFile);
            // Note that this Writer will delete the file if it exists
            Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(absolutePathFile), StandardCharsets.UTF_8));
            try {
                out.write(json);
            } catch (Exception e) {
                commonspec.getLogger().error("Custom file {} hasn't been created:\n{}", absolutePathFile, e.toString());
            } finally {
                out.close();
            }
        }
    }

    /**
     * Check if json is validated against a schema
     *
     * @param json   json to be validated against schema
     * @param schema schema to be validated against
     * @throws Exception exception     *
     */
    @Given("^json (.+?) matches schema (.+?)$")
    public void jsonMatchesSchema(String json, String schema) throws Exception {
        JSONObject jsonschema = new JSONObject(schema);
        JSONObject jsondeploy = new JSONObject(json);

        commonspec.matchJsonToSchema(jsonschema, jsondeploy);
    }

    /**
     * Get service status
     *
     * @param service name of the service to be checked
     * @param cluster URI of the cluster
     * @param envVar  environment variable where to store result
     * @throws Exception exception     *
     */
    @Given("^I get service '(.+?)' status in cluster '(.+?)' and save it in variable '(.+?)'")
    public void getServiceStatus(String service, String cluster, String envVar) throws Exception {
        String status = commonspec.retrieveServiceStatus(service, cluster);

        ThreadProperty.set(envVar, status);
    }

    /**
     * Get service health status
     *
     * @param service name of the service to be checked
     * @param cluster URI of the cluster
     * @param envVar  environment variable where to store result
     * @throws Exception exception     *
     */
    @Given("^I get service '(.+?)' health status in cluster '(.+?)' and save it in variable '(.+?)'")
    public void getServiceHealthStatus(String service, String cluster, String envVar) throws Exception {
        String health = commonspec.retrieveHealthServiceStatus(service, cluster);

        ThreadProperty.set(envVar, health);
    }

    /**
     * Destroy specified service
     *
     * @param service name of the service to be destroyed
     * @param cluster URI of the cluster
     * @throws Exception exception     *
     */
    @Given("^I destroy service '(.+?)' in cluster '(.+?)'")
    public void destroyService(String service, String cluster) throws Exception {
        String endPoint = "/service/deploy-api/deploy/uninstall?app=" + service;
        Future response;

        this.commonspec.setRestProtocol("https://");
        this.commonspec.setRestHost(cluster);
        this.commonspec.setRestPort(":443");

        response = this.commonspec.generateRequest("DELETE", true, null, null, endPoint, null, "json");

        this.commonspec.setResponse("DELETE", (Response) response.get());
        assertThat(this.commonspec.getResponse().getStatusCode()).as("It hasn't been possible to destroy service: " + service).isIn(Arrays.asList(200, 202));
    }

    /**
     * Check if resources are released after uninstall and framework doesn't appear as inactive on mesos
     *
     * @param service service
     * @throws Exception exception
     */
    @When("^All resources from service '(.+?)' have been freed$")
    public void checkResources(String service) throws Exception {
        Future<Response> response = commonspec.generateRequest("GET", true, null, null, "/mesos/state-summary", null, null);

        String json = "[" + response.get().getResponseBody() + "]";
        String parsedElement = "$..frameworks[?(@.active==false)].name";
        String value = commonspec.getJSONPathString(json, parsedElement, null);

        Assertions.assertThat(value).as("Inactive services").doesNotContain(service);
    }

    /**
     * A PUT request over the body value.
     *
     * @param key
     * @param value
     * @param service
     * @throws Exception
     */
    @Then("^I add a new DCOS label with key '(.+?)' and value '(.+?)' to the service '(.+?)'?$")
    public void sendAppendRequest(String key, String value, String service) throws Exception {
        String[] serviceArray = service.split("/");
        String serviceName = serviceArray[serviceArray.length - 1];

        commonspec.runCommandAndGetResult("touch /dcos/" + serviceName + ".json && dcos marathon app show " + service + " > /dcos/" + serviceName + ".json");
        commonspec.runCommandAndGetResult("cat /dcos/" + serviceName + ".json");

        String configFile = commonspec.getRemoteSSHConnection().getResult();
        String myValue = commonspec.getJSONPathString(configFile, ".labels", "0");
        String myJson = commonspec.updateMarathonJson(commonspec.removeJSONPathElement(configFile, "$.labels"));

        String newValue = myValue.replaceFirst("\\{", "{\"" + key + "\": \"" + value + "\", ");
        newValue = "\"labels\":" + newValue;
        String myFinalJson = myJson.replaceFirst("\\{", "{" + newValue.replace("\\n", "\\\\n") + ",");
        if (myFinalJson.contains("uris")) {
            String test = myFinalJson.replaceAll("\"uris\"", "\"none\"");
            commonspec.runCommandAndGetResult("echo '" + test + "' > /dcos/final" + serviceName + ".json");
        } else {
            commonspec.runCommandAndGetResult("echo '" + myFinalJson + "' > /dcos/final" + serviceName + ".json");
        }
        commonspec.runCommandAndGetResult("dcos marathon app update " + service + " < /dcos/final" + serviceName + ".json");

        commonspec.setCommandExitStatus(commonspec.getRemoteSSHConnection().getExitStatus());
    }

    /**
     * Set a environment variable in marathon and deploy again.
     *
     * @param key
     * @param value
     * @param service
     * @throws Exception
     */
    @Then("^I modify marathon environment variable '(.+?)' with value '(.+?)' for service '(.+?)'?$")
    public void setMarathonProperty(String key, String value, String service) throws Exception {
        commonspec.runCommandAndGetResult("touch " + service + "-env.json && dcos marathon app show " + service + " > /dcos/" + service + "-env.json");
        commonspec.runCommandAndGetResult("cat /dcos/" + service + "-env.json");

        String configFile = commonspec.getRemoteSSHConnection().getResult();
        String myJson1 = commonspec.replaceJSONPathElement(configFile, key, value);
        String myJson4 = commonspec.updateMarathonJson(myJson1);
        String myJson = myJson4.replaceAll("\"uris\"", "\"none\"");

        commonspec.runCommandAndGetResult("echo '" + myJson + "' > /dcos/final" + service + "-env.json");
        commonspec.runCommandAndGetResult("dcos marathon app update " + service + " < /dcos/final" + service + "-env.json");
        commonspec.setCommandExitStatus(commonspec.getRemoteSSHConnection().getExitStatus());
    }

    /**
     * Check service status has value specified
     *
     * @param service name of the service to be checked
     * @param cluster URI of the cluster
     * @param status  status expected
     * @throws Exception exception     *
     */
    @Then("^service '(.+?)' status in cluster '(.+?)' is '(suspended|running|deploying)'( in less than '(\\d+)')?( seconds checking every '(\\d+)' seconds)?")
    public void serviceStatusCheck(String service, String cluster, String status, String sTotalWait, String sInterval) throws Exception {
        Integer totalWait = sTotalWait != null ? Integer.parseInt(sTotalWait) : null;
        Integer interval = sInterval != null ? Integer.parseInt(sInterval) : null;
        String response;
        Integer i = 0;
        boolean matched;

        response = commonspec.retrieveServiceStatus(service, cluster);

        if (totalWait != null && interval != null) {
            matched = status.matches(response);
            while (!matched && i < totalWait) {
                this.commonspec.getLogger().info("Service status not found yet after " + i + " seconds");
                i = i + interval;
                response = commonspec.retrieveServiceStatus(service, cluster);
                matched = status.matches(response);
            }
        }

        assertThat(status).as("Expected status: " + status + " doesn't match obtained one: " + response).matches(response);
    }

    /**
     * Check service health status has value specified
     *
     * @param service name of the service to be checked
     * @param cluster URI of the cluster
     * @param status  health status expected
     * @throws Exception exception     *
     */
    @Then("^service '(.+?)' health status in cluster '(.+?)' is '(unhealthy|healthy|unknown)'( in less than '(\\d+)')?( seconds checking every '(\\d+)' seconds)?")
    public void serviceHealthStatusCheck(String service, String cluster, String status, String sTotalWait, String sInterval) throws Exception {
        Integer totalWait = sTotalWait != null ? Integer.parseInt(sTotalWait) : null;
        Integer interval = sInterval != null ? Integer.parseInt(sInterval) : null;
        String response;
        Integer i = 0;
        boolean matched;

        response = commonspec.retrieveHealthServiceStatus(service, cluster);

        if (totalWait != null && interval != null) {
            matched = status.matches(response);
            while (!matched && i < totalWait) {
                this.commonspec.getLogger().info("Service health status not found yet after " + i + " seconds");
                i = i + interval;
                response = commonspec.retrieveHealthServiceStatus(service, cluster);
                matched = status.matches(response);
            }
        }

        assertThat(status).as("Expected status: " + status + " doesn't match obtained one: " + response).matches(response);
    }

    @Then("^I obtain metabase id for user '(.+?)' and password '(.+?)' in endpoint '(.+?)' and save in context cookies$")
    public void saveMetabaseCookie(String user, String password, String url) throws Exception {
        String command = "curl -X POST -k -H \"Content-Type: application/json\" -d '{\"username\": \"" + user + "\", \"password\": \"" + password + "\"}' " + url;
        commonspec.runLocalCommand(command);
        commonspec.runCommandLoggerAndEnvVar(0, null, Boolean.TRUE);

        Assertions.assertThat(commonspec.getCommandExitStatus()).isEqualTo(0);
        String result = JsonPath.parse(commonspec.getCommandResult().trim()).read("$.id");

        com.ning.http.client.cookie.Cookie cookie = new com.ning.http.client.cookie.Cookie("metabase.SESSION_ID", result, false, "", "", 99999L, false, false);
        ArrayList cookieList = new ArrayList();
        cookieList.add(cookie);
        this.commonspec.setCookies(cookieList);
    }


    /**
     * Check if a role of a service complies the established constraints
     *
     * @param role        name of role of a service or scheduler
     * @param service     name of service of exhibitor
     * @param instance    name of instance of a service, for scheduler is id
     * @param constraints all stablished contraints separated by a semicolumn.
     *                    Example: constraint1,constraint2,...
     * @throws Exception
     */
    @Then("^The role '(.+?)' of the service '(.+?)' with instance '(.+?)' complies the constraints '(.+?)'$")
    public void checkComponentConstraints(String role, String service, String instance, String constraints) throws Exception {
        checkComponentConstraint(role, service, instance, constraints.split(","));
    }

    public void checkComponentConstraint(String role, String service, String instance, String[] constraints) throws Exception {
        for (int i = 0; i < constraints.length; i++) {
            String[] elements = constraints[i].split(":");
            Assertions.assertThat(elements.length).overridingErrorMessage("Error while parsing constraints. The constraint's format is ATRIBUTE:CONSTRAINT:VALOR or ATRIBUTE:CONSTRAINT").isIn(2, 3);
            Assertions.assertThat(elements[1]).overridingErrorMessage("Error while parsing constraints. Constraints should be CLUSTER, UNIQUE, LIKE, UNLIKE, GROUP_BY, MAX_PER or IS").isIn("UNIQUE", "CLUSTER", "GROUP_BY", "LIKE", "UNLIKE", "MAX_PER", "IS");
            if (elements.length == 2) {
                Assertions.assertThat(elements[1]).overridingErrorMessage("Error while parsing constraints. The constraint's format " + elements[1] + " is ATRIBUTE:CONSTRAINT:VALOR").isIn("UNIQUE", "CLUSTER", "GROUP_BY");
                if (!elements[1].equals("GROUP_BY")) {
                    checkConstraint(role, service, instance, elements[0], elements[1], null);
                }
            } else {
                Assertions.assertThat(elements[1]).overridingErrorMessage("Error while parsing constraints. The constraint's format " + elements[1] + " is ATRIBUTE:CONSTRAINT").isNotEqualTo("UNIQUE");
                checkConstraint(role, service, instance, elements[0], elements[1], elements[2]);
            }
        }
    }

    public void checkConstraint(String role, String service, String instance, String tag, String constraint, String value) throws Exception {
        RestSpec restspec = new RestSpec(commonspec);
        MiscSpec miscspec = new MiscSpec(commonspec);
        if (role.equals("scheduler")) {
            restspec.sendRequestTimeout(100, 5, "GET", "/service/marathon/v2/apps" + instance, null, "app");
            miscspec.saveElementEnvironment(null, "$.app.tasks[0]", "marathon_answer");
            Assertions.assertThat(ThreadProperty.get("marathon_answer")).overridingErrorMessage("Error while parsing constraints. The instance " + instance + " of the service " + service + " isn't deployed").isNotEmpty();
        } else {
            restspec.sendRequestTimeout(100, 5, "GET", "/exhibitor/exhibitor/v1/explorer/node-data?key=/datastore/" + service + "/" + instance + "/plan-v2-json&_=", null, "str");
            miscspec.saveElementEnvironment(null, "$.str", "exhibitor_answer");
            Assertions.assertThat(ThreadProperty.get("exhibitor_answer")).overridingErrorMessage("Error while parsing constraints. The instance " + instance + " of the service " + service + " isn't deployed").isNotEmpty();
        }
        CommandExecutionSpec commandexecutionspec = new CommandExecutionSpec(commonspec);
        if (tag.equals("hostname")) {
            if (role.equals("scheduler")) {
                commandexecutionspec.executeLocalCommand("echo '" + ThreadProperty.get("marathon_answer") + "' | jq .host | sed 's/\"//g'", "0", "elementsConstraint");
            } else {
                selectElements(role, service, "agent_hostname");
            }
            String[] hostnames = ThreadProperty.get("elementsConstraint").split("\n");
            checkConstraintType(role, instance, tag, constraint, value, hostnames);
        } else {
            if (role.equals("scheduler")) {
                commandexecutionspec.executeLocalCommand("echo '" + ThreadProperty.get("marathon_answer") + "' | jq .slaveId | sed 's/\"//g'", "0", "elementsConstraint");
            } else {
                selectElements(role, service, "slaveid");
            }
            String[] slavesid = ThreadProperty.get("elementsConstraint").split("\n");
            String[] valor = new String[slavesid.length];
            for (int i = 0; i < slavesid.length; i++) {
                restspec.sendRequestTimeout(100, 5, "GET", "/mesos/slaves?slave_id=" + slavesid[i], null, "slaves");
                miscspec.saveElementEnvironment(null, "$", "mesos_answer");
                commandexecutionspec.executeLocalCommand("echo '" + ThreadProperty.get("mesos_answer") + "' | jq '.slaves[0].attributes." + tag + "' | sed 's/^.\\|.$//g'", "0", "valortag");
                valor[i] = ThreadProperty.get("valortag");
            }
            checkConstraintType(role, instance, tag, constraint, value, valor);
        }
    }

    /**
     * @param role   name of role of a service
     * @param IP     Ip of the machine from which you want to save the nodes
     * @param envVar environment variable where store nodes
     * @throws Exception
     */
    @When("^I save nodes '(.+?)' that are in machine '(.+?)' in environment variable '(.+?)'$")
    public void saveNodes(String role, String IP, String envVar) throws Exception {
        selectElements(role, "pbd", "agent_hostname", IP, envVar);
    }

    /**
     * @param role        name of role of a service
     * @param envVar      environment variable where before you save nodes
     * @param envVar2     environment variable when you want to check slave nodes
     * @param sTimeout    Same RestSpec.sendRequest
     * @param sWait       Same RestSpec.sendRequest
     * @param requestType Same RestSpec.sendRequest
     * @param endPoint    Same RestSpec.sendRequest
     * @param status      Same RestSpec.sendRequest
     * @throws Exception
     */
    @Then("^I check status of nodes '(.+?)' using environment variable '(.+?)'(,'(.+?)')? in less than '(\\d+?)' seconds, checking each '(\\d+?)' seconds, I send a '(.+?)' request to '(.+?)' checking status '(.+?)' of nodes$")
    public void checkProxyNodesStatus(String role, String envVar, String envVar2, String sTimeout, String sWait, String requestType, String endPoint, String status) throws Exception {
        Integer timeout = Integer.parseInt(sTimeout);
        Integer wait = Integer.parseInt(sWait);
        String estadoNodo;

        RestSpec restspec = new RestSpec(commonspec);

        //datanodes
        selectElements(role, "pbd", "name");
        String[] dataNodes = ThreadProperty.get("elementsConstraint").split("\n");

        //Si tenemos algún DataNode caído, chequeamos los datanodeSlave de ese dataNode
        if (role.contains("datanode_slave") && envVar2 != null) {

            for (int i = 0; i < dataNodes.length; i++) {
                if (dataNodes[i].split("_")[1].contains(ThreadProperty.get(envVar2).split("_")[1])) {
                    estadoNodo = status;
                } else {
                    estadoNodo = "RUNNING";
                }
                restspec.sendRequestTimeout(timeout, wait, requestType, endPoint, null, dataNodes[i] + "\",\"role\":\"" + role + "\",\"status\":\"" + estadoNodo);
            }

        } else {
            for (int i = 0; i < dataNodes.length; i++) {
                if (dataNodes[i].contains(ThreadProperty.get(envVar)) && !ThreadProperty.get(envVar).isEmpty()) {
                    estadoNodo = status;
                } else {
                    estadoNodo = "RUNNING";
                }
                restspec.sendRequestTimeout(timeout, wait, requestType, endPoint, null, dataNodes[i] + "\",\"role\":\"" + role + "\",\"status\":\"" + estadoNodo);
            }
        }
    }

    public void selectElements(String role, String service, String element, String elementValue, String envValue) throws Exception {
        CommandExecutionSpec commandexecutionspec = new CommandExecutionSpec(commonspec);
        commandexecutionspec.executeLocalCommand("echo '" + ThreadProperty.get("exhibitor_answer") + "' | jq '.phases[] | .[] | .steps[] | .[] | select(.role==\"" + role + "\") | select(.status==\"RUNNING\") | select(." + element + "==\"" + elementValue + "\").name' | sed 's/\"//g'", "0", envValue);
    }


    public void selectElements(String role, String service, String element) throws Exception {
        CommandExecutionSpec commandexecutionspec = new CommandExecutionSpec(commonspec);
        commandexecutionspec.executeLocalCommand("echo '" + ThreadProperty.get("exhibitor_answer") + "' | jq '.phases[] | .[] | .steps[] | .[] | select(.role==\"" + role + "\") | select(.status==\"RUNNING\")." + element + "' | sed 's/\"//g'", "0", "elementsConstraint");
    }

    public void checkConstraintType(String role, String instance, String tag, String constraint, String value, String[] elements) throws Exception {
        Pattern p = value != null ? Pattern.compile(value) : null;
        Matcher m;
        switch (constraint) {
            case "UNIQUE":
                for (int i = 0; i < elements.length; i++) {
                    for (int j = i + 1; j < elements.length; j++) {
                        Assertions.assertThat(elements[i]).overridingErrorMessage("The role " + role + " of the instance " + instance + " doesn't complies the established constraint " + tag + ":" + constraint).isNotEqualTo(elements[j]);
                    }
                }
                break;
            case "CLUSTER":
                if (value == null) {
                    for (int i = 0; i < elements.length; i++) {
                        for (int j = i + 1; j < elements.length; j++) {
                            Assertions.assertThat(elements[i]).overridingErrorMessage("The role " + role + " of the instance " + instance + " doesn't complies the established constraint " + tag + ":" + constraint).isEqualTo(elements[j]);
                        }
                    }
                } else {
                    checkConstraintClusterValueIs(role, instance, tag, constraint, value, elements);
                }
                break;
            case "LIKE":
                for (int i = 0; i < elements.length; i++) {
                    m = p.matcher(elements[i]);
                    Assertions.assertThat(m.find()).overridingErrorMessage("The role " + role + " of the instance " + instance + " doesn't complies the established constraint " + tag + ":" + constraint + ":" + value).isEqualTo(true);
                }
                break;
            case "UNLIKE":
                for (int i = 0; i < elements.length; i++) {
                    m = p.matcher(elements[i]);
                    Assertions.assertThat(m.find()).overridingErrorMessage("The role " + role + " of the instance " + instance + " doesn't complies the established constraint " + tag + ":" + constraint + ":" + value).isEqualTo(false);
                }
                break;
            case "IS":
                checkConstraintClusterValueIs(role, instance, tag, constraint, value, elements);
                break;
            case "MAX_PER":
                Map<String, Integer> diferent = new HashMap<String, Integer>();
                int count;
                for (int i = 0; i < elements.length; i++) {
                    if (!diferent.containsKey(elements[i])) {
                        diferent.put(elements[i], 1);
                    } else {
                        count = diferent.get(elements[i]);
                        count = count + 1;
                        diferent.put(elements[i], count);
                    }
                }
                Iterator it = diferent.keySet().iterator();
                while (it.hasNext()) {
                    Assertions.assertThat(diferent.get(it.next())).overridingErrorMessage("The role " + role + " of the instance " + instance + " doesn't complies the established constraint " + tag + ":" + constraint + ":" + value).isLessThanOrEqualTo(Integer.parseInt(value));
                }
                break;
            case "GROUP_BY":
                ArrayList<String> dif = new ArrayList<>();
                dif.add(elements[0]);
                boolean ok;
                for (int i = 1; i < elements.length; i++) {
                    ok = false;
                    for (int j = 0; j < dif.size(); j++) {
                        if (elements[i].equals(dif.get(j))) {
                            ok = true;
                            j = dif.size();
                        }
                    }
                    if (!ok) {
                        dif.add(elements[i]);
                    }
                }
                Assertions.assertThat(dif.size()).overridingErrorMessage("The role " + role + " of the instance " + instance + " doesn't complies the established constraint " + tag + ":" + constraint + ":" + value).isLessThanOrEqualTo(Integer.parseInt(value));
                break;
            default:
                commonspec.getExceptions().add(new Exception("Error while parsing constraints. Constraints should be CLUSTER, UNIQUE, LIKE, UNLIKE, GROUP_BY, UNIQUE, LIKE, UNLIKE, GROUP_BY, MAX_PER or IS"));
        }
    }

    public void checkConstraintClusterValueIs(String role, String instance, String tag, String constraint, String value, String[] elements) throws Exception {
        for (int i = 0; i < elements.length; i++) {
            for (int j = i + 1; j < elements.length; j++) {
                Assertions.assertThat(elements[i]).overridingErrorMessage("The role " + role + " of the instance " + instance + " doesn't complies the established constraint " + tag + ":" + constraint + ":" + value).isEqualTo(elements[j]);
                Assertions.assertThat(elements[i]).overridingErrorMessage("The role " + role + " of the instance " + instance + " doesn't complies the established constraint " + tag + ":" + constraint + ":" + value).isEqualTo(value);
            }
        }
    }

    /**
     * Obtains info from file passed as a parameter based on the jq expression passed as a parameter
     *
     * @param jqExpression    jq expression to retrieve required information
     * @param absolutPathFile absolut path to file to retrieve information from
     * @param envVar          environment variable where to store retrieved information
     * @throws Exception
     */
    @Given("^I obtain '(.+?)' from json file '(.+?)' and save it in environment variable '(.+?)'$")
    public void obtainInfoFromFile(String jqExpression, String absolutPathFile, String envVar) throws Exception {
        String prefix = "set -o pipefail && cat ";

        try {
            commonspec.getRemoteSSHConnection().runCommand("test -f " + absolutPathFile);
        } catch (Exception e) {
            commonspec.getLogger().debug("SSH connection not opened.");
            throw new Exception("SSH connection not opened.");
        }

        if (commonspec.getRemoteSSHConnection().getExitStatus() != 0) {
            commonspec.getLogger().debug("File passed as parameter: " + absolutPathFile + ", does not exist in remote system");
            throw new Exception("File: " + absolutPathFile + " does not exist in remote system.");
        }

        commonspec.getRemoteSSHConnection().runCommand(prefix + absolutPathFile + " | " + jqExpression);
        if (commonspec.getRemoteSSHConnection().getExitStatus() != 0) {
            commonspec.getLogger().debug("Problem with jq expression passed as parameter.");
            throw new Exception("Error obtaining info from json file: " + commonspec.getRemoteSSHConnection().getResult());
        }

        if (commonspec.getRemoteSSHConnection().getResult().equals("null")) {
            commonspec.getLogger().debug("jq expression passed as parameter returns null.");
            throw new Exception("Info obtained from json file with jq: " + jqExpression + ", is null.");
        }

        ThreadProperty.set(envVar, commonspec.getRemoteSSHConnection().getResult());
    }

    /**
     * Obtains required information from descriptor file
     *
     * @param info   information required from descriptor.json in bootstrap system
     * @param envVar environment variable where to store retrieved information
     * @throws Exception
     */
    @Given("^I obtain '(MASTERS|NODES|PRIV_NODES|PUBLIC_NODES|PUBLIC_NODE|GOSEC_NODES|ID|DNS_SEARCH|INTERNAL_DOMAIN|ARTIFACT_REPO|DOCKER_REGISTRY|EXTERNAL_DOCKER_REGISTRY|REALM|KRB_HOST|LDAP_HOST|VAULT_HOST|IP|ADMIN_USER|TENANT|ACCESS_POINT|LDAP_URL|LDAP_PORT|LDAP_USER_DN|LDAP_GROUP_DN|LDAP_BASE|LDAP_ADMIN_GROUP)' from descriptor and save it in environment variable '(.+?)'$")
    public void obtainInfoFromDescriptor(String info, String envVar) throws Exception {
        String jqExpression = "";

        switch (info) {
            case "MASTERS":
                jqExpression = "jq -crM '.nodes[] | select(.role ?== \"master\") | .networking[0].ip' | paste -sd \",\" -";
                break;
            case "NODES":
                jqExpression = "jq -crM '.nodes[] | select(.role ?== \"agent\") | .networking[0].ip' | paste -sd \",\" -";
                break;
            case "PRIV_NODES":
                jqExpression = "jq -crM '.nodes[] | select((.role ?== \"agent\") and .public ?== false) | .networking[0].ip' | paste -sd \",\" -";
                break;
            case "PUBLIC_NODES":
                jqExpression = "jq -crM '.nodes[] | select((.role ?== \"agent\") and .public ?== true) | .networking[0].ip' | paste -sd \",\" -";
                break;
            case "PUBLIC_NODE":
                jqExpression = "jq -cM '.nodes[] | select((.role ?== \"agent\") and .public ?== true) | .networking[0].ip' | jq -crMs '.[0]'";
                break;
            case "GOSEC_NODES":
                jqExpression = "jq -crM '.nodes[] | select(.role ?== \"gosec\") | .networking[0].ip' | paste -sd \",\" -";
                break;
            case "ID":
                jqExpression = "jq -crM .id";
                break;
            case "DNS_SEARCH":
                jqExpression = "jq -crM .dnsSearch";
                break;
            case "INTERNAL_DOMAIN":
                jqExpression = "jq -crM .internalDomain";
                break;
            case "ARTIFACT_REPO":
                jqExpression = "jq -crM .artifactRepository";
                break;
            case "DOCKER_REGISTRY":
                jqExpression = "jq -crM .dockerRegistry";
                break;
            case "EXTERNAL_DOCKER_REGISTRY":
                jqExpression = "jq -crM .externalDockerRegistry";
                break;
            case "REALM":
                jqExpression = "jq -crM .security.kerberos.realm";
                break;
            case "KRB_HOST":
                jqExpression = "jq -crM .security.kerberos.kdcHost";
                break;
            case "LDAP_HOST":
                jqExpression = "jq -crM .security.ldap.url";
                break;
            case "VAULT_HOST":
                jqExpression = "jq -crM '.nodes[] | select(.role ?== \"gosec\") | .networking[0].ip' | paste -sd \",\" - | cut -d, -f1";
                break;
            case "IP":
                jqExpression = "jq -crM '.nodes[] | select(.role ?== \"master\") | .networking[0].ip' | paste -sd \",\" - | cut -d, -f1";
                break;
            case "ADMIN_USER":
                jqExpression = "jq -crM .security.ldap.adminUserUuid";
                break;
            case "TENANT":
                jqExpression = "jq -crM .security.tenantSSODefault | sed 's/null/NONE/g'";
                break;
            case "ACCESS_POINT":
                jqExpression = "jq -crM .proxyAccessPointURL | sed 's/https:\\/\\///g'";
                break;
            case "LDAP_URL":
                jqExpression = "jq -crM .security.ldap.url";
                break;
            case "LDAP_PORT":
                jqExpression = "jq -crM .security.ldap.port";
                break;
            case "LDAP_USER_DN":
                jqExpression = "jq -crM .security.ldap.userDn";
                break;
            case "LDAP_GROUP_DN":
                jqExpression = "jq -crM .security.ldap.groupDN";
                break;
            case "LDAP_BASE":
                jqExpression = "jq -crM .security.ldap.ldapBase";
                break;
            case "LDAP_ADMIN_GROUP":
                jqExpression = "jq -crM .security.ldap.adminrouterAuthorizedGroup";
                break;
            default:
                break;
        }

        obtainInfoFromFile(jqExpression, this.descriptorPath, envVar);

    }

    public void obtainBasicInfoFromETCD() throws Exception {
        String localVaultResponseFile;
        String localVaultResponseFilePath;
        String localCaTrustFilePath;

        String bootstrap_ip = System.getProperty("BOOTSTRAP_IP");
        String bootstrap_user;
        String bootstrap_pem;

        // Check if needed parameters have been passed
        if (bootstrap_ip == null) {
            throw new Exception("BOOTSTRAP_IP variable needs to be provided in order to obtain information from system.");
        } else {
            bootstrap_user = System.getProperty("REMOTE_USER");
            if (bootstrap_user == null) {
                throw new Exception("REMOTE_USER variable needs to be provided in order to obtain information from system.");
            } else {
                bootstrap_pem = (System.getProperty("PEM_FILE_PATH"));
                if (bootstrap_pem == null) {
                    throw new Exception("PEM_FILE_PATH variable needs to be provided in order to obtain information from system.");
                }
            }

            localVaultResponseFile = "vault_response_" + bootstrap_ip;
            localVaultResponseFilePath = "./target/test-classes/" + localVaultResponseFile;
            localCaTrustFilePath = "./target/test-classes/ca_test.crt";
        }

        // Open connection to bootstrap
        commonspec.getLogger().debug("Openning connection to bootstrap to obtain descriptor file: " + descriptorPath);
        commonspec.setRemoteSSHConnection(new RemoteSSHConnection(bootstrap_user, null, bootstrap_ip, "22", bootstrap_pem), "bootstrap_connection");

        // Make local copy of vault response file
        commonspec.getLogger().debug("Copying vault_response file to: " + localVaultResponseFilePath);
        commonspec.getRemoteSSHConnection().copyFrom(vaultResponsePath, localVaultResponseFilePath);

        // Make local copy of CA
        String caTrust = (System.getProperty("EOS_CA_TRUST") != null) ? System.getProperty("EOS_CA_TRUST") : "/stratio_volume/cas_trusted/ca.crt";
        commonspec.getLogger().debug("Copying ca_trust file to: " + localCaTrustFilePath);
        commonspec.getRemoteSSHConnection().copyFrom(caTrust, localCaTrustFilePath);

        // Close connection to bootstrap system
        commonspec.getRemoteSSHConnection().closeConnection();
        RemoteSSHConnectionsUtil.getRemoteSSHConnectionsMap().remove("bootstrap_connection");
        RemoteSSHConnectionsUtil.setLastRemoteSSHConnectionId(null);
        RemoteSSHConnectionsUtil.setLastRemoteSSHConnection(null);

        // Save content of vault response file in memory to speed up process
        String response = commonspec.retrieveData(localVaultResponseFile, "json");

        if (ThreadProperty.get("configuration_api_id") == null) {
            fail("configuration_api_id variable is not set. Check configuratio-api is installed and @dcos annotation is working properly.");
        }

        // Set sso token
        setGoSecSSOCookie(null, null, ThreadProperty.get("EOS_ACCESS_POINT"), ThreadProperty.get("DCOS_USER"), System.getProperty("DCOS_PASSWORD"), ThreadProperty.get("DCOS_TENANT"), null);
        // Securely send requests
        commonspec.setRestProtocol("https://");
        commonspec.setRestHost(ThreadProperty.get("EOS_ACCESS_POINT"));
        commonspec.setRestPort(":443");

        // Obtain configuration-api endpoint
        String path = "/dcs/v1/fabric";
        String endpoint = "/service/" + ThreadProperty.get("configuration_api_id") + "/etcd?path=" + path;

        Future<Response> responseETCD = commonspec.generateRequest("GET", false, null, null, endpoint, "", null);
        commonspec.setResponse("GET", responseETCD.get());

        if (commonspec.getResponse().getStatusCode() != 200) {
            logger.error("Obtain info from ETCD: " + endpoint + " failed with status code: " + commonspec.getResponse().getStatusCode() + " and response: " + commonspec.getResponse().getResponse());
            throw new Exception("Obtain info from ETCD: " + endpoint + " failed with status code: " + commonspec.getResponse().getStatusCode() + " and response: " + commonspec.getResponse().getResponse());
        }

        String etcdInfo = commonspec.getResponse().getResponse();

        obtainJSONInfoAndExpose(etcdInfo, "$.eos.internalDomain", "EOS_INTERNAL_DOMAIN", null);
        obtainJSONInfoAndExpose(etcdInfo, "$.eos.dockerRegistry", "DOCKER_REGISTRY", null);
        obtainJSONInfoAndExpose(etcdInfo, "$.eos.externalDockerRegistry", "EXTERNAL_DOCKER_REGISTRY", null);
        obtainJSONInfoAndExpose(etcdInfo, "$.eos.artifactRepository", "ARTIFACT_REPOSITORY", null);
        obtainJSONInfoAndExpose(etcdInfo, "$.eos.proxyAccessPointURL", "EOS_ACCESS_POINT", null);
        obtainJSONInfoAndExpose(etcdInfo, "$.globals.sso.ssoTenantDefault", "DCOS_TENANT", null);
        obtainJSONInfoAndExpose(etcdInfo, "$.globals.kerberos.realm", "EOS_REALM", null);
        obtainJSONInfoAndExpose(etcdInfo, "$.globals.ldap.adminUserUuid", "DCOS_USER", null);
        obtainJSONInfoAndExpose(etcdInfo, "$.globals.ldap.url", "LDAP_URL", null);
        obtainJSONInfoAndExpose(etcdInfo, "$.globals.ldap.port", "LDAP_PORT", null);
        obtainJSONInfoAndExpose(etcdInfo, "$.globals.ldap.userDn", "LDAP_USER_DN", null);
        obtainJSONInfoAndExpose(etcdInfo, "$.globals.ldap.groupDN", "LDAP_GROUP_DN", null);
        obtainJSONInfoAndExpose(etcdInfo, "$.globals.ldap.ldapBase", "LDAP_BASE", null);
        obtainJSONInfoAndExpose(etcdInfo, "$.globals.ldap.adminrouterAuthorizedGroup", "LDAP_ADMIN_GROUP", null);
        obtainJSONInfoAndExpose(etcdInfo, "$.globals.vault.vaultHost", "EOS_VAULT_HOST_INTERNAL", null);

        String[] schemaVersion = ThreadProperty.get("EOS_SCHEMA_VERSION").split("\\.");
        if (Integer.parseInt(schemaVersion[0]) > 0 && Integer.parseInt(schemaVersion[1]) > 3) {
            obtainJSONInfoAndExpose(etcdInfo, "$.cluster_info.descriptor.id", "EOS_CLUSTER_ID", null);
            obtainJSONInfoAndExpose(etcdInfo, "$.cluster_info.descriptor.dnsSearch", "EOS_DNS_SEARCH", null);
            obtainJSONInfoAndExpose(etcdInfo, "$.cluster_info.descriptor.nodes[?(@.role == \"master\")].networking[0].ip", "DCOS_IP", "0");
            obtainJSONInfoAndExpose(etcdInfo, "$.cluster_info.descriptor.nodes[?(@.role == \"agent\" && @.public == true)].networking[0].ip", "PUBLIC_NODE", "0");
            obtainJSONInfoAndExpose(etcdInfo, "$.cluster_info.descriptor.nodes[?(@.role == \"gosec\")].networking[0].ip", "EOS_VAULT_HOST", "0");
            obtainJSONInfoAndExpose(etcdInfo, "$.globals.overlayNetwork.addressPool", "ADDRESS_POOL", null);
        }

        obtainJSONInfo(response, "ROOT_TOKEN", "VAULT_TOKEN");

    }

    public void obtainJSONInfoAndExpose(String json, String jqExpression, String envVar, String position) {
        String value = "";
        try {
            value = commonspec.getJSONPathString(json, jqExpression, position).replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\"", "");
        } catch (Exception e) {
            if (envVar == "PUBLIC_NODE" || envVar == "DCOS_TENANT") {
                value = null;
            } else {
                throw e;
            }
        }

        switch (envVar) {
            case "DCOS_TENANT":
                if (value == null) {
                    value = "NONE";
                }
                break;
            case "EOS_ACCESS_POINT":
                value = value.replaceAll("https://", "");
                break;
            default:
                break;
        }

        if (!(envVar == "PUBLIC_NODE" && value == null)) {
            ThreadProperty.set(envVar, value);
        }
    }


    /**
     * Obtains basic information for tests from descriptor file:
     * EOS_CLUSTER_ID, EOS_DNS_SEARCH, EOS_INTERNAL_DOMAIN, DCOS_IP, DCOS_USER, DCOS_TENANT, VAULT_TOKEN
     * LDAP_URL, LDAP_PORT, LDAP_USER_DN, LDAP_GROUP_DN, LDAP_BASE, LDAP_ADMIN_GROUP
     *
     * @throws Exception
     */
    @Given("^I( force to)? obtain basic information from bootstrap$")
    public void obtainBasicInfoFromDescriptor(String force) throws Exception {
        String localDescriptorFile;
        String localDescriptorFilePath;
        String localVaultResponseFile;
        String localVaultResponseFilePath;
        String localCaTrustFilePath;

        // General values
        String varClusterID = "EOS_CLUSTER_ID";
        String varSchemaVersion = "EOS_SCHEMA_VERSION";
        String varClusterDomain = "EOS_DNS_SEARCH";
        String varInternalDomain = "EOS_INTERNAL_DOMAIN";
        String varIp = "DCOS_IP";
        String varAdminUser = "DCOS_USER";
        String varTenant = "DCOS_TENANT";
        String varVaultHost = "EOS_VAULT_HOST";
        String varVaultToken = "VAULT_TOKEN";
        String varPublicNode = "PUBLIC_NODE";
        String varAccessPoint = "EOS_ACCESS_POINT";
        String varRealm = "EOS_REALM";
        String varAddressPool = "ADDRESS_POOL";
        String varDockerRegistry = "DOCKER_REGISTRY";
        String varExternalDockerRegistry = "EXTERNAL_DOCKER_REGISTRY";
        String varArtifactRepository = "ARTIFACT_REPOSITORY";

        // LDAP values
        String varLDAPurl = "LDAP_URL";
        String varLDAPport = "LDAP_PORT";
        String varLDAPuserDn = "LDAP_USER_DN";
        String varLDAPgroupDn = "LDAP_GROUP_DN";
        String varLDAPbase = "LDAP_BASE";
        String varLDAPadminGroup = "LDAP_ADMIN_GROUP";

        String bootstrap_ip = System.getProperty("BOOTSTRAP_IP");
        String bootstrap_user;
        String bootstrap_pem;

        // Check if needed parameters have been passed
        if (bootstrap_ip == null) {
            throw new Exception("BOOTSTRAP_IP variable needs to be provided in order to obtain information from system.");
        } else {
            bootstrap_user = System.getProperty("REMOTE_USER");
            if (bootstrap_user == null) {
                throw new Exception("REMOTE_USER variable needs to be provided in order to obtain information from system.");
            } else {
                bootstrap_pem = (System.getProperty("PEM_FILE_PATH"));
                if (bootstrap_pem == null) {
                    throw new Exception("PEM_FILE_PATH variable needs to be provided in order to obtain information from system.");
                }
            }

            localDescriptorFile = "descriptor_" + bootstrap_ip + ".json";
            localDescriptorFilePath = "./target/test-classes/" + localDescriptorFile;
            localVaultResponseFile = "vault_response_" + bootstrap_ip;
            localVaultResponseFilePath = "./target/test-classes/" + localVaultResponseFile;
            localCaTrustFilePath = "./target/test-classes/ca_test.crt";
        }

        String[] vars = {varClusterID, varClusterDomain, varInternalDomain, varIp, varAdminUser, varTenant, varVaultHost, varVaultToken, varPublicNode, varAccessPoint, varRealm, varAddressPool, varLDAPurl, varLDAPport, varLDAPuserDn, varLDAPgroupDn, varLDAPbase, varLDAPadminGroup};
        boolean bootstrapInfoObtained = true;

        // Check if info have been retrieved previously
        if (force != null) {
            bootstrapInfoObtained = false;
        } else {
            for (String var : vars) {
                if (ThreadProperty.get(var) == null) {
                    bootstrapInfoObtained = false;
                    break;
                }
            }
        }

        if (!bootstrapInfoObtained) {
            commonspec.getLogger().debug("Openning connection to bootstrap to obtain descriptor file: " + descriptorPath);
            commonspec.setRemoteSSHConnection(new RemoteSSHConnection(bootstrap_user, null, bootstrap_ip, "22", bootstrap_pem), "bootstrap_connection");

            commonspec.getLogger().debug("Copying descriptor file to: " + localDescriptorFilePath);
            commonspec.getRemoteSSHConnection().copyFrom(descriptorPath, localDescriptorFilePath);

            commonspec.getLogger().debug("Copying vault_response file to: " + localVaultResponseFilePath);
            commonspec.getRemoteSSHConnection().copyFrom(vaultResponsePath, localVaultResponseFilePath);

            String caTrust = (System.getProperty("EOS_CA_TRUST") != null) ? System.getProperty("EOS_CA_TRUST") : "/stratio_volume/cas_trusted/ca.crt";
            commonspec.getLogger().debug("Copying ca_trust file to: " + localCaTrustFilePath);
            commonspec.getRemoteSSHConnection().copyFrom(caTrust, localCaTrustFilePath);

            // Close connection to bootstrap system
            commonspec.getRemoteSSHConnection().closeConnection();
            RemoteSSHConnectionsUtil.getRemoteSSHConnectionsMap().remove("bootstrap_connection");
            RemoteSSHConnectionsUtil.setLastRemoteSSHConnectionId(null);
            RemoteSSHConnectionsUtil.setLastRemoteSSHConnection(null);

            // Save content of files in memory to speed up process
            String descriptor = commonspec.retrieveData(localDescriptorFile, "json");
            String response = commonspec.retrieveData(localVaultResponseFile, "json");

            obtainJSONInfo(descriptor, "ID", varClusterID);
            obtainJSONInfo(descriptor, "SCHEMA_VERSION", varSchemaVersion);
            obtainJSONInfo(descriptor, "DNS_SEARCH", varClusterDomain);
            obtainJSONInfo(descriptor, "INTERNAL_DOMAIN", varInternalDomain);
            obtainJSONInfo(descriptor, "IP", varIp);
            obtainJSONInfo(descriptor, "ADMIN_USER", varAdminUser);
            obtainJSONInfo(descriptor, "TENANT", varTenant);
            obtainJSONInfo(descriptor, "VAULT_HOST", varVaultHost);
            obtainJSONInfo(descriptor, "REALM", varRealm);
            obtainJSONInfo(descriptor, "ACCESS_POINT", varAccessPoint);
            obtainJSONInfo(descriptor, "LDAP_URL", varLDAPurl);
            obtainJSONInfo(descriptor, "LDAP_PORT", varLDAPport);
            obtainJSONInfo(descriptor, "LDAP_USER_DN", varLDAPuserDn);
            obtainJSONInfo(descriptor, "LDAP_GROUP_DN", varLDAPgroupDn);
            obtainJSONInfo(descriptor, "LDAP_BASE", varLDAPbase);
            obtainJSONInfo(descriptor, "LDAP_ADMIN_GROUP", varLDAPadminGroup);
            obtainJSONInfo(descriptor, "PUBLIC_NODE", varPublicNode);
            obtainJSONInfo(descriptor, "ADDRESS_POOL", varAddressPool);
            obtainJSONInfo(descriptor, "DOCKER_REGISTRY", varDockerRegistry);
            obtainJSONInfo(descriptor, "EXTERNAL_DOCKER_REGISTRY", varExternalDockerRegistry);
            obtainJSONInfo(descriptor, "ARTIFACT_REPOSITORY", varArtifactRepository);

            obtainJSONInfo(response, "ROOT_TOKEN", varVaultToken);
        } else {
            commonspec.getLogger().debug("Basic information from bootstrap was previously obtained");
        }
    }


    /**
     * Obtains info from a json stored in a variable and expose it in a thread variable
     *
     * @param json  json where to get info from
     * @param info  info to be obtained from json
     * @param envVar    thread environment variable where to expose obtained info
     */
    public void obtainJSONInfo(String json, String info, String envVar) {
        String jqExpression = "";
        String position = null;

        switch (info) {
            case "MASTERS":
                jqExpression = "$.nodes[?(@.role == \"master\")].networking[0].ip";
                break;
            case "NODES":
                jqExpression = "$.nodes[?(@.role == \"agent\")].networking[0].ip";
                break;
            case "PRIV_NODES":
                jqExpression = "$.nodes[?(@.role == \"agent\" && @.public == false)].networking[0].ip";
                break;
            case "PUBLIC_NODES":
                jqExpression = "$.nodes[?(@.role == \"agent\" && @.public == true)].networking[0].ip";
                break;
            case "PUBLIC_NODE":
                jqExpression = "$.nodes[?(@.role == \"agent\" && @.public == true)].networking[0].ip";
                position = "0";
                break;
            case "GOSEC_NODES":
                jqExpression = "$.nodes[?(@.role == \"gosec\")].networking[0].ip";
                break;
            case "ID":
                jqExpression = "$.id";
                break;
            case "SCHEMA_VERSION":
                jqExpression = "$.schemaVersion";
                break;
            case "DNS_SEARCH":
                jqExpression = "$.dnsSearch";
                break;
            case "INTERNAL_DOMAIN":
                jqExpression = "$.internalDomain";
                break;
            case "ARTIFACT_REPOSITORY":
                jqExpression = "$.artifactRepository";
                break;
            case "DOCKER_REGISTRY":
                jqExpression = "$.dockerRegistry";
                break;
            case "EXTERNAL_DOCKER_REGISTRY":
                jqExpression = "$.externalDockerRegistry";
                break;
            case "REALM":
                jqExpression = "$.security.kerberos.realm";
                break;
            case "KRB_HOST":
                jqExpression = "$.security.kerberos.kdcHost";
                break;
            case "LDAP_HOST":
                jqExpression = "$.security.ldap.url";
                break;
            case "VAULT_HOST":
                jqExpression = "$.nodes[?(@.role == \"gosec\")].networking[0].ip";
                position = "0";
                break;
            case "IP":
                jqExpression = "$.nodes[?(@.role == \"master\")].networking[0].ip";
                position = "0";
                break;
            case "ADMIN_USER":
                jqExpression = "$.security.ldap.adminUserUuid";
                break;
            case "TENANT":
                jqExpression = "$.security.tenantSSODefault";
                break;
            case "ACCESS_POINT":
                jqExpression = "$.proxyAccessPointURL";
                break;
            case "LDAP_URL":
                jqExpression = "$.security.ldap.url";
                break;
            case "LDAP_PORT":
                jqExpression = "$.security.ldap.port";
                break;
            case "LDAP_USER_DN":
                jqExpression = "$.security.ldap.userDn";
                break;
            case "LDAP_GROUP_DN":
                jqExpression = "$.security.ldap.groupDN";
                break;
            case "LDAP_BASE":
                jqExpression = "$.security.ldap.ldapBase";
                break;
            case "LDAP_ADMIN_GROUP":
                jqExpression = "$.security.ldap.adminrouterAuthorizedGroup";
                break;
            case "ROOT_TOKEN":
                jqExpression = "$.root_token";
                break;
            case "ADDRESS_POOL":
                jqExpression = "$.security.overlayNetwork.addressPool";
                break;
            default:
                break;
        }

        String value = "";
        try {
            value = commonspec.getJSONPathString(json, jqExpression, position).replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\"", "");
        } catch (Exception e) {
            if (info == "PUBLIC_NODE" || info == "TENANT") {
                value = null;
            } else {
                throw e;
            }
        }

        switch (info) {
            case "TENANT":
                if (value == null) {
                    value = "NONE";
                }
                break;
            case "ACCESS_POINT":
                value = value.replaceAll("https://", "");
                break;
            default:
                break;
        }

        if (!(info == "PUBLIC_NODE" && value == null)) {
            ThreadProperty.set(envVar, value);
        }

    }


    @Given("^I get services info from marathon")
    public void getServicesInfoFromMarathon() throws Exception {
        if (ThreadProperty.get("marathonVariables") == null) {
            getServicesInfoFromMarathonImpl();
        }
    }

    /**
     * Using marathon API, get all services deployed in cluster and set CCT variables
     *
     * @throws Exception
     */
    private void getServicesInfoFromMarathonImpl() throws Exception {
        if (System.getProperty("DCOS_PASSWORD") == null) {
            throw new Exception("DCOS_PASSWORD should be defined when we use @dcos annotation");
        }
        String marathonEndPoint = "/service/marathon/v2/apps";
        // Set sso token
        setGoSecSSOCookie(null, null, ThreadProperty.get("EOS_ACCESS_POINT"), ThreadProperty.get("DCOS_USER"), System.getProperty("DCOS_PASSWORD"), ThreadProperty.get("DCOS_TENANT"), null);
        // Securely send requests
        commonspec.setRestProtocol("https://");
        commonspec.setRestHost(ThreadProperty.get("EOS_ACCESS_POINT"));
        commonspec.setRestPort(":443");
        // Invoke marathon API
        Future<Response> response = commonspec.generateRequest("GET", false, null, null, marathonEndPoint, "", null);
        commonspec.setResponse(marathonEndPoint, response.get());
        if (commonspec.getResponse().getStatusCode() != 200) {
            throw new Exception("Error in marathon request. Response code: " + commonspec.getResponse().getStatusCode());
        }
        // Save versions
        List<String> appsToSaveVersion = Arrays.asList("gosec-management", "dyplon-http", "gosec-identities-daas", "gosec-services-daas", "command-center", "cct-deploy-api", "cct-universe", "cct-marathon-services", "cct-configuration-api");
        JSONObject marathonAnswer = new JSONObject(commonspec.getResponse().getResponse());
        JSONArray marathonApps = (JSONArray) marathonAnswer.get("apps");
        for (Object oApp : marathonApps) {
            if (oApp instanceof JSONObject) {
                JSONObject jsonApp = (JSONObject) oApp;
                try {
                    String serviceName = jsonApp.getString("id").replace("/command-center/", "");
                    String dockerImage = jsonApp.getJSONObject("container").getJSONObject("docker").getString("image");
                    String dockerImageName = dockerImage.substring(dockerImage.lastIndexOf("/") + 1, dockerImage.lastIndexOf(":"));
                    String dockerImageVersion = dockerImage.substring(dockerImage.lastIndexOf(":") + 1);
                    if (appsToSaveVersion.contains(dockerImageName)) {
                        ThreadProperty.set(dockerImageName + "_version", dockerImageVersion);
                        commonspec.getLogger().debug(dockerImageName + " - " + dockerImageVersion);
                        switch (dockerImageName) {
                            case "cct-marathon-services":
                                ThreadProperty.set("cct-marathon-services_id", serviceName);
                                break;
                            case "cct-universe":
                                ThreadProperty.set("cct-universe_id", serviceName);
                                break;
                            case "cct-deploy-api":
                                ThreadProperty.set("deploy_api_id", serviceName);
                                break;
                            case "command-center":
                                ThreadProperty.set("cct_ui_id", serviceName);
                                break;
                            case "cct-configuration-api":
                                ThreadProperty.set("configuration_api_id", serviceName);
                                break;
                            default:
                                break;
                        }
                    }
                } catch (Exception e) {
                    if (jsonApp.get("id") != JSONObject.NULL) {
                        commonspec.getLogger().debug("Error obtaining container in service with id: " + jsonApp.getString("id"));
                    }
                }
            }
        }

        ThreadProperty.set("marathonVariables", "true");
    }
}
