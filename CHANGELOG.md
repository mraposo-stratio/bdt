# Changelog

Only listing significant user-visible, not internal code cleanups and minor bug fixes.

## 0.9.0 (upcoming)

* [QATM-1899] Upgrade Cucumber to 2.4.0
* [QATM-1981] Refactor specs and tests
* [QATM-2049] Fix for incorrect step after specs refactor
* [QATM-1992] Fix include aspect error when we have tags in the next scenario after scenario included
* [QATM-2054] add new headers in case of governance request
* [QATM-2058] new step to check REST request does not contain text and fixes for gov requests
* [QATM-2071] fix step to check services distribution in datacenters
* [QATM-1974] RunOnEnv / SkipOnEnv improvements
* [QATM-2067] @always tag allows execute a test even if an important scenario failed previously

## 0.8.0 (February 07, 2019)

* [LDG-96] New kafka steps
* [QASAI-33] New step to check if a cookie has been saved correctly
* [QATM-967] Updated Selenium version to 3.9.1
* [PIT-68] Updated commons-lang3 version to 3.7
* [QATM-938] Add capability for getting raw HTML code
* [QATM-1201] Added method to convert json to yaml
* [QATM-1362] allow boolean expressions in runOnEnv and skipOnEnv tags
* [QATM-1362] modify regular expression for boolean conditions in runOnEnv and skipOnEnv
* [QATM-1181] Add glue for PGBouncer
* [QATM-1414] Add changes for search by text in selenium
* [QATM-1411] fix dcos-cli problems with ssh connection
* [QATM-324] Step to retrieve secrets from Vault
* [QATM-1267] Capability to operate with headers on selenium
* [QATM-1267] Changes to include keyboard generated events and delete non editable fields
* [QATM-1421] Changes to include parameter type Long in json files
* [QATM-1412] Step created to connect postgres with tls
* [QATM-1431] Add sql steps to bdt
* [QATM-1431] Refactor close database method name
* [QATM-1264] Add method to obtain dcos-cookie to bdt
* [QATM-1437] fix method to add a new label to framework
* [QATM-1464] set tenant to retrieve auth_token 
* [QATM-1464] fix error in condition to add tenant
* [QATM-1497] Changes to adapt step for different cases
* [QATM-1481] add kibana framework to glue file
* [QATM-1638] Include BODY in DELETE method
* [QATM-1644] fix to add label in DC/OS 1.11
* [QATM-1643] Change regex to match for loop and multiloop
* [QATM-1646] Add Postgres Database resource
* [QATM-1593] new steps for command center integration
* [QATM-1689] Replace securely by security type in DB connection
* [QATM-1593] new steps for command center integration
* [QASAI-33] New step to check if a cookie has benn saved correctly
* [QASAI-4] Step for Double click for selenium
* [QATM-1827] Add < and > in RunOnEnv or SkipOnEnv aspects
* [LDG-96] Added new kafka steps
* [QATM-1678] Change on replacement aspect
* [QATM-1842] Fix error changing result --> result in checkParams having logical operators
* [QASAI-34] Fix JUnitReport
* [QATM-1873] Fix Selenium tests
* [QATM-1871] New command center steps 
* [QATM-1870] add CucumberRunner for CommandCenter
* [QATM-1559] Connect to Cassandra with ssl
* [QATM-1878] Fix error in report when step is passed and it has text like a variable
* [QATM-1933] fix in function to convert jsonschema to json for CCT
* [QATM-1934] fix when converting jsonschema to json handling strings
* [QATM-1935] Allow empty string as default value in variables
* [QATM-1948] retrieve DCOS_AUTH_COOKIE and save in thread variable
* [QATM-1968] Fix to avoid false errors when we check a query response
* [QATM-1950] Create step to check resources after uninstall
* [QATM-1976] Print response if status code returned is different that expected
* [QATM-1977] New step to create metabase cookie
* [QATM-1979] New step to obtain metabase session id and generate cookie

## 0.7.0 (April 05, 2018)

* [CROSSDATA-1550] Add @multiloop tag
* [QATM-1061] Add step to send requests to endpoint with timeout and datatable

## 0.6.0 (February 22, 2018)

* [QATM-236] Quit after tagged scenario fails
* [QATM-70] New background tag
* [QA-189] Removed extra dot in 'service response status' step. Old step removed.
* [QA-152] New aspect merging 'include' and 'loop'. Old aspects removed.
* [QATM-74] New step to store text in a webElement in environment variable.
* [QATM-73] New step to read file, modify according to parameters and store in environment variable.
* [AR-732] Add Command Center - Configuration API Glue

## 0.5.1 (July 05, 2017)

* [QATM-78] Fix public releasing in maven central

## 0.5.0 (June 12, 2017)

* [QA-342] New cucumber tag @loop to multiple scenario executions

## 0.4.0 (March 06, 2017)

* [QA-272] better classes packaging
* [QA-298] Apache2 license. Step definitions redefined

## 0.3.0 (January 26, 2017)

* CukesGHooks will invoke the logger with each step

## 0.2.0 (June 2016)

* Ignored scenarios will fail if ignore cause was an already done jira ticket
* No more a submodule project
* Added new aspect to force the browser name and version in tests development ,using FORCE_BROWSER (even availableUniqueBrowsers factory is defined).

  Eg: mvn -U verify -DSELENIUM_GRID=jenkins.stratio.com:4444 **-DFORCE_BROWSER=chrome_33**
