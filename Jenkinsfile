@Library('libpipelines@master') _

hose {
    EMAIL = 'qa'
    LANG = 'java'
    MODULE = 'bdt'
    REPOSITORY = 'github.com/bdt'
    SLACKTEAM = 'stratioqa'
    DEVTIMEOUT = 30
    RELEASETIMEOUT = 30
    MAXITRETRIES = 2
    FOSS = true
    BUILDTOOL = 'maven'
    BUILDTOOLVERSION = '3.5.0'
    FORCETICKETONPRS = true

    ITSERVICES = [
        ['ZOOKEEPER': [
           'image': 'zookeeper:3.5.7',
           'env': [],
           'sleep': 30,
           'healthcheck': 2181]],
        ['MONGODB': [
           'image': 'stratio/mongo:3.0.4']],
        ['ELASTICSEARCH': [
        'image': 'elasticsearch:7.4.2',
        'env': [
        'ES_JAVA_OPTS="-Des.cluster.name=%%JUID -Des.network.host=%%OWNHOSTNAME -Des.enforce.bootstrap.checks=true"',
        'discovery.type=single-node'
        ],
        'sleep': 40,
        'healthcheck': 9300]],
        ['CASSANDRA': [
           'image': 'stratio/cassandra-lucene-index:3.0.7.3',
           'volumes':[
                 'jts:1.14.0'],
           'env': [
                 'MAX_HEAP=256M'],
           'sleep': 30,
           'healthcheck': 9042]],
        ['KAFKA': [
           'image': 'confluentinc/cp-kafka:5.3.1',
           'env': [
                 'KAFKA_ZOOKEEPER_CONNECT=%%ZOOKEEPER:2181',
                 'KAFKA_ADVERTISED_HOST_NAME=%%OWNHOSTNAME',
                 'KAFKA_DELETE_TOPIC_ENABLE=true',
                 'KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%%OWNHOSTNAME:29092,PLAINTEXT_HOST://localhost:9092',
                 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT',
                 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1',
                 'KAFKA_BROKER_ID=1'],
           'sleep': 30,       
           'healthcheck': 9300]],
        ['LDAP': [
            'image': 'stratio/ldap-docker:0.2.0',
            'env': [
                  'LDAP_SCHEMA=rfc2307',
                  'LDAP_HOSTNAME=%%OWNHOSTNAME',
                  'HOSTNAME=%%OWNHOSTNAME.cd',
                  'LDAP_ORGANISATION=Stratio',
                  'LDAP_DOMAIN=stratio.com',
                  'LDAP_ADMIN_PASSWORD=stratio'],
            'sleep': 30,
            'healthcheck': 389]],
	    ['CHROME': [
	        'image': 'selenium/standalone-chrome-debug:3.141.59',
            'volumes': [
		    '/dev/shm:/dev/shm']]],
        ['UBUNTU': [
           'image': 'stratio/ubuntu-base:16.04',
           'cmd': '/usr/sbin/sshd -D -e']],
        ['VAULT': [
           'image': 'vault:0.6.2',
           'env': [
              'VAULT_DEV_ROOT_TOKEN_ID=stratio',
              'VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200'],
           'sleep': 5,
           'healthcheck': 8200]],
    ]
    
    ITPARAMETERS = """
	    | -DFORCE_BROWSER=%%CHROME
        | -DMONGO_HOST=%%MONGODB
        | -DCASSANDRA_HOST=%%CASSANDRA#0
        | -DES_NODE=%%ELASTICSEARCH#0
        | -DES_CLUSTER=%%JUID
        | -DZOOKEEPER_HOSTS=%%ZOOKEEPER:2181
        | -DSECURIZED_ZOOKEEPER=false
        | -DWAIT=1
        | -DAGENT_LIST=1,2
        | -DPROGLOOP=2
        | -DKAFKA_HOSTS=%%KAFKA:29092
        | -DSSH=%%UBUNTU
        | -DSLEEPTEST=1
        | -DLDAP_USER=admin
        | -DLDAP_BASE=dc=stratio,dc=com
        | -DLDAP_PASSWORD=stratio
        | -DLDAP_SSL=false
        | -DVAULT_URL=%%VAULT
        | -DVAULT_PROTOCOL=http://
        | -DVAULT_HOST=%%VAULT
        | -DVAULT_TOKEN=stratio
        | -DLDAP_URL=%%LDAP
        | -DINCLUDE=1
        | -DLDAP_PORT=389""".stripMargin().stripIndent()
    
    DEV = { config ->        
        doCompile(config)

        parallel(UT: {
            doUT(config)
        }, IT: {
            doIT(config)
        }, failFast: config.FAILFAST)

        doPackage(config)
    
        parallel(DOC: {
            doDoc(config)
        }, QC: {
            doStaticAnalysis(config)
	    doCoverallsAnalysis(config)
        }, DEPLOY: {
            doDeploy(config)
        }, failFast: config.FAILFAST)

     }
}
