@Library('jenkins-helpers@v0.1.10')

def label = "opcua-extractor-net-${UUID.randomUUID().toString()}"

podTemplate(
    label: label,
    annotations: [
        podAnnotation(key: "jenkins/build-url", value: env.BUILD_URL ?: ""),
        podAnnotation(key: "jenkins/github-pr-url", value: env.CHANGE_URL ?: ""),
    ],
    containers: [
    containerTemplate(name: 'docker',
        command: '/bin/cat -',
        image: 'docker:17.06.2-ce',
        resourceRequestCpu: '1000m',
        resourceRequestMemory: '500Mi',
        resourceLimitCpu: '1000m',
        resourceLimitMemory: '500Mi',
        ttyEnabled: true),
    containerTemplate(
        name: 'test-servers',
        image: 'python:3.6',
        command: '/bin/cat -',
        resourceRequestCpu: '1000m',
        resourceRequestMemory: '800Mi',
        resourceLimitCpu: '1000m',
        resourceLimitMemory: '800Mi',
        envVars: [
            envVar(key: 'PYTHONPATH', value: '/usr/local/bin')
        ],
        ttyEnabled: true),
    containerTemplate(name: 'dotnet-mono',
        image: 'eu.gcr.io/cognitedata/dotnet-mono:2.2-sdk',
        envVars: [
            secretEnvVar(key: 'CODECOV_TOKEN', secretName: 'codecov-tokens', secretKey: 'opcua-extractor-net'),
            // /codecov-script/upload-report.sh relies on the following
            // Jenkins and Github environment variables.
            envVar(key: 'JENKINS_URL', value: env.JENKINS_URL),
            envVar(key: 'BRANCH_NAME', value: env.BRANCH_NAME),
            envVar(key: 'BUILD_NUMBER', value: env.BUILD_NUMBER),
            envVar(key: 'BUILD_URL', value: env.BUILD_URL),
            envVar(key: 'CHANGE_ID', value: env.CHANGE_ID),
        ],
        resourceRequestCpu: '1500m',
        resourceRequestMemory: '3000Mi',
        resourceLimitCpu: '1500m',
        resourceLimitMemory: '3000Mi',
        ttyEnabled: true)
    ],
    volumes: [
        secretVolume(
            secretName: 'nuget-credentials',
            mountPath: '/nuget-credentials',
            readOnly: true),
        secretVolume(
            secretName: 'jenkins-docker-builder',
            mountPath: '/jenkins-docker-builder',
            readOnly: true),
        configMapVolume(configMapName: 'codecov-script-configmap', mountPath: '/codecov-script'),
        hostPathVolume(hostPath: '/var/run/docker.sock', mountPath: '/var/run/docker.sock')]
) {
    properties([buildDiscarder(logRotator(daysToKeepStr: '30', numToKeepStr: '20'))])
    node(label) {
        container('jnlp') {
            stage('Checkout') {
                checkout(scm)
                imageRevision = sh(returnStdout: true, script: 'git rev-parse --short=8 HEAD').trim()
                buildDate = sh(returnStdout: true, script: 'date +%Y-%m-%dT%H%M').trim()
                dockerImageName = "eu.gcr.io/cognitedata/opcua-extractor-net"
                dockerImageTag = "${buildDate}-${imageRevision}"
            }
        }
        container('test-servers') {
            stage('Install pipenv') {
                sh('pip install pipenv')
            }
            stage('Install server dependencies') {
                sh('pipenv install -d --system')
                sh('pip list')
            }
            stage('Start servers') {
                sh('./startservers.sh')
            }
        }
        container('dotnet-mono') {
            stage('Install dependencies') {
                sh('apt-get update && apt-get install -y libxml2-utils')
                sh('cp /nuget-credentials/nuget.config ./nuget.config')
                sh('./credentials.sh')
                sh('mono .paket/paket.exe install')
            }
            stage('Build') {
                sh('dotnet build')
            }
            stage('Run tests') {
                sh('./test.sh')
                archiveArtifacts artifacts: 'coverage.lcov', fingerprint: true
            }
            stage("Upload report to codecov.io") {
                sh('bash </codecov-script/upload-report.sh')
            }
        }
        container('docker') {
            stage("Build Docker images") {
                sh('docker images | head')
                sh('#!/bin/sh -e\n'
                        + 'docker login -u _json_key -p "$(cat /jenkins-docker-builder/credentials.json)" https://eu.gcr.io')

                sh('cp /nuget-credentials/nuget.config ./nuget.config')
                // sh("docker build -f Dockerfile.build .")
                // Building twice to get sensible output. The second build will be quick.
                sh("image=\$(docker build -f Dockerfile.build . | awk '/Successfully built/ {print \$3}')"
                       + "&& id=\$(docker create \$image)"
                       + "&& docker cp \$id:/build/deploy ."
                       + "&& docker rm -v \$id"
                       + "&& docker build -t ${dockerImageName}:${dockerImageTag} .")
                sh('docker images | head')
            }
            if (env.BRANCH_NAME == 'master') {
                stage('Push Docker images') {
                    sh("docker push ${dockerImageName}:${dockerImageTag}")
                }
            }
        }
    }
}
