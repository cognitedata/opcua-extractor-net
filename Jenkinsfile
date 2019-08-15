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
            envVar(key: 'LC_ALL', value: 'C.UTF-8'),
            envVar(key: 'LANG', value: 'C.UTF-8')
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
                checkout([$class: 'GitSCM',
                  branches: scm.branches,
                  extensions: [
                    [ $class: 'SubmoduleOption',
                      disableSubmodules: false,
                      parentCredentials: true,
                      recursiveSubmodules: true,
                      reference: '',
                      trackingSubmodules: false],
                    [ $class: 'CleanCheckout' ]
                  ],
                  userRemoteConfigs: scm.userRemoteConfigs
                ])
                dockerImageName = "eu.gcr.io/cognitedata/opcua-extractor-net"
                version = sh(returnStdout: true, script: "git describe --tags HEAD || true").trim()
                version = version.replaceFirst(/-(\d+)-.*/, '-pre.$1')
                lastTag = sh(returnStdout: true, script: "git describe --tags --abbrev=0").trim()
                echo "$version"
                echo "$lastTag"
                echo "${env.BRANCH_NAME}"
            }
        }
        container('dotnet-mono') {
            stage('Install dependencies') {
                sh('apt-get update && apt-get install -y libxml2-utils python3-pip')
                sh('pip3 install pipenv')
                sh('pipenv install -d --system')           
                sh('cp /nuget-credentials/nuget.config ./nuget.config')
                sh('./credentials.sh')
                sh('mono .paket/paket.exe install')
            }
            stage('Start servers') {
                sh('./startservers.sh')
            }

            stage('Build') {
                sh('dotnet build')
            }
            timeout(5) {
                stage('Run tests') {
                    sh('./test.sh')
                    archiveArtifacts artifacts: 'coverage.lcov', fingerprint: true
                }
            }
            stage("Upload report to codecov.io") {
                sh('bash </codecov-script/upload-report.sh')
            }
            if ("$lastTag" == "$version" && env.BRANCH_NAME == "master") {
                stage('Build release versions') {
                    sh('apt-get update && apt-get install -y zip')
                    packProject('win-x64', "$version", "run.bat")
                    packProject('win81-x64', "$version", "run.bat")
                    packProject('linux-x64', "$version", "run.sh")
                }
                stage('Deploy to github release') {
                    withCredentials([usernamePassword(credentialsId: '5ad41c53-4df7-4ca8-a276-9822375568b3', usernameVariable: 'ghusername', passwordVariable: 'ghpassword')]) {
                        sh("python3 deploy.py cognitedata opcua-extractor-net $ghpassword $version win-x64.${version}.zip win81-x64.${version}.zip linux-x64.${version}.zip")
                    }               
                }
            }
        }
        container('docker') {
            stage("Build Docker images") {
                sh('docker images | head')
                sh('#!/bin/sh -e\n'
                        + 'docker login -u _json_key -p "$(cat /jenkins-docker-builder/credentials.json)" https://eu.gcr.io')

                sh('cp /nuget-credentials/nuget.config ./nuget.config')
                // Building twice to get sensible output. The second build will be quick.
                sh("image=\$(docker build -f Dockerfile.build . | awk '/Successfully built/ {print \$3}')"
                       + "&& id=\$(docker create \$image)"
                       + "&& docker cp \$id:/build/deploy ."
                       + "&& docker rm -v \$id"
                       + "&& docker build -t ${dockerImageName}:${version} .")
                sh('docker images | head')
            }
            if (env.BRANCH_NAME == 'master') {
                stage('Push Docker images') {
                    sh("docker push ${dockerImageName}:${version}")
                }
            }
        }
    }
}

void packProject(String configuration, String version, String executor) {
    sh("dotnet publish -c Release -r $configuration --self-contained true Extractor/")
    sh("mkdir -p ./${configuration}/bin/")
    sh("mv Extractor/bin/Release/netcoreapp2.2/${configuration}/publish/* ./${configuration}/bin/")
    sh("cp -r ./config ./${configuration}/")
    sh("cp ./Extractor/${executor} ./${configuration}/")
    sh("cp ./LICENSE.md ./${configuration}/")
    dir("$configuration") {
        sh("zip -r ../${configuration}.${version}.zip *")
    }
}

