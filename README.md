# Data Prep on Airflow

*<p align="center">Workflow for this Repository.</p>*
<p align="center">
  <a href="https://github.com/DeloitteHux/Data_Prep_Airflow/actions/workflows/changelog.yml"><img src="https://github.com/leon-ai/leon/actions/workflows/build.yml/badge.svg?branch=develop" /></a>
  <br>
</p>

## 👋 Introduction
**Apache Airflow** is a platform to programmatically schedule and monitor workflows.

Users run **Airflow**  in order to take advantage of the increased stability and autoscaling options.

### Why Airflow?

> 1. Airflow is a popular tool used for managing and monitoring workflows.
> 2. It works well for most of our data science workflows at Bluecore, but there are some use cases where other tools perform better.

### What is this repository for?

> This repository contains the following :
> - The Airflow
> - The Data Prep Dags
> - The File Splitter packages/modules
> - The Docker Build

## References:

<p align="center">
  <a href="https://airflow.apache.org/">References</a> ::
  <a href="https://github.com/marclamberti/webinar-airflow-chart">Webinar Airflow</a> 
</p>

### ☁️ Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/engine/install/)
- [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/) 
- [Helm](https://helm.sh/docs/intro/install/)
- [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-windows/)
- Supported OSes: Linux, macOS and Windows

### Installation

```sh
# Clone the repository (stable branch)
git clone -b master https://github.com/DeloitteHux/Data_Prep_Airflow.git

# Running First Time, Run Docker command
docker build -t airflow-custom:1.0.0 ./

# Create Cluster 
kind create cluster --name airflow-cluster --config kind-cluster.yaml

# Create Namespace
kubectl create namespace airflow

# Load docker image 
kind load docker-image airflow-custom:1.0.0 --name airflow-cluster

# Add Github SSH private key to the apply_env.yaml file (instructions in the file)

# For Local
kubectl apply -f apply_local.yaml

# For Deployed Env
kubectl apply -f apply_deploy.yaml

# Mongo data file load
kubectl exec -it mongo-db-0 -n airflow -- mongoimport --jsonArray -d test -c admin --file tmp/data/config/data.json

# helm repo add
helm repo add apache-airflow https://airflow.apache.org
 
# helm update
helm repo update

# For Local helm update
helm upgrade --install airflow apache-airflow/airflow -n airflow -f values_local.yaml --debug

# For Deployed Env helm update
helm upgrade --install airflow apache-airflow/airflow -n airflow -f values_deploy.yaml --debug

kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow --context kind-airflow-cluster

```

### 🚀 NOTE:

> 1. Now you can open localhost:8080 in your browser to access airflow UI
> 2. Import the variables.json in airflow variables. Update your snowflake username and password in the variables.
> 3. All set to go!

### Debugging

```sh
# Get all the pods on the cluster
kubectl get pods -n airflow

# Get the kubectl events executed
kubectl -n airflow get events --sort-by='{.lastTimestamp}'

# Get the logs of the scheduler in case of errors 
kubectl logs < airflow-scheduler-pod-name > -c git-sync-init -n airflow

# Execute an airflow command
exec < airflow-scheduler-pod-name > -n airflow -- < command >

# Delete the cluster
kind delete cluster --name airflow-cluster

# Run docker realted commands
sudo chmod 666 /var/run/docker.sock

# Deployment find services running
kubectl get svc -n airflow

# Deployment edit Webserver service from ClusterIP to LoadBalancer
kubectl edit svc -n airflow

kubectl delete pvc -n airflow pvc-airflow-poc && kubectl delete pv pv-airflow-poc

kubectl patch pvc -n airflow pvc-airflow-poc -p '{"metadata":{"finalizers":null}}'

kubectl apply -f pv.yaml && kubectl apply -f pvc.yaml


```
