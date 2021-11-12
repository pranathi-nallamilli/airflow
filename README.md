How to run:
Make sure you have docker kind, helm, kubectl installed on your system

If running first time, run

-- docker build -t airflow-custom:1.0.0 ./

-- kind create cluster --name airflow-cluster --config kind-cluster.yaml

-- kubectl create namespace airflow

-- kind load docker-image airflow-custom:1.0.0 --name airflow-cluster 

Adding Github SSH private key to the apply_env.yaml file (instructions in the file)

-- For Local
-- kubectl apply -f apply_local.yaml

--For Deployed Env
-- kubectl apply -f apply_deploy.yaml

-- kubectl exec -it mongo-db-0 -n airflow -- mongoimport --jsonArray -d test -c admin --file tmp/data/config/data.json

-- helm repo add apache-airflow https://airflow.apache.org

-- helm repo update

-- For Local
-- helm upgrade --install airflow apache-airflow/airflow -n airflow -f values_local.yaml --debug 

--For Deployed Env
-- helm upgrade --install airflow apache-airflow/airflow -n airflow -f values_deploy.yaml --debug 

-- kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow --context kind-airflow-cluster

Now you can open localhost:8080 in your browser to access airflow ui

Import the variables.json in airflow variables.
Update your snowflake username and password in the variables.

All set to go!


debugging:

To get all the pods on the cluster:
-- kubectl get pods -n airflow

To get the kubectl events executed
-- kubectl -n airflow get events --sort-by='{.lastTimestamp}'

To get the logs of the scheduler in case of errors
-- kubectl logs < airflow-scheduler-pod-name > -c git-sync-init -n airflow

To execute an airflow command
-- exec < airflow-scheduler-pod-name > -n airflow -- < command >

To delete the cluster:
-- kind delete cluster --name airflow-cluster

To run docker realted commands
--sudo chmod 666 /var/run/docker.sock    

For deployment find services running
-- kubectl get svc -n airflow

For deployment edit Webserver service from ClusterIP to LoadBalancer by
-- kubectl edit svc -n airflow


kubectl delete pvc -n airflow pvc-airflow-poc && kubectl delete pv pv-airflow-poc


kubectl patch pvc -n airflow pvc-airflow-poc -p '{"metadata":{"finalizers":null}}'

kubectl apply -f pv.yaml && kubectl apply -f pvc.yaml