### Airflow Docker Compose Setup Guide

**1. Create a new directory**

```bash
mkdir airflow_docker
cd airflow_docker

```

**2. Fetch the `docker-compose.yaml` file for Airflow 3.1.7**
This file contains the "recipe" for all the containers (webserver, scheduler, database) that make up the Airflow stack.

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.7/docker-compose.yaml'

```

**3. Initialize the environment folders and permissions**
```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

```

**4. Verify Docker is ready**
Ensure the Docker Desktop application is running and the command-line tools are accessible.

```bash
docker compose version 
docker info

```

**5. Initialize the Airflow Database**
This runs a one-time setup to prepare the PostgreSQL database and create the default user.

```bash
docker compose up airflow-init

```

Wait until you see `airflow-init exited with code 0` before moving to the next step.

**6. Start all Airflow services**
This command pulls the images and spins up the entire architecture.

```bash
docker compose up -d

```

**7. Add your DAG file**
While the services are starting, you can move your script into the mapped folder:

* **Placement:** Place your `tutorial_dag.py` inside the `./dags` folder you created in Step 3.
* **Syncing:** Docker will automatically pick up the file and show it in the UI within about 60 seconds.

**8. Access the UI**
Once the containers are healthy, navigate to **port 8080**.

* **URL:** `http://localhost:8080`
* **Username:** `airflow`
* **Password:** `airflow`

**9. Cleanup**
To stop the containers when your demo is finished, press `Ctrl + C` and then run:

```bash
docker compose down

```