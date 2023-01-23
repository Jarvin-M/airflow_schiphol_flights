[TOC]

# Apache Airflow
[Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html) is an open-source platform for developing, scheduling, and monitoring batch-oriented workflows. Airflow�s extensible Python framework enables you to build workflows connecting with virtually any technology. A web interface helps manage the state of your workflows.

## Key Concepts
Basic illustration of a DAG:

![Basic DAG](images/basic-dag.png)

A DAG has 3 types of Tasks:

1. [Operators](https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html) - predefined tasks e.g `BashOperator` - executes bash commands, `PythonOperator` - calls python functions and `EmailOperator`- sends email
2. [Sensors](https://airflow.apache.org/docs/apache-airflow/stable/concepts/sensors.html) - subclass of operators for external event listening
3. [Taskflow](https://airflow.apache.org/docs/apache-airflow/stable/concepts/taskflow.html) decorated `@task` - custom python function

Dependencies between tasks can then be declated with either  `>>` and  `<<` operators or `set_upstream` and `set_downstream` methods. This ensures ordered execution of the tasks.[Other features](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#concepts-branching) for influencing the order of execution are Branching, LatestOnly, Depends on Past, and Trigger rules.

Data between dependent tasks can be passed via: [Xcoms](https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html) (Cross-communications) or External cloud storage service.


Airflow has an intergrated user interface to monitor and manage these workflows. Workflows are represented as Directed Acyclic Graphs(DAGs) and work to be done as Tasks which are ordered based on dependency.

## Why use Airflow?

* **Workflow as code**: reproducibility, version control and maintability
* **Open-source**: Lower cost, innovation, and community support come with open-source
* **Widely Integrated**: Can be used with multiple cloud providers and other tools like databases -> List of all [Airlfow plugins/operators](https://registry.astronomer.io/modules?page=1&types=Operators)
* **User interface**: Airflow UI allows users to monitor and troubleshoot pipelines with ease
* **Automation**: easy of scheduling and orchestration. e,g Airflow has it built-in retry mechanism for fault tolerance
* **Alerting**: Continous monitoring for success or failure of tasks


## Architectural Components
![scenario_3.png](images/arch-airflow.png)

1. **Scheduler** - triggers scheduled worfklows and submits tasks to executor
2. **Executor** - runs submitted tasks
3. **Web Server** - user interface for inspecting, trigger and debugging DAGs and tasks
4. **DAG Directory** - folder with code definition of DAGs and tasks
5. **Metadata database** - stores state of scheduler, executor and webserver







### When to use Airflow
Powerful in orchestrating dynamic and time interval/schedule based workflows for the following example usecases:

* ETL data pipelines(utilize intermediate data stores for different tasks/stages)
* Machine learning workflow
* Automated reporting
* Backups and DevOps tasks

### When not to use Airflow

* Realtime streaming pipelines - since airflow is schedule based, it is not appropriate for continous realtime events
* Limited knowledge of Python


## Resources
1. https://www.contino.io/insights/apache-airflow


## Getting Started
![](images/flights_dag.png)
### 1. Setup Airflow to run on Docker (Simplified steps)
For detailed steps on running Airflow on Docker; refer to Airflow documentation [here](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)

1. Install Docker Desktop Community Edition. Docker-compose is already installed in Docker Desktop for both Mac and Windows users, hence, no need to install it separately.
2. docker-compose.yaml with preconfigured required service definitions including the scheduler, webserver, worker, initialization, flower(for monitoring), postgres database and redis broker
`curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.5.1/docker-compose.yaml'`
    * The generic Airflow architecture docker-compose is tailored to cater for the following:
        1. Additional Postgres Database for our usecase -  `curated-postgres`, additional port mapping is done to avoid port conflict with the default airflow postgres database
        ```docker
         curated-postgres:
            image: postgres:13
            ports:
            - "5961:5432"
        ```
        2. Connection ID for the curated Postgres database is needed to grant Airflow access to this database. More about managing connections -> [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#managing-connections)
        ```
        AIRFLOW_CONN_POSTGRES_DEFAULT: postgres://airflow:airflow@host.docker.internal:5961/airflow
        ```

        2. Set up SMTP server via docker environment variables. In this example we utilise the gmail smtp server via setting a [google app login token](https://support.google.com/mail/answer/185833?hl=en-GB). Other options are [SendGrid](https://airflow.apache.org/docs/apache-airflow/stable/howto/email-config.html#send-email-using-sendgrid) and AWS SES
        ```docker
        AIRFLOW__SMTP__SMTP_HOST: 'smtp.gmail.com'
        AIRFLOW__SMTP__SMTP_STARTTLS: 'false'
        AIRFLOW__SMTP__SMTP_SSL: 'true'
        AIRFLOW__SMTP__SMTP_USER: '<your-email>'
        AIRFLOW__SMTP__SMTP_PASSWORD: '<google-app-token>' 
        AIRFLOW__SMTP__SMTP_PORT: 465
        AIRFLOW__SMTP__SMTP_MAIL_FROM: '<your-email>
        ```
        3. Connection ID for cura
3. The docker-compose mounts and synchronize directories between your computer and docker container. We create these directories
    ```
    mkdir -p ./dags ./logs ./plugins```
4. Initialize database to create the first user account based on the logins specified in the `docker-compose.yaml`
```
docker compose up airflow-init
```
5. For subsequent runs you can start and clean up all services as below
    ```docker
        #start
        docker compose up

        # clean up 
        docker compose down --volumes --remove-orphans
    ```

### 2. Flights API Taskflow
To orchestrate the usecase described above, we define an airflow taskflow as a DAG with operators that perform a unit of work.

https://gist.github.com/Jarvin-M/cc1035db315121483d34bfc545b1e55f

1. DAG declaration.
A context manager is used, with which the DAG definition is added implicitly. Other DAG declaration options exist -> [link](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#declaring-a-dag)
Schedule interval is done daily at 23:00.
Default arguments can be set in the DAG declaration to be inherited by all operators rather than setting them individually for each operator.

2. `create_flights_table` is a `PostgresOperator` that creates a table - `flights` in our curated Postgress database using a connection ID set in the docker compose. 

3. `process_data` is a `PythonOperator` which loads python support function - `process_and_store_data` in `dags/scripts/flights_utils` which ingests data from the schiphol API, transforms it and stores it in our flights table. 

4. `retrieve_insights` - is a `PythonOperator` which loads python support function - `generate_insights` in `dags/scripts/flights_insights` which generates a summary of delayed flights per destination per gate

5. `send_insights_email` is a `PythonOperator` which encapsulates `EmailOperator` that builds an emails, attaches a csv file of the insights and sends to the recipient emails.

6. Task dependency definition specifying the execution order - ` create_flights_table >> process_data >> retrieve_insights >> send_insights_email`. Branching control flow options exist for conditional flows -> [link](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#control-flow). 



### 3. Run DAG in Airflow UI
Within the Airflow UI, you can manage DAGs, configurations, access documentation and security settings. Below is a screenshot of the trigggered successful DAG run

![](images/successful_dag.png)