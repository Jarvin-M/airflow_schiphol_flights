# Airflow
[Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html) is an open source tool for workflow orchestration of complex data pipelines by enforcing correct order of execution and allocation of resources. Airflow has an intergrated user interface to monitor and manage these workflows. Workflows are represented as Directed Acyclic Graphs(DAGs) and work to be done as Tasks which are ordered based on dependency.


## Architectural Components
![scenario_3.png](images/arch-airflow.png)

1. Scheduler - triggers scheduled worfklows and submits tasks to executor
2. Executor - runs submitted tasks
3. Web Server - user interface for inspecting, trigger and debugging DAGs and tasks
4. DAG Directory - folder with code definition of DAGs and tasks
5. Metadata database - stores state of scheduler, executor and webserver

A DAG has 3 types of Tasks:
1. [Operators](https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html) - predefined tasks e.g `BashOperator` - executes bash commands, `PythonOperator` - calls python functions and `EmailOperator`- sends email
2. [Sensors](https://airflow.apache.org/docs/apache-airflow/stable/concepts/sensors.html) - subclass of operators for external event listening
3. [Taskflow](https://airflow.apache.org/docs/apache-airflow/stable/concepts/taskflow.html) decorated `@task` - custom python function

Dependencies between tasks can then be declated with either  `>>` and  `<<` operators or `set_upstream` and `set_downstream` methods. This ensures ordered execution of the tasks.[Other features](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#concepts-branching) for influencing the order of execution are Branching, LatestOnly, Depends on Past, and Trigger rules.

Data between dependent tasks can be passed via:[Xcoms](https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html) (Cross-communications) or an external cloud storage service.

### Advantages

### Considerations

### Use Cases
Powerful in orchestrating 

## Getting Started

### Folder structure

### Run Airflow in Docker (Simplified steps)
For detailed steps; refer to Airflow documentation (here)[https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html]
<!-- <div class=\"alert alert-block alert-danger\">
    <b>Important:</b> Before beginning, set up your environment correctly as instructed in `Readme.md`
    This example set-up should not be used for production
</div> -->

1. Install Docker Desktop Community Edition. Docker-compose is already installed in Docker Desktop for both Mac and Windows users, hence, no need to install it separately.
2. docker-compose.yaml with preconfigured required service definitions including the scheduler, webserverm worker, initialization, flower(for monitoring), postgres database and redis broker
3. 

