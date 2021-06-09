# SparkActivity

## Docker
### Jupyter Notebook
    docker pull jupyter/all-spark-notebook

    docker run -it --rm -p 8888:8888 -v /home/francesc/work:/home/jovyan/work -w /home/jovyan/work jupyter/all-spark-notebook
### Spark
        docker pull sequenceiq/spark:1.6.0

        docker run -it -p 8088:8088 -p 8042:8042 -h sandbox sequenceiq/spark:1.6.0 bash