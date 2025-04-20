# Health-Monitor

Health-Monitor is a project aimed to monitor node health in a distributed architecture. The kafka producer produces random streams of node health to simulate node heartbeats and we use spark to help with aggregating data and monitor node health.

## Getting Started

Follow the instructions below to set up and run the project.

### Setting Up and Running the Project

1. **Install Docker and Docker Compose**  
    Ensure Docker and Docker Compose are installed on your system. You can download Docker from [Docker's official website](https://www.docker.com/).

2. **Start the Services**  
    Use the provided `docker-compose.yml` file to start the services. Navigate to the project directory and run:
    ```bash
    docker-compose up
    ```

3. **Run the Files in Parallel**  
    Open two terminal windows and execute the following commands:

    - **File 1**:  
      ```bash
      python kafka_streamer/producer.py
      ```

    - **File 2**:  
      ```bash
      python spark/spark.py
      ```

5. **View the Output**  
    The output of the application will be displayed on the terminal windows where the scripts are running.
