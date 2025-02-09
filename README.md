# All-in-One Docker BigDataOps

<p align="center">
    <img alt="logo" src="img.png" width=75% />
</p>

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)  
[![GitHub Stars](https://img.shields.io/github/stars/heirinsinho/all-in-one-docker-bigdataops)](https://github.com/heirinsinho/all-in-one-docker-bigdataops/stargazers)

All-in-One Docker BigDataOps is a comprehensive Docker Compose 
environment that bundles **Hadoop**, **Spark**, **Hive**, **Hue**, and **Airflow** into a ready-to-run stack. This project simplifies Big Data operations, but it is intended to be an academic approach to Big Data, do not consider it as best practices for production environments

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Why Use This Repository?](#why-use-this-repository)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Quick Start](#quick-start)
- [Airflow Configuration](#airflow-configuration)
- [Documentation & Examples](#documentation--examples)
- [Contributing](#contributing)
- [License](#license)
- [Support](#support)

---

## Overview

This repository provides an **all-in-one solution** for Big DataOps by integrating industry-leading tools into a single Docker Compose setup. With minimal configuration, you can deploy a powerful stack that covers everything from distributed processing with Hadoop to data orchestration with Airflow.

---

## Key Features

- **Fully Integrated Tools**  
  Enjoy a pre-configured environment featuring:
  - **Hadoop**: Distributed storage and processing
  - **Spark**: Big data analytics
  - **Hive**: SQL-based querying
  - **Hue**: User-friendly web interface
  - **Airflow**: Robust data pipeline orchestration

- **Quick Setup**  
  Start everything with a single command:
  ```bash
  make start-all
  ```

- **Versatile Usage**  
  Perfect for:
  - Learning and experimentation
  - Development and testing
  - Small-scale production deployments

- **Example Workflows**  
  Includes sample jobs and scripts to help you kickstart your big data projects.

- **Customizable**  
  Easily extend or modify the stack to match your unique requirements.

---

## Why Use This Repository?

- **Ease of Use**: Say goodbye to tedious configurations. Focus on building and testing your data solutions.
- **All-in-One Solution**: All necessary tools are bundled together for a seamless experience.
- **Community-Driven**: Designed for accessibility and collaboration, making it a great resource for the Big DataOps community.

---

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

If you are using Windows:
- [Cygwin64](https://cygwin.com/install.html) Make sure you activate "Make" as aditional package

or

- [Make](https://gnuwin32.sourceforge.net/packages/make.htm) Make you sure you download grep and core utils too

### Quick Start

Clone the repository and launch the full stack with one command:

```bash
git clone https://github.com/heirinsinho/all-in-one-docker-bigdataops.git
cd all-in-one-docker-bigdataops
make start-all
```

In case you prefer to run only a specific part, you can just:

```bash
make start-spark
make start-hadoop
make start-streaming
make start-airflow
```

---

## Airflow Configuration

After starting the environment, configure Airflow with the following connections:

- **Spark Connection**
  - **conn_id**: `spark_docker`
  - **conn_type**: `spark`
  - **conn_host**: `spark://spark-master`
  - **conn_port**: `7077`

- **Hadoop SSH Connection**
  - **conn_id**: `ssh_hadoop`
  - **conn_type**: `ssh`
  - **conn_host**: `namenode`
  - **conn_port**: `22`
  - **conn_login**: `root`
  - **conn_password**: `root123`

Refer to the Makefile for additional commands and usage examples.

---

## Examples

There is a complete example of big data streaming application within this repo:

**MadFlow: A real time metric of the occupancy status in Madrid**

You can find the scripts for the execution both as an Airflow DAG and in Jupyter notebooks
Finally the api_madflow folder contains backend and frontend for the application to run locally
If you encounter any issues or have suggestions, please add an issue or submit a pull request. Your feedback helps improve the project for everyone.

---

## Contributing

Contributions are encouraged! If you'd like to enhance the project or fix an issue, please fork the repository and submit a pull request. See our [Contribution Guidelines](CONTRIBUTING.md) for more details.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for further information.

---

## Support

If you encounter any issues or have suggestions, please [open an issue](https://github.com/heirinsinho/all-in-one-docker-bigdataops/issues) or submit a pull request. Your feedback helps improve the project for everyone.

---

Happy Big DataOps!
