<h1 align="center">Redis Stream Implementation with Boost 1.86.0</h1>

<br />
The sample Redis application runs as standalone exe on a machine. 
Requires boost_1_86_0 (maybe 1_85 at minimum for redis boost)
<br />

Design to be a submodule to a parent project to use redis work queue stream as
a producer and/or a consumer.

# 🚀 Available Scripts

In the project directory, you can build the Application with CMake

<br />

Use current folder as: ~/redisnet (Project root folder)
```
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_WORKQ_TESTS=ON -DCMAKE_INSTALL_PREFIX=/usr/local -G "Unix Makefiles" . -B ./build
cmake -DCMAKE_BUILD_TYPE=Debug -DBUILD_WORKQ_TESTS=ON -DCMAKE_INSTALL_PREFIX=/usr/local -G "Unix Makefiles" . -B ./build
cmake --build build --target all
```

## 🧪 test

Launches the test runner.

```
./redis_stream_go.sh
```
and
```
./redis_stream_stop.sh
```

<br />

## 🧪 Containment Docker image
Minikube env docker do use command:

```
eval $(minikube docker-env)
```

<br />

# 🧬 Project structure

This is the structure of the files in the project:

```sh
    │
    ├── clientProducer          # Client Producer for test application
    │   ├── io_utility          # Logging to files code
    │   ├── nholmann            # C++ JSON
    │   ├── CMakeLists.txt
    │   └── main.cpp          
    ├── clientRedis             # Client Redis for test application
    │   ├── io_utility          # Logging to files code
    │   ├── CMakeLists.txt
    │   └── main.cpp
    ├── cmake                   # cmake scripts (3.13)
    ├── workqstream      
    │   ├── consume             # Consumer of work queue stream
    │   │   ├── CMakeLists.txt
    │   │   └── *.cpp/*.h       # code
    │   └── produce             # Producer of work queue stream
    │       ├── CMakeLists.txt
    │       └── *.cpp/*.h       # code
    ├── .dockerignore
    ├── .gitignore
    ├── CMakeLists.txt          # Main CMake file
    ├── docker-compose.yaml
    ├── Dockerfile
    ├── INSTALL.txt       
    ├── LICENCE.txt
    ├── redis_stream_go.sh     # Test scripts
    ├── redis_stream_stop.sh
    └─ README.md               # This README.md document
 
```

```