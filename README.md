<h1 align="center">Redis Implementation with Boost 1.86.0</h1>

<br />
The sample ClientRedis application runs as standalone exe on a machine. 
Requires boost_1_86_0 (maybe 1_85 at minimum for redis boost)
<br />

# 🚀 Available Scripts

In the project directory, you can build the Application with CMake

<br />

Use current folder as: ~/redisnet (Project root folder)
```
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local -G "Unix Makefiles" . -B ./build
cmake --build build --target all
```

## 🧪 test

No tests implemented.

```
cmake --build build --target test

```
Launches the test runner.

```
docker compose up -d
```

<br />

## 🧪 Containment Docker image
Minikube env docker do use command:

```
eval $(minikube docker-env)
```

```
docker build -t redisnet:v1.0 -f Dockerfile .

docker run --network="host" --env REDIS_HOST=0.0.0.0 --env REDIS_PORT=6379 --env REDIS_CHANNEL=csToken_request,csToken_acquire --env REDIS_PASSWORD=<password>  -w /usr/src redisnet:v1.0

```

<br />

# 🧬 Project structure

This is the structure of the files in the project:

```sh
    │
    ├── clientPublisher         # ClientPublish application
    │   ├── io_utility          # Logging to files code
    │   ├── nholmann            # C++ JSON
    │   ├── CMakeLists.txt
    │   └── main.cpp          
    ├── clientRedis             # ClientRedis application
    │   ├── io_utility          # Logging to files code
    │   ├── CMakeLists.txt
    │   └── main.cpp
    ├── cmake                   # cmake scripts (3.13)
    ├── nmtoken_runner          # folder for nm_go.bat working directory
    ├── redisPublish            # Publish subjects
    │   ├── CMakeLists.txt
    │   └── *.cpp/*.h           # code
    ├── redisSubscribe          # Subcribe to subjects
    │   ├── CMakeLists.txt
    │   └── *.cpp/*.h           # code
    ├── tests                   # NIY
    ├── .dockerignore
    ├── .gitignore
    ├── api.http                # VS code extension POSTMAN alternative
    ├── CMakeLists.txt          # Main CMake file
    ├── docker-compose.yaml
    ├── Dockerfile
    ├── INSTALL.txt       
    ├── LICENCE.txt
    ├── nm_go.sh                # Scripts
    ├── nm_stop.sh
    └─ README.md               # This README.md document
 
```
