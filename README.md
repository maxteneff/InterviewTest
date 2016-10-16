# InterviewTest

## Usage:
```
ServerLogsStreaming <hostname> <port> <checkpoint-directory> <output-file>. 
     <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data. 
     <checkpoint-directory> directory to local (file:/) or HDFS-compatible (hdfs:/) file system.
     <output-file> should be in local file system

In local mode, <master> should be 'local[n]' with n > 1
Both <checkpoint-directory> and <output-file> must be absolute paths
```
## How to run:

#### 1. Download: 
```
git clone https://github.com/maxteneff/InterviewTest.git
```
#### 2. Build: 
```
cd InterviewTest && sbt package
```
#### 3. Run: 
```
spark-submit --master local[2] interviewtest_2.11-1.0.jar localhost 9999 file:/tmp output.txt
```
#### 4. Look for output file: 
```
tail -f output.txt
```

## Logs generator:
```
python logs_gen.py
```
