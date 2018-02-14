# Nifi-JsonToChunksProcessor
Default SplitJson processor works onlly in-memory and it can fail while parsing large json files. This processor reads the json file in a streaming fashion and divides the large json array into smaller chunks. 
This is a Apache Nifi plugin and it only works with it. 

### Prerequisites

Java 1.8
Maven 3.*

### Compilation

it is very strait-forward process to compile this plugin

```
cd <CODE_HOME>
```
```
mvn clean install
```
or 
```
mvn clean package
```

### Usage

The default plugin packaging is nar bunles so the mvn comman above generates a nar bundle for the plugin under 
```
nifi-com.trendyol-nar/target 
```
directory. Copy the 

```
nifi-com.trendyol-nar-1.0-SNAPSHOT.nar
```
file to the 

```
<NIFI_HOME>/lib
```

directory and restart the nifi. The processor will appear in the processors list.


