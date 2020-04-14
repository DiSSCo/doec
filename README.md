# Disco Digital Object Controller #

## Version 1.0.0 ##

This java project provides a simple mechanism to process events over digital objects, keeping a provenance
record of those events

## 1. Getting Started

### 1.1. Generate the jar file of the project with dependencies
```mvn package```

### 1.2 Deploy jar file in CORDRA
In order to be able to process events fired automatically when digital object is created, received, updated and/or deleted 
in CORDRA, we need to deploy this jar file inside the CORDRA's data/lib directory and restart CORDRA.

### 1.3 Create the doec configuration file
The class DigitalObjectEventController on this jar library receives in its constructor a parameter with the path of the 
configuration file that has the information that tell the java class what CORDRA instance is the source of the event and 
which one the target. Please, by using the config_template.properties, create a doec_config.properties file and place it 
inside the CORDRA's data folder of the source Cordra instance

### 1.4 Configure the desired CORDRA's schema to record provenance for its instances 
<pre><code>
var cordra = require('cordra');

exports.beforeSchemaValidation = beforeSchemaValidation;
exports.objectForIndexing = objectForIndexing;
exports.onObjectResolution = onObjectResolution;
exports.beforeDelete = beforeDelete;

exports.methods = {};
exports.methods.getVersionAtGivenTime = getVersionAtGivenTime;
exports.methods.publishVersion = publishVersion;
exports.methods.getProvenanceRecords = getProvenanceRecords;
exports.methods.getObjectAtGivenTime = getObjectAtGivenTime;
exports.methods.processEvent = processEvent;


function getDigitalObjectEventController(){
   var dataDir = java.lang.System.getProperty("cordra.data");
   var doecConfigFilePath = java.nio.file.Paths.get(dataDir).resolve("doec_config.properties");
   
   var DigitalObjectEventController = Java.type("eu.dissco.doec.DigitalObjectEventController");
   var doec = new DigitalObjectEventController(doecConfigFilePath);
   
   return doec;
}

function beforeSchemaValidation(object, context) {   
   var doec = getDigitalObjectEventController();
   if (context.isNew) {
       doec.processCreateEvent(JSON.stringify(object),JSON.stringify(context));    
   } else {    
       var originalObject = cordra.get(context.objectId);
       doec.processUpdateEvent(JSON.stringify(originalObject),JSON.stringify(object),JSON.stringify(context));    
   }    
   return object;
}

function objectForIndexing(object, context) {
   /* Insert code here */
   return object;
}

function onObjectResolution(object, context) {
   var doec = getDigitalObjectEventController();
   //doec.processRetrieveEvent(JSON.stringify(object),JSON.stringify(context));    
   return object;
}

function beforeDelete(object, context) {
   var doec = getDigitalObjectEventController();
   doec.processDeleteEvent(JSON.stringify(object),JSON.stringify(context));   
}

/*
Function to process a custom event over a digital object
For example, to process the event DepositInMuseum for the Digital Specimen 20.5000.1025/testDS
the doip call should look like:
{
  "targetId": "20.5000.1025/testDS",
  "operationId": "processEvent",
  "authentication": { "username": "YOUR_USERNAME", "password": "YOUR_PASSWORD" },
  "input": {
    "eventType":"DepositInMuseum",
    "agentId":"20.5000.1025/d298a8c18cb62ee602b8",
    "role":"Scientist",
    "timestamp": "2019-12-02T18:42:59.361Z",
    "description":"Specimen deposit in museum for exhibition",
    "data":{
        "museumId":"20.5000.1025/2fd4b4e4525def2122bb"
    }
  }
}
#
#
*/
function processEvent(object, context) {   
   var doec = getDigitalObjectEventController();
   var event = JSON.stringify(context.params);
   doec.processCustomEvent(event,object.id);
   return object;
}


/*
Function that get the published version of a digital object at a given time
For example, to get the version of the Digital Specimen 20.5000.1025/testDS
at 2019-12-02T18:42:59.361Z the doip call should look like:
{
  "targetId": "20.5000.1025/testDS",
  "operationId": "getVersionAtGivenTime",
  "authentication": { "username": "YOUR_USERNAME", "password": "YOUR_PASSWORD" },
  "input": {
    "timestamp": "2019-12-02T18:42:59.361Z"
  }
}
#
#
*/
function getVersionAtGivenTime(object, context) {   
   var doec = getDigitalObjectEventController();
   var timestamp = context.params.timestamp;
   var version = doec.getVersionOfObjectAtGivenTime(object.id,timestamp);
   return version;    
}    


/*
Function that recreates the the digital object at a given time by lookig at the provenance record
For example, to get the Digital Specimen 20.5000.1025/testDS 
at 2019-12-02T18:42:59.361Z the doip call should look like:
{
  "targetId": "20.5000.1025/testDS",
  "operationId": "getObjectAtGivenTime",
  "authentication": { "username": "YOUR_USERNAME", "password": "YOUR_PASSWORD" },
  "input": {
    "timestamp": "2019-12-02T18:42:59.361Z"
  }
}
#
#
*/
function getObjectAtGivenTime(object, context) {   
   var doec = getDigitalObjectEventController();
   var timestamp = context.params.timestamp;
   var version = doec.getObjectAtGivenTime(object.id,timestamp);
   return version;    
}  

/*
Function that gets the list of provenance records of a given object
For example, to get provenance records for the Digital Specimen 20.5000.1025/testDS, the doip call should look like:
{
  "targetId": "20.5000.1025/testDS",
  "operationId": "getProvenanceRecords",
  "authentication": { "username": "francisco", "password": "fran1234" }
}
#
#
*/
function getProvenanceRecords(object, context) {
   var doec = getDigitalObjectEventController();
   var provenanceRecords = doec.getProvenanceRecordsForObject(object.id);
   object.provenanceRecords=JSON.parse(provenanceRecords);
   return object;    
}

/*
Function that publishes a version of the digital object and returns the id
For example, to publish the current version of the Digital Specimen 20.5000.1025/testDS, the doip call should look like:
{
  "targetId": "20.5000.1025/testDS",
  "operationId": "publishVersion",
  "authentication": { "username": "francisco", "password": "fran1234" }
}
#
#
*/
function publishVersion(object, context) {   
   var doec = getDigitalObjectEventController();
   var versionId = doec.publishVersion(object.id);
   return versionId;    
}    

</code></pre>


### 1.4 Configure the ACL for desired CORDRA's schema to allow calls to instance methods
<pre><code>
    "DigitalSpecimen": {
      "defaultAclRead": [
        "public"
      ],
      "defaultAclWrite": [
        "creator"
      ],
      "aclCreate": [
        "authenticated"
      ],
      "aclMethods": {
        "instance": {
          "processEvent": [
            "writers"
          ],
          "getVersionAtGivenTime": [
            "public"
          ],
          "getProvenanceRecords": [
            "public"
          ]
        },
        "default": {
          "instance": [
            "writers"
          ]
        }
      }
    }
</code></pre>


### Funding
This code was created to demonstrate how to process events done over digital objects, as part of the ICEDIG project 
https://icedig.eu/ ICEDIG a DiSSCo Project H2020-INFRADEV-2016-2017 – Grant Agreement No. 777483 Funded by the Horizon 
2020 Framework Programme of the European Union