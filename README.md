# Disco Digital Object Controller #

## Version 1.0.0 ##

This java project provides a simple mechanism to process events over digital objects, keeping a provenance
record of those events

## 1. Getting Started

### 1.1. Generate the jar file of the project with dependencies
mvn package

### 1.2 Deploy jar file to CORDRA object repository
In order to be able to process events fired automatically when digital object is created, received, updated and/or deleted 
in CORDRA, we need to deploy this jar file inside the CORDRA's data/lib directory and restart CORDRA.
  

### 1.3 Configure the desired CORDRA's schema to record provenance for its instances 
<pre><code>
var cordra = require('cordra');

exports.beforeSchemaValidation = beforeSchemaValidation;
exports.objectForIndexing = objectForIndexing;
exports.onObjectResolution = onObjectResolution;
exports.beforeDelete = beforeDelete;

exports.methods = {};
exports.methods.processEvent = processEvent;
exports.methods.getVersionAtGivenTime = getVersionAtGivenTime;
exports.methods.getProvenanceRecords = getProvenanceRecords;


function beforeSchemaValidation(object, context) {
   var DigitalObjectEventController = Java.type("eu.dissco.doec.DigitalObjectEventController");
   var doec = new DigitalObjectEventController();

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
   var DigitalObjectEventController = Java.type("eu.dissco.doec.DigitalObjectEventController");
   var doec = new DigitalObjectEventController();
   //doec.processRetrieveEvent(JSON.stringify(object),JSON.stringify(context));    
   return object;
}

function beforeDelete(object, context) {
   var DigitalObjectEventController = Java.type("eu.dissco.doec.DigitalObjectEventController");
   var doec = new DigitalObjectEventController();
   doec.processDeleteEvent(JSON.stringify(object),JSON.stringify(context));   
}

/*
Function to process a custom event over a digital object
For example, to process the event DepositInMuseum for the Digital Specimen 20.5000.1025/c4942d87a9f89d8929c1
the doip call should look like:
{
  "targetId": "20.5000.1025/c4942d87a9f89d8929c1",
  "operationId": "processEvent",
  "authentication": { "username": "YOUR_USERNAME", "password": "YOUR_PASSWORD" },
  "input": {
    "eventTypeId":"prov.994/46b7c3b13faa76b5af0f",
    "agentId":"20.5000.1025/d298a8c18cb62ee602b8",
    "roleId":"20.5000.1025/808d7dca8a74d84af27a",
    "timestamp": "1575291173882",
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
   var event = JSON.stringify(context.params);
   var DigitalObjectEventController = Java.type("eu.dissco.doec.DigitalObjectEventController");
   var doec = new DigitalObjectEventController();
   doec.processCustomEvent(event,object.id);
   return object;
}


/*
Function that get the version of a digital object at a given time
For example, to get the version of the Digital Specimen 20.5000.1025/c4942d87a9f89d8929c1
at 2019-12-02T18:42:59.361Z[Europe/London] the doip call should look like:
{
  "targetId": "20.5000.1025/c4942d87a9f89d8929c1",
  "operationId": "getVersionAtGivenTime",
  "authentication": { "username": "YOUR_USERNAME", "password": "YOUR_PASSWORD" },
  "input": {
    "timestamp": "1575312179361"
  }
}
#
#
*/
function getVersionAtGivenTime(object, context) {
   var timestamp = context.params.timestamp;
   var DigitalObjectEventController = Java.type("eu.dissco.doec.DigitalObjectEventController");
   var doec = new DigitalObjectEventController();
   var version = doec.getVersionOfObjectAtGivenTime(object.id,timestamp);
   return version;    
}    


function getProvenanceRecords(object, context) {
   var DigitalObjectEventController = Java.type("eu.dissco.doec.DigitalObjectEventController");
   var doec = new DigitalObjectEventController();
   var provenanceRecords = doec.getProvenanceRecordsForObject(object.id);
   object.provenanceRecords=JSON.parse(provenanceRecords);
   return object;    
}
</code></pre>