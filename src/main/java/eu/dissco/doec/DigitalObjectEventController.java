package eu.dissco.doec;

import com.google.common.collect.MapDifference;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import eu.dissco.doec.digitalObjectRepository.DigitalObjectRepositoryClient;
import eu.dissco.doec.digitalObjectRepository.DigitalObjectRepositoryException;
import eu.dissco.doec.digitalObjectRepository.DigitalObjectRepositoryInfo;
import eu.dissco.doec.utils.FileUtils;
import eu.dissco.doec.utils.JsonUtils;
import net.dona.doip.client.DigitalObject;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/*
    object contains id, type, content, acl, metadata, and payloads

    context is an object with several useful properties.
        Property Name	Value
        isNew	Flag which is true for creations and false for modifications. Applies to beforeSchemaValidation.
                objectId	The id of the object.
                userId	The id of the user performing the operation.
        groups	A list of the ids of groups to which the user belongs.
                effectiveAcl	The computed ACLs for the object, either from the object itself or inherited from configuration. This is an object with readers and writers properties.
                aclCreate	The creation ACL for the type being created, in beforeSchemaValidation for a creation.
        newPayloads	A list of payload metadata for payloads being updated, in beforeSchemaValidation for an update operation.
                payloadsToDelete	A list of payload names of payloads being deleted, in beforeSchemaValidation for an update operation.
                params	The input supplied to a Type Methods call.
        requestContext	A user-suppled requestContext query parameter.
 */
public class DigitalObjectEventController {

    private Configuration config;

    protected Configuration getConfig() {
        return config;
    }

    public DigitalObjectEventController(String configFilePath) throws Exception{
        if (configFilePath!=null){
            this.config = FileUtils.loadConfigurationFromFilePath(configFilePath);
        } else{
            this.config = FileUtils.loadConfigurationFromResourceFile("config.properties");
        }
    }

    public void processCreateEvent(String strJsonObject, String strJsonContext) {
        DigitalObject digitalObject = this.getDigitalObjectFromString(strJsonObject);
        JsonObject context = this.getJsonObjectFromString(strJsonContext);

        Runnable saveProvenanceRecordForCreateEvent = () -> {
            DigitalObjectRepositoryInfo digitalObjectRepositoryInfo =  DigitalObjectRepositoryInfo.getDigitalObjectRepositoryInfoFromConfig(this.getConfig());
            DigitalObjectRepositoryInfo provenanceRepositoryInfo =  DigitalObjectRepositoryInfo.getProvenanceRepositoryInfoFromConfig(this.getConfig());
            try(DigitalObjectRepositoryClient digitalObjectRepositoryClient = new DigitalObjectRepositoryClient(digitalObjectRepositoryInfo);
                DigitalObjectRepositoryClient provenanceRepositoryClient = new DigitalObjectRepositoryClient(provenanceRepositoryInfo)){

                //Wait until object is stored in repository as the processCreateEvent is triggered on beforeSchemaValidation,
                //so it might not have been created in the repository yet
                TimeUnit.SECONDS.sleep(3);

                String metaQuery = "metadata/createdBy:" + digitalObjectRepositoryClient.escapeQueryParamValue(context.get("userId").getAsString());
                DigitalObject digitalObjectFound = digitalObjectRepositoryClient.searchForObject(digitalObject, metaQuery);
                if (digitalObjectFound != null) {
                    //Generate a revision for the object
                    String revisionId = ""; //this.publishRevision(digitalObjectFound);

                    String eventTypeId = provenanceRepositoryClient.searchOne("type:EventType AND /name:Insert").id;
                    String roleId = digitalObjectRepositoryClient.searchOne("type:Role AND /name:Scientist").id;

                    //Save provenance record of the event
                    DigitalObject provenanceRecord = new DigitalObject();
                    provenanceRecord.type = "EventProvenanceRecord";
                    JsonObject provenanceContent = new JsonObject();
                    provenanceContent.addProperty("eventTypeId", eventTypeId);
                    provenanceContent.addProperty("entityId", digitalObjectFound.id);
                    provenanceContent.addProperty("entityType", digitalObjectFound.type);
                    provenanceContent.addProperty("agentId", context.get("userId").getAsString());
                    provenanceContent.addProperty("roleId", roleId);
                    provenanceContent.addProperty("timestamp",  Instant.ofEpochMilli(digitalObjectFound.attributes.getAsJsonObject("metadata").get("createdOn").getAsLong()).toString());
                    provenanceContent.addProperty("description", "Digital object created");

                    JsonObject extraAttributes = new JsonObject();
                    extraAttributes.addProperty("revisionId", revisionId);
                    extraAttributes.add("entityContent", digitalObjectFound.attributes.getAsJsonObject("content"));
                    provenanceContent.add("data",extraAttributes);

                    provenanceRecord.setAttribute("content", provenanceContent);
                    DigitalObject provRecordSaved = provenanceRepositoryClient.create(provenanceRecord);
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        };

        new Thread(saveProvenanceRecordForCreateEvent).start();
    }

    public void processUpdateEvent(String strOriginalObject, String strModifiedObject, String strJsonContext) {
        DigitalObject originalDigitalObject = this.getDigitalObjectFromString(strOriginalObject);
        DigitalObject modifiedDigitalObject = this.getDigitalObjectFromString(strModifiedObject);
        JsonObject context = this.getJsonObjectFromString(strJsonContext);

        Runnable saveProvenanceRecordForUpdateEvent = () -> {
            DigitalObjectRepositoryInfo digitalObjectRepositoryInfo =  DigitalObjectRepositoryInfo.getDigitalObjectRepositoryInfoFromConfig(this.getConfig());
            DigitalObjectRepositoryInfo provenanceRepositoryInfo =  DigitalObjectRepositoryInfo.getProvenanceRepositoryInfoFromConfig(this.getConfig());
            try(DigitalObjectRepositoryClient digitalObjectRepositoryClient = new DigitalObjectRepositoryClient(digitalObjectRepositoryInfo);
                DigitalObjectRepositoryClient provenanceRepositoryClient = new DigitalObjectRepositoryClient(provenanceRepositoryInfo)){

                //Wait until object is stored in repository as the processUpdateEvent is triggered on beforeSchemaValidation,
                //so it might not have been updated in the repository yet
                TimeUnit.SECONDS.sleep(3);
                Long endEpoch = Instant.now().toEpochMilli();
                Long startEpoch = endEpoch - (5*1000);

                String query = "type:"+ modifiedDigitalObject.type +
                        " AND /id:"+ digitalObjectRepositoryClient.escapeQueryParamValue(modifiedDigitalObject.id) +
                        " AND metadata/modifiedBy:" + digitalObjectRepositoryClient.escapeQueryParamValue(context.get("userId").getAsString()) +
                        " AND metadata/modifiedOn:[" + Long.toString(startEpoch) + " TO " + Long.toString(endEpoch) + "]";
                List<DigitalObject> digitalObjectList = digitalObjectRepositoryClient.searchAll(query);
                if (digitalObjectList.size() == 1) {
                    //Generate a revision for the object
                    DigitalObject digitalObjectFound = digitalObjectList.get(0);
                    String revisionId = ""; //this.publishRevision(digitalObjectFound);

                    String eventTypeId = provenanceRepositoryClient.searchOne("type:EventType AND /name:Update").id;
                    String roleId = digitalObjectRepositoryClient.searchOne("type:Role AND /name:Scientist").id;

                    //Save provenance record of the event
                    MapDifference<String, Object> mapDifference = digitalObjectRepositoryClient.compareContentDigitalObjects(digitalObjectFound,originalDigitalObject);
                    JsonObject comparisonResult = (JsonObject)JsonUtils.convertObjectToJsonElement(mapDifference);
                    comparisonResult.remove("onBoth");
                    JsonObject extraAttributes = new JsonObject();
                    extraAttributes.add("changes",comparisonResult);
                    extraAttributes.addProperty("revisionId", revisionId);
                    extraAttributes.add("entityContent", digitalObjectFound.attributes.getAsJsonObject("content"));

                    DigitalObject provenanceRecord = new DigitalObject();
                    provenanceRecord.type = "EventProvenanceRecord";
                    JsonObject provenanceContent = new JsonObject();
                    provenanceContent.addProperty("eventTypeId",eventTypeId);
                    provenanceContent.addProperty("entityId", digitalObjectFound.id);
                    provenanceContent.addProperty("entityType", digitalObjectFound.type);
                    provenanceContent.addProperty("agentId", context.get("userId").getAsString());
                    provenanceContent.addProperty("roleId", roleId);
                    provenanceContent.addProperty("timestamp",  Instant.ofEpochMilli(digitalObjectFound.attributes.getAsJsonObject("metadata").get("modifiedOn").getAsLong()).toString());
                    provenanceContent.addProperty("description","Digital object updated");
                    provenanceContent.add("data",extraAttributes);

                    provenanceRecord.setAttribute("content", provenanceContent);
                    DigitalObject provRecordSaved = provenanceRepositoryClient.create(provenanceRecord);
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        };
        new Thread(saveProvenanceRecordForUpdateEvent).start();
    }

    public void processDeleteEvent(String strJsonObject, String strJsonContext) throws DigitalObjectRepositoryException {
        DigitalObject digitalObject = this.getDigitalObjectFromString(strJsonObject);
        JsonObject context = this.getJsonObjectFromString(strJsonContext);

        Runnable saveProvenanceUpdateEvent = () -> {
            DigitalObjectRepositoryInfo digitalObjectRepositoryInfo =  DigitalObjectRepositoryInfo.getDigitalObjectRepositoryInfoFromConfig(this.getConfig());
            DigitalObjectRepositoryInfo provenanceRepositoryInfo =  DigitalObjectRepositoryInfo.getProvenanceRepositoryInfoFromConfig(this.getConfig());
            try(DigitalObjectRepositoryClient digitalObjectRepositoryClient = new DigitalObjectRepositoryClient(digitalObjectRepositoryInfo);
                DigitalObjectRepositoryClient provenanceRepositoryClient = new DigitalObjectRepositoryClient(provenanceRepositoryInfo)){

                Long deleteTimestamp = Instant.now().toEpochMilli();

                //Wait until object is deleted in repository as the processDeleteEvent is triggered on beforeDelete,
                //so it might not have been deleted in the repository yet
                TimeUnit.SECONDS.sleep(3);

                DigitalObject digitalObjectFound = digitalObjectRepositoryClient.retrieve(digitalObject.id);
                if (digitalObjectFound == null) {
                    String eventTypeId = provenanceRepositoryClient.searchOne("type:EventType AND /name:Delete").id;
                    String roleId = digitalObjectRepositoryClient.searchOne("type:Role AND /name:Scientist").id;


                    DigitalObject provenanceRecord = new DigitalObject();
                    provenanceRecord.type = "EventProvenanceRecord";
                    JsonObject provenanceContent = new JsonObject();
                    provenanceContent.addProperty("eventTypeId",eventTypeId);
                    provenanceContent.addProperty("entityId",context.get("objectId").getAsString());
                    provenanceContent.addProperty("entityType", digitalObject.type);
                    provenanceContent.addProperty("agentId",context.get("userId").getAsString());
                    provenanceContent.addProperty("roleId",roleId);
                    provenanceContent.addProperty("timestamp",  Instant.ofEpochMilli(deleteTimestamp).toString());
                    provenanceContent.addProperty("description","Digital object deleted");

                    provenanceRecord.setAttribute("content", provenanceContent);
                    DigitalObject provRecordSaved = provenanceRepositoryClient.create(provenanceRecord);
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        };
        new Thread(saveProvenanceUpdateEvent).start();
    }

    public void processRetrieveEvent(String strJsonObject, String strJsonContext) throws DigitalObjectRepositoryException {
        DigitalObject digitalObject = this.getDigitalObjectFromString(strJsonObject);
        JsonObject context = this.getJsonObjectFromString(strJsonContext);

        DigitalObjectRepositoryInfo digitalObjectRepositoryInfo =  DigitalObjectRepositoryInfo.getDigitalObjectRepositoryInfoFromConfig(this.getConfig());
        DigitalObjectRepositoryInfo provenanceRepositoryInfo =  DigitalObjectRepositoryInfo.getProvenanceRepositoryInfoFromConfig(this.getConfig());
        try(DigitalObjectRepositoryClient digitalObjectRepositoryClient = new DigitalObjectRepositoryClient(digitalObjectRepositoryInfo);
            DigitalObjectRepositoryClient provenanceRepositoryClient = new DigitalObjectRepositoryClient(provenanceRepositoryInfo)){

            String eventTypeId = provenanceRepositoryClient.searchOne("type:EventType AND /name:Retrieve").id;
            String roleId = digitalObjectRepositoryClient.searchOne("type:Role AND /name:Scientist").id;

            Long retrieveTimestamp = Instant.now().toEpochMilli();
            DigitalObject provenanceRecord = new DigitalObject();
            provenanceRecord.type = "EventProvenanceRecord";
            JsonObject provenanceContent = new JsonObject();
            provenanceContent.addProperty("eventTypeId",eventTypeId);
            provenanceContent.addProperty("entityId",context.get("objectId").getAsString());
            provenanceContent.addProperty("entityType", digitalObject.type);
            provenanceContent.addProperty("agentId",context.get("userId").getAsString());
            provenanceContent.addProperty("roleId",roleId);
            provenanceContent.addProperty("timestamp",Instant.now().toString());
            provenanceContent.addProperty("description","Digital object retrieved");

            provenanceRecord.setAttribute("content", provenanceContent);
            DigitalObject provRecordSaved = provenanceRepositoryClient.create(provenanceRecord);
        }
    }

    public void processCustomEvent(String strJsonEvent, String objectId) throws DigitalObjectRepositoryException{
        JsonObject jsonEvent = this.getJsonObjectFromString(strJsonEvent);
        if (!jsonEvent.has("eventTypeId") || !jsonEvent.has("agentId") || !jsonEvent.has("timestamp")){
            throw new DigitalObjectRepositoryException("The event can't be processed as some as it missing some required attributes: " +
                    "eventTypeName, agentName, timestamp");
        }

        DigitalObjectRepositoryInfo digitalObjectRepositoryInfo =  DigitalObjectRepositoryInfo.getDigitalObjectRepositoryInfoFromConfig(this.getConfig());
        DigitalObjectRepositoryInfo provenanceRepositoryInfo =  DigitalObjectRepositoryInfo.getProvenanceRepositoryInfoFromConfig(this.getConfig());
        try(DigitalObjectRepositoryClient digitalObjectRepositoryClient = new DigitalObjectRepositoryClient(digitalObjectRepositoryInfo);
            DigitalObjectRepositoryClient provenanceRepositoryClient = new DigitalObjectRepositoryClient(provenanceRepositoryInfo)){

            DigitalObject entity = digitalObjectRepositoryClient.retrieve(objectId);
            DigitalObject agent = digitalObjectRepositoryClient.retrieve(jsonEvent.get("agentId").getAsString());
            DigitalObject eventType = provenanceRepositoryClient.searchOne("type:EventType AND /name:" + jsonEvent.get("eventType").getAsString());

            if (entity!=null && eventType!=null && agent!=null){
                DigitalObject provenanceRecord = new DigitalObject();
                provenanceRecord.type = "EventProvenanceRecord";
                JsonObject provenanceContent = new JsonObject();
                provenanceContent.addProperty("eventTypeId",eventType.id);
                provenanceContent.addProperty("entityId",entity.id);
                provenanceContent.addProperty("entityType", entity.type);
                provenanceContent.addProperty("agentId",agent.id);
                provenanceContent.addProperty("timestamp",jsonEvent.get("timestamp").getAsString());
                if (jsonEvent.has("role") && StringUtils.isNotBlank(jsonEvent.get("role").getAsString())){
                    DigitalObject role = digitalObjectRepositoryClient.searchOne("type:Role AND /name:" + jsonEvent.get("role").getAsString());
                    if (role!=null){
                        provenanceContent.addProperty("roleId",role.id);
                    }
                }
                if (jsonEvent.has("description") && StringUtils.isNotBlank(jsonEvent.get("description").getAsString())){
                    provenanceContent.addProperty("description",jsonEvent.get("description").getAsString());
                }

                if (jsonEvent.has("data") && !jsonEvent.getAsJsonObject("data").isJsonNull() &&
                        jsonEvent.getAsJsonObject("data").keySet().size()>0){
                    provenanceContent.add("data",jsonEvent.getAsJsonObject("data"));
                }

                if (!JsonUtils.validateJsonAgainstSchema(provenanceContent.getAsJsonObject("data"),eventType.attributes.getAsJsonObject("content").getAsJsonObject("additionalDataSchema"),false)){
                    throw new DigitalObjectRepositoryException("The event can't be processed as its additional data doesn't validate against the event type");
                }

                provenanceRecord.setAttribute("content", provenanceContent);
                DigitalObject dobjSaved = provenanceRepositoryClient.create(provenanceRecord);
            } else{
                throw new DigitalObjectRepositoryException("The event can't be processed as some of its required attributes are not found in the system");
            }
        }
    }

    /**
     * Function that returns the published version of the requested object at the desired time
     * Please note that this is not looking at provenance but only at published versions
     * @param objectId
     * @param utcIsoDatetime
     * @return
     * @throws DigitalObjectRepositoryException
     */
    public DigitalObject getVersionOfObjectAtGivenTime(String objectId, String utcIsoDatetime) throws DigitalObjectRepositoryException {
        DigitalObjectRepositoryInfo digitalObjectRepositoryInfo = DigitalObjectRepositoryInfo.getDigitalObjectRepositoryInfoFromConfig(this.getConfig());
        try (DigitalObjectRepositoryClient digitalObjectRepositoryClient = new DigitalObjectRepositoryClient(digitalObjectRepositoryInfo)) {
            DigitalObject version = digitalObjectRepositoryClient.getVersionOfObjectAtGivenTime(objectId,utcIsoDatetime);
            if (version!=null){
                version.id = objectId;
            }
            return version;
        }
    }

    /**
     * Function that returns the requested object at the desired time.
     * Please note that this is obtained by looking at the provenance records of this object for events Insert, Update and Delete
     * @param objectId
     * @param utcIsoDatetime
     * @return
     * @throws DigitalObjectRepositoryException
     */
    public DigitalObject getObjectAtGivenTime(String objectId, String utcIsoDatetime) throws DigitalObjectRepositoryException {
        DigitalObjectRepositoryInfo digitalObjectRepositoryInfo =  DigitalObjectRepositoryInfo.getDigitalObjectRepositoryInfoFromConfig(this.getConfig());
        DigitalObjectRepositoryInfo provenanceRepositoryInfo =  DigitalObjectRepositoryInfo.getProvenanceRepositoryInfoFromConfig(this.getConfig());
        try(DigitalObjectRepositoryClient digitalObjectRepositoryClient = new DigitalObjectRepositoryClient(digitalObjectRepositoryInfo);
            DigitalObjectRepositoryClient provenanceRepositoryClient = new DigitalObjectRepositoryClient(provenanceRepositoryInfo)){

            DigitalObject digitalObject = digitalObjectRepositoryClient.retrieve(objectId);
            if (digitalObject!=null){
                String query = "type:EventProvenanceRecord " +
                        "AND (/eventTypeId:EventTypeInsert OR /eventTypeId:EventTypeUpdate OR /eventTypeId:EventTypeDelete) " +
                        "AND /entityId:" + provenanceRepositoryClient.escapeQueryParamValue(objectId);

                List<DigitalObject> provenanceRecords = provenanceRepositoryClient.searchAll(query);
                Comparator<DigitalObject> compareByTimestamp = (DigitalObject o1, DigitalObject o2) ->
                        o1.attributes.getAsJsonObject("content").get("timestamp").getAsString().compareTo( o2.attributes.getAsJsonObject("content").get("timestamp").getAsString() );

                provenanceRecords = provenanceRecords
                        .stream()
                        .filter(provenanceRecord ->
                                provenanceRecord.attributes.getAsJsonObject("content").get("timestamp").getAsString().compareTo(utcIsoDatetime)<=0)
                        .sorted(compareByTimestamp.reversed())
                        .collect(Collectors.toList());

                if (provenanceRecords.size()==0 || StringUtils.containsIgnoreCase(provenanceRecords.get(0).attributes.getAsJsonObject("content").get("eventTypeId").getAsString(),"EventTypeDelete")){
                    digitalObject=null;
                } else{
                    digitalObject.setAttribute("content",provenanceRecords.get(0).attributes.getAsJsonObject("content").getAsJsonObject("data").getAsJsonObject("entityContent"));

                    JsonObject metadata = digitalObject.attributes.getAsJsonObject("metadata");
                    metadata.addProperty("modifiedOn",provenanceRecords.get(0).attributes.getAsJsonObject("metadata").get("modifiedOn").getAsLong());
                    metadata.addProperty("modifiedBy",provenanceRecords.get(0).attributes.getAsJsonObject("content").get("agentId").getAsString());
                    digitalObject.setAttribute("metadata",metadata);
                }
            }
            return digitalObject;
        }
    }

    /**
     * Function that generate a revision for the digital object id received as parameter
     * @param objectId
     * @return id of the revision generated
     */
    public String publishVersion(String objectId) throws DigitalObjectRepositoryException {
        DigitalObjectRepositoryInfo digitalObjectRepositoryInfo =  DigitalObjectRepositoryInfo.getDigitalObjectRepositoryInfoFromConfig(this.getConfig());
        try(DigitalObjectRepositoryClient digitalObjectRepositoryClient = new DigitalObjectRepositoryClient(digitalObjectRepositoryInfo);){
            DigitalObject version = digitalObjectRepositoryClient.publishVersion(objectId);
            return version.id;
        }
    }

    public String getProvenanceRecordsForObject(String objectId) throws DigitalObjectRepositoryException {
        DigitalObjectRepositoryInfo provenanceRepositoryInfo =  DigitalObjectRepositoryInfo.getProvenanceRepositoryInfoFromConfig(this.getConfig());
        try(DigitalObjectRepositoryClient provenanceRepositoryClient = new DigitalObjectRepositoryClient(provenanceRepositoryInfo)){
            String query = "type:EventProvenanceRecord AND /entityId:" + provenanceRepositoryClient.escapeQueryParamValue(objectId);
            List<DigitalObject> provenanceRecords = provenanceRepositoryClient.searchAll(query);
            return JsonUtils.serializeObject(provenanceRecords.stream().toArray(DigitalObject[]::new));
        }
    }


    /**
     * Function that generate a revision for the digital object received as parameter
     * @param digitalObject
     * @return id of the revision generated
     */
    private String publishRevision(DigitalObject digitalObject) throws DigitalObjectRepositoryException {
        DigitalObjectRepositoryInfo digitalObjectRepositoryInfo =  DigitalObjectRepositoryInfo.getDigitalObjectRepositoryInfoFromConfig(this.getConfig());
        try(DigitalObjectRepositoryClient digitalObjectRepositoryClient = new DigitalObjectRepositoryClient(digitalObjectRepositoryInfo);){
            DigitalObject version = digitalObjectRepositoryClient.publishVersion(digitalObject.id);
            return version.id;
        }
    }

    private DigitalObject getDigitalObjectFromString(String strJsonObject){
        Gson gson = new Gson();
        JsonObject object = gson.fromJson(strJsonObject, JsonObject.class);
        JsonObject dobjContent = object.getAsJsonObject("content");
        DigitalObject digitalObject = new DigitalObject();
        digitalObject.type=object.get("type").getAsString();
        if (object.has("id")){
            digitalObject.id=object.get("id").getAsString();
        }
        digitalObject.setAttribute("content",dobjContent);
        return digitalObject;
    }

    private JsonObject getJsonObjectFromString(String strJson){
        Gson gson = new Gson();
        return gson.fromJson(strJson, JsonObject.class);
    }

    public static void main(String[] args) throws Exception {
        DigitalObjectEventController doec = new DigitalObjectEventController(null);
        System.out.println("DiSSCo Digital Object Controller");
    }
}
