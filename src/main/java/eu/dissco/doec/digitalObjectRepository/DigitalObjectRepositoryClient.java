package eu.dissco.doec.digitalObjectRepository;

import com.google.common.collect.MapDifference;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.dissco.doec.utils.JsonUtils;
import net.cnri.cordra.api.CordraClient;
import net.cnri.cordra.api.CordraException;
import net.cnri.cordra.api.HttpCordraClient;
import net.cnri.cordra.api.VersionInfo;
import net.dona.doip.DoipRequestHeaders;
import net.dona.doip.InDoipMessage;
import net.dona.doip.client.*;
import net.dona.doip.client.transport.DoipClientResponse;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public class DigitalObjectRepositoryClient implements AutoCloseable {

    /**************/
    /* ATTRIBUTES */
    /**************/

    private final DigitalObjectRepositoryInfo digitalObjectRepositoryInfo;
    private final DoipClient doipClient;
    private final CordraClient restClient;
    private final AuthenticationInfo authInfo;
    private final ServiceInfo serviceInfo;


    /**************/
    /* ENUM TYPES */
    /**************/

    public enum DIGITAL_OBJECT_OPERATION {
        INSERT,
        UPDATE,
        DELETE
    }


    /***********************/
    /* GETTERS AND SETTERS */
    /***********************/

    protected DigitalObjectRepositoryInfo getDigitalObjectRepositoryInfo() {
        return digitalObjectRepositoryInfo;
    }

    protected DoipClient getDoipClient() {
        return doipClient;
    }

    protected CordraClient getRestClient() {
        return restClient;
    }

    protected AuthenticationInfo getAuthInfo() {
        return authInfo;
    }

    protected ServiceInfo getServiceInfo() {
        return serviceInfo;
    }


    /****************/
    /* CONSTRUCTORS */
    /****************/

    /**
     *  Create a new DigitalObjectRepositoryClient
     * @param digitalObjectRepositoryInfo
     * @throws DigitalObjectRepositoryException
     */
    public DigitalObjectRepositoryClient(DigitalObjectRepositoryInfo digitalObjectRepositoryInfo) throws DigitalObjectRepositoryException {
        try{
            this.digitalObjectRepositoryInfo=digitalObjectRepositoryInfo;
            this.doipClient=new DoipClient();
            this.authInfo= new PasswordAuthenticationInfo(digitalObjectRepositoryInfo.getUsername(), digitalObjectRepositoryInfo.getPassword());
            this.serviceInfo = new ServiceInfo(digitalObjectRepositoryInfo.getServiceId(), digitalObjectRepositoryInfo.getHostAddress(), digitalObjectRepositoryInfo.getDoipPort());
            this.restClient = new HttpCordraClient(digitalObjectRepositoryInfo.getUrl(),digitalObjectRepositoryInfo.getUsername(),digitalObjectRepositoryInfo.getPassword());
        } catch (Exception e){
            throw new DigitalObjectRepositoryException("Error setting up DigitalObjectRepositoryClient " + e.getMessage(),e);
        }
    }


    /*******************/
    /* PUBLIC METHODS */
    /******************/

    /***
     * Function that get the list of version of a given object
     * Note: This function use the CORDRA REST API as this functionality is not provided in DOIP yet
     * @param objectId
     * @return List of versions (digital objects) of a given object. The list of versions is sorted from the oldest to the most recent
     * @throws DigitalObjectRepositoryException
     */
    public List<DigitalObject> getVersionsOfObject(String objectId) throws DigitalObjectRepositoryException{
        try {
            List<DigitalObject> listDigitalObjects = null;
            List<VersionInfo> versions = this.getRestClient().getVersionsFor(objectId);
            if (versions!=null && versions.size()>0){
                versions.sort(Comparator.comparing(v -> v.publishedOn, Comparator.nullsLast(Long::compareTo)));
                listDigitalObjects = new ArrayList<>();
                DigitalObject previousVersion = null;
                for (VersionInfo version:versions) {
                    DigitalObject digitalObject = this.retrieve(version.id);
                    if (previousVersion!=null){
                        MapDifference<String, Object> mapDifference = this.compareContentDigitalObjects(previousVersion,digitalObject);
                        JsonObject comparisonResult = (JsonObject)JsonUtils.convertObjectToJsonElement(mapDifference);
                        comparisonResult.remove("onBoth");
                        digitalObject.attributes.add("comparisonAgainstPreviousVersion",JsonUtils.convertObjectToJsonElement(comparisonResult));
                    }
                    previousVersion=digitalObject;
                    listDigitalObjects.add(digitalObject);
                }
            }
            return listDigitalObjects;
        } catch (CordraException e) {
            throw DigitalObjectRepositoryException.convertCordraException(e);
        }
    }

    /***
     * Function that get the object as it was the the time specified
     * Note: This function use the CORDRA REST API as this functionality is not provided in DOIP yet
     * @param objectId
     * @param zonedDateTime datetime from when we want to retrieve the status of the digital object
     * @return List of versions (digital objects) of a given object
     * @throws DigitalObjectRepositoryException
     */
    public DigitalObject getVersionOfObjectAtGivenTime(String objectId, ZonedDateTime zonedDateTime) throws DigitalObjectRepositoryException{
        if (zonedDateTime==null){
            zonedDateTime = LocalDateTime.now().atZone(ZoneId.systemDefault());
        }
        Long datetimeEpoch = zonedDateTime.toInstant().toEpochMilli();
        return this.getVersionOfObjectAtGivenTime(objectId,datetimeEpoch);
    }

    /**
     * Function that get the object as it was the the time specified
     * @param objectId
     * @param utcDatetime string of utc time in ISO 8601
     * @return Version of the object at the given time
     * @throws DigitalObjectRepositoryException
     */
    public DigitalObject getVersionOfObjectAtGivenTime(String objectId, String utcDatetime) throws DigitalObjectRepositoryException {
        Long timestamp;
        if (StringUtils.isNotBlank(utcDatetime)){
            timestamp = Instant.parse(utcDatetime).toEpochMilli();
        } else{
            timestamp = Instant.now().toEpochMilli();
        }
        return this.getVersionOfObjectAtGivenTime(objectId,timestamp);
    }


    /***
     * Function that get the object as it was the the time specified
     * Note: This function use the CORDRA REST API as this functionality is not provided in DOIP yet
     * @param objectId
     * @param datetimeEpoch datetime from when we want to retrieve the status of the digital object
     * @return List of versions (digital objects) of a given object
     * @throws DigitalObjectRepositoryException
     */
    public DigitalObject getVersionOfObjectAtGivenTime(String objectId, Long datetimeEpoch) throws DigitalObjectRepositoryException{
        try {
            DigitalObject digitalObjectAtGivenTime = null;
            List<VersionInfo> versions = this.getRestClient().getVersionsFor(objectId);

            if (versions!=null && versions.size()>0){
                //The list of versions is sorted from the oldest to the most recent
                versions.sort(Comparator.comparing(v -> v.publishedOn, Comparator.nullsLast(Long::compareTo)));
                int versionPos=-1;
                for (VersionInfo version:versions) {
                    if (version.publishedOn==null){
                        version.publishedOn = Instant.now().toEpochMilli();
                    }

                    if (version.publishedOn>datetimeEpoch){
                        break;
                    } else{
                        versionPos++;
                    }
                }

                if (versionPos!=-1){
                    digitalObjectAtGivenTime = this.retrieve(versions.get(versionPos).id);
                } else{
                    //The search date is before the first version was created. Although it is still possible that the object
                    // existed at that time, as we only create the version just before the first modification is done
                    DigitalObject firstVersionObject = this.retrieve(versions.get(0).id);
                    if (firstVersionObject.attributes.getAsJsonObject("metadata").get("createdOn").getAsLong()<=datetimeEpoch){
                        digitalObjectAtGivenTime = firstVersionObject;
                    }
                }

                if (digitalObjectAtGivenTime!=null){
                    //Calculate differences with current version
                    if (!digitalObjectAtGivenTime.id.equalsIgnoreCase(objectId)){
                        DigitalObject currentObject = this.retrieve(objectId);
                        MapDifference<String, Object> mapDifference =  this.compareContentDigitalObjects(digitalObjectAtGivenTime,currentObject);
                        JsonObject comparisonResult = (JsonObject)JsonUtils.convertObjectToJsonElement(mapDifference);
                        comparisonResult.remove("onBoth");
                        digitalObjectAtGivenTime.attributes.add("comparisonAgainstCurrentVersion",JsonUtils.convertObjectToJsonElement(comparisonResult));
                    }
                }
            }
            return digitalObjectAtGivenTime;
        } catch (CordraException e) {
            throw DigitalObjectRepositoryException.convertCordraException(e);
        }
    }

    /**
     * Function that creates a version of the digital object id received as parameter
     * Note: Note: This function use the CORDRA REST API as this functionality is not provided in DOIP yet
     * @param objectId
     * @return Digital object resulting of the versioning
     * @throws DigitalObjectRepositoryException
     */
    public DigitalObject publishVersion(String objectId) throws DigitalObjectRepositoryException {
        try {
            DigitalObject digitalObject=null;
            VersionInfo version = this.getRestClient().publishVersion(objectId,null,false);
            if (version!=null) digitalObject = this.retrieve(version.id);
            return digitalObject;
        } catch (CordraException e) {
            throw DigitalObjectRepositoryException.convertCordraException(e);
        }
    }

    /***
     * Function that returns a list with all digital objects in the repository that satisfy the query criteria
     * @param query query using Lucene Query Syntax https://lucene.apache.org/core/2_9_4/queryparsersyntax.html
     *              Make sure characters are escaped similarly to the following example
     *              String query = "type:Schema AND /name:" + escapeQueryParamValue(name);
     * @return List of digital object that match search criteria
     * @throws DigitalObjectRepositoryException
     */
    public List<DigitalObject> searchAll(String query) throws DigitalObjectRepositoryException{
        return searchAll(query,0,this.getDigitalObjectRepositoryInfo().getPageSize());
    }

    public DigitalObject searchOne(String query) throws DigitalObjectRepositoryException{
        List<DigitalObject> searchResults = this.searchAll(query);
        if (searchResults.size()==1) {
            return searchResults.get(0);
        } else {
            return null;
        }
    }

    /***
     * Function to call the hello operation of the repository
     * @return Digital object with the hello response from the repository
     * @throws DigitalObjectRepositoryException
     */
    public DigitalObject hello() throws DigitalObjectRepositoryException {
        return this.hello(this.getDigitalObjectRepositoryInfo().getServiceId());
    }

    /***
     * Funtion that list the operations available in the digital repository
     * @return list the operations available in the digital repository
     * @throws DigitalObjectRepositoryException
     */
    public  List<String> listOperations() throws DigitalObjectRepositoryException {
        return this.listOperations(this.getDigitalObjectRepositoryInfo().getServiceId());
    }

    /**
     * Function that get the difference in the content of 2 digital specimens
     * Note: it removes their "id" for comparison
     * @param leftDobj left digital specimen
     * @param rightDobj left digital specimen
     * @return MapDifference with the result of the comparison
     */
    public MapDifference<String, Object> compareContentDigitalObjects(DigitalObject leftDobj, DigitalObject rightDobj){
        JsonObject leftDsContent = leftDobj.attributes.getAsJsonObject("content");
        JsonObject rightDsContent = rightDobj.attributes.getAsJsonObject("content");
        //Exclude DS ids for comparison
        String leftDsId = leftDsContent.has("id")?leftDsContent.get("id").getAsString():null;
        String rightDsId = rightDsContent.has("id")?rightDsContent.get("id").getAsString():null;
        leftDsContent.remove("id");
        rightDsContent.remove("id");
        MapDifference<String, Object> comparisonResult = JsonUtils.compareJsonElements(leftDsContent,rightDsContent);
        //Add the DS ids back to the content
        if (StringUtils.isNotBlank(leftDsId)) leftDsContent.addProperty("id",leftDsId);
        if (StringUtils.isNotBlank(rightDsId)) rightDsContent.addProperty("id",rightDsId);
        return comparisonResult;
    }

    public DigitalObject searchForObject(DigitalObject digitalObject, String metaQuery) throws DigitalObjectRepositoryException {
        DigitalObject dobj=null;
        StringBuilder sb = new StringBuilder();
        sb.append("type:"+digitalObject.type);
        sb.append(" AND " + metaQuery);

        Set<Map.Entry<String, JsonElement>> contentAttributes = digitalObject.attributes.get("content").getAsJsonObject().entrySet();
        for (Map.Entry<String, JsonElement> contentAttribute:contentAttributes) {
            JsonElement value = contentAttribute.getValue();
            if (value.isJsonPrimitive() && StringUtils.isNotBlank(value.getAsString())) {
                sb.append(" AND /"+contentAttribute.getKey()+":"+this.escapeQueryParamValue(value.getAsString()));
            }
        }
        String query=sb.toString();

        List<DigitalObject> listDigitalObjects = this.searchAll(query);
        if (listDigitalObjects.size()==1){
            dobj=listDigitalObjects.get(0);
        }
        return dobj;
    }

    /**
     * Function to escape a query param value
     * @param paramValue value to escape
     * @return value escaped
     */
    public String escapeQueryParamValue(String paramValue){
        StringBuilder sb = new StringBuilder();
        sb.append("\"");
        for(int i = 0; i < paramValue.length(); ++i) {
            char c = paramValue.charAt(i);
            if (c == '\\' || c == '+' || c == '-' || c == '!' || c == '(' || c == ')' || c == ':' || c == '^'
                    || c == '[' || c == ']' || c == '"' || c == '{' || c == '}' || c == '~' || c == '*' || c == '?'
                    || c == '|' || c == '&' || c == '/') {
                sb.append('\\');
            }
            sb.append(c);
        }
        sb.append("\"");
        return sb.toString();
    }


    /********************/
    /* PRIVATE METHODS */
    /*******************/

    /**
     * Function that get all entries that match the query. It using recursion to iterate through all the pages
     * returned by the Digital Object repository
     * @param query query to do the search
     * @param pageNumber page to get it
     * @param pageSize number of element to get per page
     * @return All digital objects in the repository entries that match the query
     * @throws DigitalObjectRepositoryException
     */
    private List<DigitalObject> searchAll(String query, Integer pageNumber, Integer pageSize) throws DigitalObjectRepositoryException{
        List<DigitalObject> results = new ArrayList<DigitalObject>();
        QueryParams queryParams = new QueryParams(pageNumber, pageSize);
        String digitalObjectRepositoryServiceId = this.getDigitalObjectRepositoryInfo().getServiceId();
        SearchResults<DigitalObject> searchResults = this.search(digitalObjectRepositoryServiceId,query, queryParams);
        searchResults.iterator().forEachRemaining(results::add);
        if (results.size()==pageSize){
            results.addAll(searchAll(query,++pageNumber,pageSize));
        }
        return results;
    }


    /*****************************************************************************************************************/
    /* Methods to act as facade for DOIP client in order to avoid passing all the times the authInfo and serviceInfo */
    /*****************************************************************************************************************/

    /**
     * Function that release the resource taken by the digital object repository client
     */
    public synchronized void close() {
        this.getDoipClient().close();
    }


    public DoipClientResponse performOperation(String targetId, String operationId, JsonObject attributes) throws DigitalObjectRepositoryException {
        try{
            return this.getDoipClient().performOperation(targetId,operationId,this.getAuthInfo(),attributes,this.getServiceInfo());
        } catch (DoipException e){
            throw DigitalObjectRepositoryException.convertDoipException(e);
        }

    }

    public DoipClientResponse performOperation(String targetId, String operationId, JsonObject attributes, JsonElement input) throws DigitalObjectRepositoryException {
        try{
            return this.getDoipClient().performOperation(targetId,operationId,this.getAuthInfo(),attributes,input,this.getServiceInfo());
        } catch (DoipException e){
            throw DigitalObjectRepositoryException.convertDoipException(e);
        }
    }

    public DoipClientResponse performOperation(String targetId, String operationId, JsonObject attributes, InDoipMessage input) throws DigitalObjectRepositoryException {
        try{
            return this.getDoipClient().performOperation(targetId,operationId,this.getAuthInfo(),attributes,input,this.getServiceInfo());
        } catch (DoipException e){
            throw DigitalObjectRepositoryException.convertDoipException(e);
        }
    }

    public DoipClientResponse performOperation(DoipRequestHeaders headers, InDoipMessage input) throws DigitalObjectRepositoryException {
        try {
            return this.getDoipClient().performOperation(headers, input, this.getServiceInfo());
        } catch (DoipException e){
            throw DigitalObjectRepositoryException.convertDoipException(e);
        }
    }

    public DigitalObject create(DigitalObject dobj) throws DigitalObjectRepositoryException {
        try {
            return this.getDoipClient().create(dobj,this.getAuthInfo(),this.getServiceInfo());
        } catch (DoipException e){
            throw DigitalObjectRepositoryException.convertDoipException(e);
        }
    }

    public DigitalObject update(DigitalObject dobj) throws DigitalObjectRepositoryException {
        try {
            return this.getDoipClient().update(dobj,this.getAuthInfo(),this.getServiceInfo());
        } catch (DoipException e) {
           throw DigitalObjectRepositoryException.convertDoipException(e);
        }
    }

    public DigitalObject retrieve(String targetId) throws DigitalObjectRepositoryException {
        try {
            return this.getDoipClient().retrieve(targetId, false, this.getAuthInfo(), this.getServiceInfo());
        } catch (DoipException e) {
           throw DigitalObjectRepositoryException.convertDoipException(e);
        }
    }

    public DigitalObject retrieve(String targetId, boolean includeElementData) throws DigitalObjectRepositoryException {
        try {
            return this.getDoipClient().retrieve(targetId,includeElementData,this.getAuthInfo(),this.getServiceInfo());
        } catch (DoipException e) {
           throw DigitalObjectRepositoryException.convertDoipException(e);
        }
    }

    public void delete(String targetId) throws DigitalObjectRepositoryException {
        try {
            this.getDoipClient().delete(targetId,this.getAuthInfo(),this.getServiceInfo());
        } catch (DoipException e) {
           throw DigitalObjectRepositoryException.convertDoipException(e);
        }
    }

    public List<String> listOperations(String targetId) throws DigitalObjectRepositoryException {
        try {
            return this.getDoipClient().listOperations(targetId,this.getAuthInfo(),this.getServiceInfo());
        } catch (DoipException e) {
           throw DigitalObjectRepositoryException.convertDoipException(e);
        }
    }

    public SearchResults<String> searchIds(String targetId, String query, QueryParams params) throws DigitalObjectRepositoryException {
        try {
            return this.getDoipClient().searchIds(targetId,query,params,this.getAuthInfo(),this.getServiceInfo());
        } catch (DoipException e) {
           throw DigitalObjectRepositoryException.convertDoipException(e);
        }
    }

    public SearchResults<DigitalObject> search(String targetId, String query, QueryParams params) throws DigitalObjectRepositoryException {
        try {
            return this.getDoipClient().search(targetId,query,params,this.getAuthInfo(),this.getServiceInfo());
        } catch (DoipException e) {
           throw DigitalObjectRepositoryException.convertDoipException(e);
        }
    }

    public DigitalObject hello(String targetId) throws DigitalObjectRepositoryException {
        try {
            return this.getDoipClient().hello(targetId,this.getAuthInfo(),this.getServiceInfo());
        } catch (DoipException e) {
           throw DigitalObjectRepositoryException.convertDoipException(e);
        }
    }

    public InputStream retrieveElement(String targetId, String elementId) throws DigitalObjectRepositoryException {
        try {
            return this.getDoipClient().retrieveElement(targetId,elementId,this.getAuthInfo(),this.getServiceInfo());
        } catch (DoipException e) {
           throw DigitalObjectRepositoryException.convertDoipException(e);
        }
    }

    public InputStream retrievePartialElement(String targetId, String elementId, Long start, Long end) throws DigitalObjectRepositoryException {
        try {
            return this.getDoipClient().retrievePartialElement(targetId,elementId,start,end,this.getAuthInfo(),this.getServiceInfo());
        } catch (DoipException e) {
           throw DigitalObjectRepositoryException.convertDoipException(e);
        }
    }
}
