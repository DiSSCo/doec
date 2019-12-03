package eu.dissco.doec.utils;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;

import java.lang.reflect.Type;
import java.util.Map;

public class JsonUtils {


    /******************/
    /* PUBLIC METHODS */
    /******************/

    /**
     * Validate the json string passed a parameter against the json schema
     * @param json json string to be validated
     * @param schema json string with the schema to be used in the validation
     * @param checkRequiredId flag to indicate if the id attribute should not be included as required field
     *                        even if the schema indicate it to be required
     * @return true if json is valid according to the schema or false otherwise
     */
    public static boolean validateJsonAgainstSchema(String json, String schema, boolean checkRequiredId){
        try {
            Gson gson = new Gson();
            JsonObject jsonObject = gson.fromJson(json, JsonObject.class);
            JsonObject jsonSchema = gson.fromJson(schema, JsonObject.class);

            return JsonUtils.validateJsonAgainstSchema(jsonObject,jsonSchema,checkRequiredId);
        } catch(Exception e){
            return false;
        }
    }

    /**
     * Validate the json object passed a parameter against the json schema
     * @param jsonObject json object to be validated
     * @param jsonSchema json object with the schema to be used in the validation
     * @param checkRequiredId flag to indicate if the id attribute should not be included as required field
     *                        even if the schema indicate it to be required
     * @return true if json is valid according to the schema or false otherwise
     */
    public static boolean validateJsonAgainstSchema(JsonObject jsonObject, JsonObject jsonSchema, boolean checkRequiredId){
        try {
            if (!checkRequiredId){
                //If we want to validate the json against the schema in order to create the object,
                // the attribute "id" should not considered as required
                JsonArray requiredFields = jsonSchema.getAsJsonArray("required");
                JsonElement jsonElementToRemove=null;
                for (JsonElement jsonElement:requiredFields) {
                    if (jsonElement.getAsString().equals("id")){
                        jsonElementToRemove=jsonElement;
                        break;
                    }
                };
                requiredFields.remove(jsonElementToRemove);
            }

            //Gson doesn't currently offer the functionality to validate an json object against a schema
            //so we need to use the library https://github.com/everit-org/json-schema to validate them,
            //and because the library works with org.json.JSONObject we need to convert our gson.JsonObjects
            org.json.JSONObject orgJsonObject = convertGsonToOrgJson(jsonObject);
            org.json.JSONObject orgJsonSchema = convertGsonToOrgJson(jsonSchema);

            //Load the schema
            SchemaLoader loader = SchemaLoader.builder()
                    .schemaJson(orgJsonSchema)
                    .draftV6Support() // or draftV7Support()
                    .build();
            Schema schema = loader.load().build();

            // Validate json against schema. Throws a ValidationException if this object is invalid
            schema.validate(orgJsonObject);

            return true;
        } catch(Exception e){
            return false;
        }
    }

    /**
     * Convert a com.google.gson.JsonObject to a org.json.JSONObject
     * @param gson com.google.gson.JsonObject to conver to org.json.JSONObject
     * @return org.json.JSONObject from converting the com.google.gson.JsonObject
     */
    public static org.json.JSONObject convertGsonToOrgJson(JsonObject gson){
        return new org.json.JSONObject(gson.getAsJsonObject().toString());
    }


    /**
     * Get the differences between 2 json elements
     * @param leftJsonElem
     * @param rightJsonElem
     * @return MapDifference object with the result of the comparision
     */
    public static MapDifference<String, Object> compareJsonElements(JsonElement leftJsonElem, JsonElement rightJsonElem){
        Gson gson = new Gson();
        String leftJson = JsonUtils.serializeObject(leftJsonElem);
        String rightJson = JsonUtils.serializeObject(rightJsonElem);
        Type mapType = new TypeToken<Map<String, Object>>(){}.getType();
        Map<String, Object> leftMap = gson.fromJson(leftJson, mapType);
        Map<String, Object> rightMap = gson.fromJson(rightJson, mapType);

        //flatten the maps. It will provide better comparison results especially for nested objects and arrays.
        leftMap = FlatMapUtils.flatten(leftMap);
        rightMap = FlatMapUtils.flatten(rightMap);

        return Maps.difference(leftMap, rightMap);
    }

    /**
     * Serialize a json object using a custom strategy (not serializing logger)
     * @param obj Object to be serialize as json
     * @return Json string with the result of the serialization
     */
    public static String serializeObject(Object obj){
        ExclusionStrategy strategy = new ExclusionStrategy() {
            @Override
            public boolean shouldSkipClass(Class<?> clazz) {
                return false;
            }

            @Override
            public boolean shouldSkipField(FieldAttributes field) {
                return field.getName().startsWith("logger");
            }
        };

        Gson gson = new GsonBuilder()
                .setExclusionStrategies(strategy)
                .create();
        return gson.toJson(obj);
    }

    /**
     * Add a property to a json object
     * @param jsonObject json object on which we want to add the property
     * @param property name of the property to be added
     * @param value value of the property to be added
     */
    public static void addPropertyToJsonObj(JsonObject jsonObject, String property, Object value){
        jsonObject.add(property,JsonUtils.convertObjectToJsonElement(value));
    }

    /**
     * Function that converts an object into a json element
     * @param obj Object to be converted as json element
     * @return json element as result of the conversion
     */
    public static JsonElement convertObjectToJsonElement(Object obj){
        JsonElement jsonElement=null;
        if(obj instanceof MapDifference){
            jsonElement = JsonUtils.convertMapDifferenceToJsonElement((MapDifference)obj);
        } else{
            Gson gson = new Gson();
            jsonElement = gson.toJsonTree(obj);
        }
        return jsonElement;
    }

    /**
     * Function that converts a MapDifference object into a json object
     * Note: the function convertObjectToJsonElement doesn't serialize correctly the attribute differences
     * @param mapDifference MapDifference object to be converted
     * @return json element as result of the conversion
     */
    private static JsonElement convertMapDifferenceToJsonElement(MapDifference mapDifference){
        Gson gson = new Gson();
        JsonElement jsonElement = gson.toJsonTree(mapDifference);
        jsonElement.getAsJsonObject().remove("differences");
        Object differences = mapDifference.entriesDiffering();
        jsonElement.getAsJsonObject().add("differences",JsonUtils.convertObjectToJsonElement(differences));
        return jsonElement;
    }

}
