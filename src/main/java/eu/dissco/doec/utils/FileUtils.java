package eu.dissco.doec.utils;

import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.stream.JsonReader;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.FileBasedBuilderParameters;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class FileUtils {

    /******************/
    /* PUBLIC METHODS */
    /******************/

    /**
     * Load the configuration file passed as parameter from the resource folder
     * @param filename Name of the configuration file in the resource folder to be loaded
     * @return apache commons configuration object
     * @throws ConfigurationException
     */
    public static Configuration loadConfigurationFromResourceFile(String filename) throws ConfigurationException {
        URL configFileUrl = Resources.getResource(filename);

        FileBasedBuilderParameters params = new Parameters().fileBased();
        params.setListDelimiterHandler(new DefaultListDelimiterHandler(';'));
        params.setFile(new File(configFileUrl.getPath()));
        FileBasedConfigurationBuilder<PropertiesConfiguration> builder = new FileBasedConfigurationBuilder<PropertiesConfiguration>(PropertiesConfiguration.class).configure(params);
        return builder.getConfiguration();
    }

    /**
     * Load the configuration file passed as parameter using its path
     * @param filepath File path of the configuration fileto be loaded
     * @return apache commons configuration object
     * @throws ConfigurationException
     */
    public static Configuration loadConfigurationFromFilePath(String filepath) throws ConfigurationException {
        FileBasedBuilderParameters params = new Parameters().fileBased();
        params.setListDelimiterHandler(new DefaultListDelimiterHandler(';'));
        params.setFile(new File(filepath));
        FileBasedConfigurationBuilder<PropertiesConfiguration> builder = new FileBasedConfigurationBuilder<PropertiesConfiguration>(PropertiesConfiguration.class).configure(params);
        return builder.getConfiguration();
    }

    /**
     * Load the json file found in the resource into a JsonElement
     * @param filename Name of the json file in the resource folder to be loaded as JsonElement
     * @return JsonElement element from the json file
     * @throws IOException
     * @throws URISyntaxException
     */
    public static JsonElement loadJsonElementFromResourceFile(String filename) throws IOException, URISyntaxException {
        Gson gson = new Gson();
        URL url = Resources.getResource(filename);
        Path path = Paths.get(url.toURI());

        return gson.fromJson(new FileReader(path.toFile()), JsonElement.class);
    }

    /**
     * Load the json file passes as parameter into a JsonElement
     * @param filepath File path of the json file to be loaded as JsonElement
     * @return JsonElement element from the json file
     * @throws IOException
     * @throws URISyntaxException
     */
    public static JsonElement loadJsonElementFromFilePath(String filepath) throws IOException {
        Gson gson = new Gson();
        JsonReader reader = new JsonReader(new FileReader(filepath));
        return gson.fromJson(reader, JsonElement.class);
    }

    /**
     * Zip a folder into a zip file
     * @param sourceFolderPath Path of the folder to be zipped
     * @param zipPath Path of the zip file to be created
     * @throws Exception
     */
    public static void zipFolder(Path sourceFolderPath, Path zipPath) throws Exception {
        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipPath.toFile()));
        java.nio.file.Files.walkFileTree(sourceFolderPath, new SimpleFileVisitor<Path>() {
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                zos.putNextEntry(new ZipEntry(sourceFolderPath.relativize(file).toString()));
                java.nio.file.Files.copy(file, zos);
                zos.closeEntry();
                return FileVisitResult.CONTINUE;
            }
        });
        zos.close();
    }

    /**
     * Zip a file
     * @param fileToZip
     * @param zipFile
     * @throws Exception
     */
    public static void zipFile(File fileToZip, File zipFile) throws Exception{
        try(FileOutputStream fos = new FileOutputStream(zipFile);
        ZipOutputStream zipOut = new ZipOutputStream(fos);
        FileInputStream fis = new FileInputStream(fileToZip);){
            ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
            zipOut.putNextEntry(zipEntry);
            byte[] bytes = new byte[1024];
            int length;
            while((length = fis.read(bytes)) >= 0) {
                zipOut.write(bytes, 0, length);
            }
        }
    }
}
