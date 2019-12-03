package eu.dissco.doec.digitalObjectRepository;

import org.apache.commons.configuration2.Configuration;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Objects;

public class DigitalObjectRepositoryInfo {

    /**************/
    /* ATTRIBUTES */
    /**************/

    private String url;
    private Integer doipPort;
    private String handlePrefix;
    private String username;
    private String password;
    private Integer pageSize;


    /***********************/
    /* GETTERS AND SETTERS */
    /***********************/

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Integer getDoipPort() {
        return doipPort;
    }

    public void setDoipPort(Integer doipPort) {
        this.doipPort = doipPort;
    }

    public String getHandlePrefix() {
        return handlePrefix;
    }

    public void setHandlePrefix(String handlePrefix) {
        this.handlePrefix = handlePrefix;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }


    /****************/
    /* CONSTRUCTORS */
    /****************/

    /**
     * Create a DigitalObjectRepositoryInfo
     * @param url
     * @param doipPort
     * @param handlePrefix
     * @param username
     * @param password
     * @param pageSize
     */
    public DigitalObjectRepositoryInfo(String url, int doipPort, String handlePrefix, String username, String password,
                                       int pageSize) {
        this.url = url;
        this.doipPort = doipPort;
        this.handlePrefix = handlePrefix;
        this.username = username;
        this.password = password;
        this.pageSize=pageSize;
    }


    /*******************/
    /* PUBLIC METHODS */
    /******************/

    public String getHostAddress() throws URISyntaxException, UnknownHostException {
        URI codraURI = new URI(this.getUrl());
        InetAddress inetAddress = InetAddress.getByName(codraURI.getHost());
        String codraHostAddress = inetAddress.getHostAddress();
        String nsidrServiceId = this.getHandlePrefix() + "/service";
        return codraHostAddress;
    }

    public String getServiceId(){
        String serviceId = this.getHandlePrefix() + "/service";
        return serviceId;
    }

    public static DigitalObjectRepositoryInfo getDigitalObjectRepositoryInfoFromConfig(Configuration config){
        DigitalObjectRepositoryInfo digitalObjectRepositoryInfo = new DigitalObjectRepositoryInfo(config.getString("digitalObjectRepository.url"),
                config.getInt("digitalObjectRepository.doipPort"),config.getString("digitalObjectRepository.handlePrefix"),
                config.getString("digitalObjectRepository.username"),config.getString("digitalObjectRepository.password"),
                config.getInt("digitalObjectRepository.searchPageSize"));
        return digitalObjectRepositoryInfo;
    }

    public static DigitalObjectRepositoryInfo getProvenanceRepositoryInfoFromConfig(Configuration config){
        DigitalObjectRepositoryInfo provenanceRepositoryInfo = new DigitalObjectRepositoryInfo(config.getString("provenanceRepository.url"),
                config.getInt("provenanceRepository.doipPort"),config.getString("provenanceRepository.handlePrefix"),
                config.getString("provenanceRepository.username"),config.getString("provenanceRepository.password"),
                config.getInt("provenanceRepository.searchPageSize"));
        return provenanceRepositoryInfo;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DigitalObjectRepositoryInfo digitalObjectRepositoryInfo = (DigitalObjectRepositoryInfo) o;
        return Objects.equals(url, digitalObjectRepositoryInfo.url) &&
                Objects.equals(doipPort, digitalObjectRepositoryInfo.doipPort) &&
                Objects.equals(handlePrefix, digitalObjectRepositoryInfo.handlePrefix) &&
                Objects.equals(username, digitalObjectRepositoryInfo.username) &&
                Objects.equals(password, digitalObjectRepositoryInfo.password) &&
                Objects.equals(pageSize, digitalObjectRepositoryInfo.pageSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, doipPort, handlePrefix, username, password, pageSize);
    }

    @Override
    public String toString() {
        return "DigitalObjectRepositoryInfo{" +
                "url='" + url + '\'' +
                ", doipPort=" + doipPort +
                ", handlePrefix='" + handlePrefix + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", pageSize='" + pageSize + '\'' +
                '}';
    }
}
