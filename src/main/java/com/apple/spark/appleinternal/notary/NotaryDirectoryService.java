package com.apple.spark.appleinternal.notary;

import com.apple.spark.AppConfig;
import com.apple.turi.directory.DirectoryService;
import com.apple.turi.directory.DirectoryServiceException;
import com.apple.turi.directory.IsMemberRequest;
import com.apple.turi.directory.IsMemberResponse;
import com.apple.turi.directory.Person;
import com.apple.turi.directory.PersonInfoRequest;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotaryDirectoryService {

  private static final Logger logger = LoggerFactory.getLogger(NotaryDirectoryService.class);
  private final DirectoryService turiDirectory;
  private List<Long> proxyAllowedGroups;

  private static final Map<String, Long> personIdCache =
      new ConcurrentHashMap<>(); // username:personId
  private static final Map<Long, String> userNameCache =
      new ConcurrentHashMap<>(); // personId:username
  private static final Map<Long, Boolean> personIdGroupsCache =
      new ConcurrentHashMap<>(); // personId:true/false

  /**
   * Getting directoryKey from BPG config and connecting to Turi Directory Service.
   *
   * @param appConfig BPG application config retrieved from config file.
   */
  public NotaryDirectoryService(AppConfig appConfig) {
    String directoryKey = null;
    AppConfig.NotaryAppConfig notary = appConfig.getNotary();
    if (notary != null) {
      directoryKey = notary.getTuriDirectoryApiKey();
      proxyAllowedGroups = notary.getProxyAllowedGroupIDList();
      logger.debug("proxyAllowedGroups: {}", proxyAllowedGroups.toString());
    } else {
      throw new WebApplicationException(
          String.format("Missing notary config! Cannot connect to Turi Directory Service!"),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    DirectoryService.Server server = DirectoryService.Server.PROD;
    turiDirectory = new DirectoryService(server, directoryKey);
  }

  /**
   * Get personId given username from Turi Directory Service.
   *
   * @param acUserName
   * @return personId
   */
  public Long getPersonIdFromTuri(String acUserName) throws DirectoryServiceException {
    logger.debug("Get personId from Turi for user: {}", acUserName);
    Long personId = personIdCache.get(acUserName);

    if (personId == null) {
      try {
        PersonInfoRequest request =
            new PersonInfoRequest.Builder().setAccountName(acUserName).build();
        Person response = turiDirectory.personInfo(request);
        logger.debug("personInfo get by Turi:{}", response.toString());
        personId = response.id;
        personIdCache.put(acUserName, personId);
      } catch (DirectoryServiceException e) {
        logger.error("Failed to fetch person info from TuriDirectory Service", e);
        throw e;
      }
    }

    return personId;
  }

  /**
   * Get username given personId from Turi Directory Service.
   *
   * @param personId
   * @return username
   */
  public String getUserNameFromTuri(Long personId) throws DirectoryServiceException {
    logger.debug("Get username from Turi for personId: {}", personId);
    String acUserName = userNameCache.get(personId);

    if (acUserName == null) {
      try {
        PersonInfoRequest request = new PersonInfoRequest.Builder().setPersonId(personId).build();
        Person response = turiDirectory.personInfo(request);
        logger.debug("personInfo get by Turi:{}", response.toString());
        acUserName = response.accountName;
        userNameCache.put(personId, acUserName);
      } catch (DirectoryServiceException e) {
        logger.error("Failed to fetch person info from TuriDirectory Service", e);
        throw e;
      }
    }

    return acUserName;
  }

  /**
   * Check if the proxy user of Notary A3 use case belongs to at least one of the groups in the
   * proxyAllowedGroups.
   *
   * @param personId
   * @return true if the user belongs to at least one group else false.
   */
  public Boolean checkIfPersonIdInAllowedGroups(Long personId) throws DirectoryServiceException {
    logger.debug("Check if {} in allowed groups list", personId);
    Boolean ifBelong = personIdGroupsCache.get(personId);

    if (ifBelong == null) {
      try {
        IsMemberRequest request =
            new IsMemberRequest.Builder()
                .setPersonId(personId)
                .setGroupIds(proxyAllowedGroups)
                .build();

        IsMemberResponse response = turiDirectory.isMember(request);
        logger.debug("Checking result: {}", response.toString());
        for (Boolean v : response.membershipById.values()) {
          if (v == true) {
            ifBelong = true;
            break;
          }
        }

        if (ifBelong == null) {
          logger.error("User {} doesn't belongs to the any allowed groups!", personId);
          ifBelong = false;
        }

        personIdGroupsCache.put(personId, ifBelong);
      } catch (DirectoryServiceException e) {
        logger.error(
            "Failed to check the membership of the personId with Turi Directory Service", e);
        throw e;
      }
    }
    return ifBelong;
  }
}
