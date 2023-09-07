package com.apple.spark.appleinternal.notary;

import com.apple.spark.AppConfig;
import com.apple.turi.directory.DirectoryServiceException;
import java.util.ArrayList;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testng.Assert;

public class NotaryDirectoryServiceTest {
  private static AppConfig appconfi;
  private static NotaryDirectoryService notaryDirectoryService;

  @BeforeClass
  public static void setupClass() {
    appconfi = new AppConfig();
    AppConfig.NotaryAppConfig notary = new AppConfig.NotaryAppConfig();
    notary.setTuriDirectoryApiKey("BJXDGZRWbkN6sazVRHvdnY9F");
    List<Long> proxyAllowedGroups = new ArrayList<>();
    proxyAllowedGroups.add(8609568L);
    notary.setProxyAllowedGroupIDList(proxyAllowedGroups);
    appconfi.setNotary(notary);

    notaryDirectoryService = new NotaryDirectoryService(appconfi);
  }

  @Test
  public void testGetPersonIdFromTuri() throws DirectoryServiceException {
    String username = "mingcui_yang";
    Long personId = notaryDirectoryService.getPersonIdFromTuri(username);
    Long expected = 2701123238L;
    Assert.assertEquals(personId, expected);
  }

  @Test
  public void getUserNameFromTuri() throws DirectoryServiceException {
    Long personId = 2701123238L;
    String username = notaryDirectoryService.getUserNameFromTuri(personId);
    Assert.assertEquals(username, "mingcui_yang");
  }

  @Test
  public void checkPersonIdMembership() throws DirectoryServiceException {
    Long personId = 2701123238L;
    Boolean ifBelong = notaryDirectoryService.checkIfPersonIdInAllowedGroups(personId);
    Assert.assertEquals(ifBelong.booleanValue(), true);
  }

  @Test
  public void checkPersonIdMembership_notInAllowedGroup() throws DirectoryServiceException {
    Long personId = 123456L;
    Boolean ifBelong = notaryDirectoryService.checkIfPersonIdInAllowedGroups(personId);
    Assert.assertEquals(ifBelong.booleanValue(), false);
  }
}
