package com.apple.spark.appleinternal;

import static com.apple.spark.core.Constants.HIVE_METASTORE_SASL_ENABLED;
import static com.apple.spark.core.Constants.LDAP_ENDPOINT;
import static com.apple.spark.rest.RestBase.LDAP_SEARCH_LATENCY_METRIC_NAME;

import com.apple.spark.AppConfig;
import com.apple.spark.api.SubmitApplicationRequest;
import com.apple.spark.operator.*;
import com.apple.spark.util.TimerMetricContainer;
import com.google.gson.Gson;
import io.fabric8.kubernetes.api.model.CSIVolumeSource;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.leangen.geantyref.TypeToken;
import io.micrometer.core.instrument.Tag;
import java.util.*;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppleKerberosUtil {

  private static final Logger logger = LoggerFactory.getLogger(AppleKerberosUtil.class);

  public static void enableKerberosSupport(
      SparkApplicationSpec sparkSpec,
      SubmitApplicationRequest request,
      AppConfig appConfig,
      String proxyUser,
      TimerMetricContainer timerMetrics) {
    // If connecting to secured HMS, add extra resources
    if (connectToSecuredHiveMetastore(request, appConfig)) {

      sparkSpec.setProxyUser(null);

      // add a Narrative volume to store identity certificate
      Map<String, String> volumeAttributes = new HashMap<String, String>();
      volumeAttributes.put("narrative.apple.com/type", "actor");
      volumeAttributes.put("narrative.apple.com/issuer-name", "delegation-token-tool-test");
      volumeAttributes.put("narrative.apple.com/fs-group", "1000");
      volumeAttributes.put("narrative.apple.com/domain", "idms");
      volumeAttributes.put("narrative.apple.com/name-json-type", "person");
      String dsid =
          timerMetrics.record(
              () -> getDsidFromAcUserName(proxyUser),
              LDAP_SEARCH_LATENCY_METRIC_NAME,
              Tag.of("name", "get_dsid"),
              Tag.of("user", proxyUser));
      volumeAttributes.put("narrative.apple.com/name-json-person_id", dsid);
      CSIVolumeSource csiVolume =
          new CSIVolumeSource("narrative.apple.com", null, null, true, volumeAttributes);
      Volume narrativeVolume = new Volume("narrative", csiVolume);

      // add a emptyDir volume to store the delegation token
      EmptyDirVolumeSource emptyDirVolumeSource = new EmptyDirVolumeSource();
      Volume tokenStoreVolume = new Volume("token-store", emptyDirVolumeSource);

      if (sparkSpec.getVolumes() != null) {
        sparkSpec.getVolumes().add(narrativeVolume);
        sparkSpec.getVolumes().add(tokenStoreVolume);
      } else {
        List<Volume> addedVolumes = new ArrayList<>();
        addedVolumes.add(narrativeVolume);
        addedVolumes.add(tokenStoreVolume);
        sparkSpec.setVolumes(addedVolumes);
      }

      // Add volumeMounts
      VolumeMount tokenStore = new VolumeMount("token-store", "/secrets");
      VolumeMount narrative = new VolumeMount("narrative", "/narrative");
      if (sparkSpec.getDriver().getVolumeMounts() != null) {
        sparkSpec.getDriver().getVolumeMounts().add(tokenStore);
        sparkSpec.getDriver().getVolumeMounts().add(narrative);
      } else {
        List<VolumeMount> volumeMounts = new ArrayList<>();
        volumeMounts.add(tokenStore);
        volumeMounts.add(narrative);
        sparkSpec.getDriver().setVolumeMounts(volumeMounts);
      }

      // Add initContainer which generates a delegation token for Spark application
      Gson gson = new Gson();
      List<InitContainer> initContainers;
      Optional<InitContainer> firstInitContainer;
      try {
        initContainers =
            gson.fromJson(
                gson.toJson(appConfig.getDriverInitContainers()),
                new TypeToken<List<InitContainer>>() {}.getType());
        // loop through the list, find the "delegation-token-tool" initContainer and add user
        // principal env variable
        firstInitContainer =
            initContainers.stream()
                .filter(
                    container ->
                        container
                            .getName()
                            .equals(AppleKerberosUtilConstants.DELEGATION_CONTAINER_NAME))
                .findFirst();
      } catch (Exception e) {
        String errMsg = "Secured HMS can't be used without having initContainer in appConfig";
        logger.error(errMsg);
        throw new WebApplicationException(errMsg, Response.Status.BAD_REQUEST);
      }

      EnvVar userPrincipal =
          new EnvVar(AppleKerberosUtilConstants.INIT_CONTAINER_ENV_KEY, proxyUser);

      firstInitContainer.ifPresentOrElse(
          container -> {
            if (container.getEnv() != null) {
              container.getEnv().add(userPrincipal);
            } else {
              container.setEnv(Collections.singletonList(userPrincipal));
            }
          },
          () -> {
            String errMsg =
                "Secured HMS can't be used without having delegation-token-tool initContainer in appConfig";
            logger.error(errMsg);
            throw new WebApplicationException(errMsg, Response.Status.BAD_REQUEST);
          });

      sparkSpec.getDriver().setInitContainers(initContainers);

      // Add HADOOP_USER_NAME env variable for driver container
      EnvVar hadoopUserName = new EnvVar("HADOOP_USER_NAME", proxyUser);
      EnvVar sparkUser = new EnvVar("SPARK_USER", proxyUser);
      EnvVar hadoopTokenFileLocation =
          new EnvVar("HADOOP_TOKEN_FILE_LOCATION", "/secrets/token-store");
      if (sparkSpec.getDriver().getEnv() != null) {
        sparkSpec.getDriver().getEnv().add(hadoopUserName);
        sparkSpec.getDriver().getEnv().add(sparkUser);
        sparkSpec.getDriver().getEnv().add(hadoopTokenFileLocation);
      } else {
        List<EnvVar> tmpListEnvVar = new ArrayList<>();
        tmpListEnvVar.add(hadoopUserName);
        tmpListEnvVar.add(hadoopTokenFileLocation);
        tmpListEnvVar.add(sparkUser);
        sparkSpec.getDriver().setEnv(tmpListEnvVar);
      }

      // Add Security Context
      SecurityContext securityContext = new SecurityContext();
      securityContext.setFsGroup(1000L);
      sparkSpec.getDriver().setSecurityContext(securityContext);
    }
  }

  /**
   * Query Ldap and get the DSID of a AppleConnect userName. This method is needed since airflow
   * currently uses the airflow bot to authenticate. This should be removed after re-design of
   * Airflow to Skate authentication.
   *
   * @param acUserName
   * @return
   */
  private static String getDsidFromAcUserName(String acUserName) {
    String dsid = "";
    try {
      Hashtable env = new Hashtable(11);
      env.put(LdapContext.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      env.put(LdapContext.PROVIDER_URL, LDAP_ENDPOINT);
      env.put(LdapContext.SECURITY_AUTHENTICATION, "none");

      LdapContext ctx = new InitialLdapContext(env, null);
      ctx.setRequestControls(null);

      NamingEnumeration<?> namingEnum =
          ctx.search(
              "cn=users,dc=apple,dc=com",
              "uid=" + acUserName + "@APPLECONNECT.APPLE.COM",
              getSimpleSearchControls());

      while (namingEnum.hasMore()) {
        SearchResult result = (SearchResult) namingEnum.next();
        Attributes attrs = result.getAttributes();
        dsid = attrs.get("uidNumber").get().toString();
      }
      namingEnum.close();
    } catch (Exception e) {
      logger.warn("Failed to query DSID by Ldap search for user {}", acUserName, e);
    }
    return dsid;
  }

  private static SearchControls getSimpleSearchControls() {
    SearchControls searchControls = new SearchControls();
    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    searchControls.setTimeLimit(30000);
    return searchControls;
  }

  /**
   * A spark job could specify to connect to a secured hive metastore, by adding a spark property
   * spark.hadoop.hive.metastore.sasl.enabled=true in the request. The admin could also set it in
   * the AppConfig as a default setting.
   *
   * @param request
   * @param appConfig
   * @return
   */
  public static boolean connectToSecuredHiveMetastore(
      SubmitApplicationRequest request, AppConfig appConfig) {
    if (request.getSparkConf() != null) {
      if (request.getSparkConf().containsKey(HIVE_METASTORE_SASL_ENABLED)) {
        return request.getSparkConf().get(HIVE_METASTORE_SASL_ENABLED).equals("true");
      }
    } else {
      if (appConfig.getDefaultSparkConf() != null) {
        if (appConfig.getDefaultSparkConf().containsKey(HIVE_METASTORE_SASL_ENABLED)) {
          return appConfig.getDefaultSparkConf().get(HIVE_METASTORE_SASL_ENABLED).equals("true");
        }
      }
    }
    return false;
  }
}
