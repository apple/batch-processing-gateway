package com.apple.spark.ranger.client;

import static com.apple.spark.ranger.client.Constants.*;

import com.sun.jersey.api.client.ClientResponse;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Author: Priyank Shah Created: Jan 31, 2022
 *
 * <p>Copied by: Mengna Lin From
 * https://github.pie.apple.com/aiml-datainfra/ranger-spark/blob/release/ranger-2.1/src/main/java/com/apple/aiml/spark/security/AdminRESTClient.java
 * Created: Sept 22, 2023
 */
public class AdminRESTClient extends RangerAdminRESTClient {
  private static final Logger LOG = LoggerFactory.getLogger(RangerAdminRESTClient.class);
  private RangerRESTClient basicAuthRESTClient;

  @Override
  public void init(String serviceName, String appId, String propertyPrefix, Configuration config) {
    super.init(serviceName, appId, propertyPrefix, config);

    String url = config.get(String.format("%s.%s", propertyPrefix, POLICY_REST_URL));
    String sslConfigFileName =
        config.get(String.format("%s.%s", propertyPrefix, POLICY_REST_SSL_CONFIG_FILE));

    int restClientConnTimeOutMs =
        config.getInt(
            String.format("%s.%s", propertyPrefix, POLICY_REST_CLIENT_CONNECTION_TIMEOUT), 120000);
    int restClientReadTimeOutMs =
        config.getInt(
            String.format("%s.%s", propertyPrefix, POLICY_REST_CLIENT_READ_TIMEOUT), 30000);

    if (!StringUtil.isEmpty(url)) {
      url = url.trim();
    }

    if (url.endsWith("/")) {
      url = url.substring(0, url.length() - 1);
    }

    this.basicAuthRESTClient = new RangerRESTClient(url, sslConfigFileName, config);
    this.basicAuthRESTClient.setRestClientConnTimeOutMs(restClientConnTimeOutMs);
    this.basicAuthRESTClient.setRestClientReadTimeOutMs(restClientReadTimeOutMs);

    LOG.debug("<== RangerAdminRESTClient.init(" + url + ", " + sslConfigFileName + ")");

    String restUser = config.get(String.format("%s.%s", propertyPrefix, PLUGIN_USER), null);
    String restPass = config.get(String.format("%s.%s", propertyPrefix, PLUGIN_PASSWORD), null);

    if (restUser != null && restPass != null) {
      try {
        basicAuthRESTClient.setBasicAuthInfo(restUser, restPass);
      } catch (Exception e) {
        LOG.error("Set auth fail on RangerAdminRESTClient.", e);
      }
    }
  }

  @Override
  public List<String> getUserRoles(String execUser) throws Exception {
    LOG.debug("==> AdminRESTClient.getUserRoles(" + execUser + ")");

    List<String> ret;
    String emptyString = "";
    ClientResponse response;
    UserGroupInformation user = MiscUtil.getUGILoginUser();
    final String relativeURL = "/service/public/v2/api/roles/user/" + execUser;
    response = this.basicAuthRESTClient.get(relativeURL, null);

    if (response != null) {
      if (response.getStatus() != 200) {
        RESTResponse resp = RESTResponse.fromClientResponse(response);
        LOG.error(
            "getUserRoles() failed: HTTP status="
                + response.getStatus()
                + ", message="
                + resp.getMessage()
                + ", user="
                + user);
        if (response.getStatus() == 401) {
          throw new AccessControlException();
        } else {
          throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
        }
      } else {
        ret = response.getEntity(getGenericType(emptyString));
        LOG.debug("<== RangerAdminRESTClient.getUserRoles(" + execUser + ")");

        return ret;
      }
    } else {
      throw new Exception("unknown error during getUserRoles. execUser=" + execUser);
    }
  }
}
