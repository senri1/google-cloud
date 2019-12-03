/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.gcp.common;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.Credentials;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.TransportOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Strings;
import static com.google.common.base.Preconditions.checkArgument;

import io.cdap.plugin.gcp.gcs.GCSPath;
import io.cdap.plugin.gcp.gcs.sink.GCSBatchSink.GCSBatchSinkConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * GCP utility class to get service account credentials
 */
public class GCPUtils {

  static URI parseProxyAddress(@Nullable String proxy) {
    if (Strings.isNullOrEmpty(proxy)) {
      return null;
    }
    String uriString = (proxy.contains("//") ? "" : "//") + proxy;
    try {
      URI uri = new URI(uriString);
      String scheme = uri.getScheme();
      String host = uri.getHost();
      int port = uri.getPort();
      checkArgument(
          Strings.isNullOrEmpty(scheme) || scheme.matches("https?"),
          "HTTP proxy address '%s' has invalid scheme '%s'.", proxy, scheme);
      checkArgument(!Strings.isNullOrEmpty(host), "Proxy address '%s' has no host.", proxy);
      checkArgument(port != -1, "Proxy address '%s' has no port.", proxy);
      checkArgument(
          uri.equals(new URI(scheme, null, host, port, null, null, null)),
          "Invalid proxy address '%s'.", proxy);
      return uri;
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Invalid proxy address '%s'.", proxy), e);
    }
  }

  public static HttpTransportFactory creatHttpTransportFactory(String path, String proxy){
    URI uri = parseProxyAddress(proxy);
    HttpTransportFactory transportFactory = new HttpTransportFactory(){
      
      @Override
      public HttpTransport create() {
        return new NetHttpTransport.Builder()
                              .setProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(uri.getHost(), uri.getPort())))
                              .build();
      }
    };
    return transportFactory;
  }

  // public static ServiceAccountCredentials loadServiceAccountCredentials(String path, @Nullable String proxy) throws IOException {
  //   File credentialsPath = new File(path);
  //   try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
  //     return ServiceAccountCredentials.fromStream(serviceAccountStream);
  //   }
  // }

  public static Credentials loadServiceAccountCredentials(String path, String proxy, @Nullable HttpTransportFactory transportFactory) throws IOException{
    Credentials credentials = null;

    if (proxy != null) {
      credentials = GoogleCredentials.fromStream(new FileInputStream(path), transportFactory);
      return credentials;
    }
    else{
      credentials = GoogleCredentials.fromStream(new FileInputStream(path));
      return credentials;
    }
  }
  

  public static Map<String, String> getFileSystemProperties(GCSBatchSinkConfig config) {
    Map<String, String> properties = new HashMap<>();
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    if (serviceAccountFilePath != null) {
      properties.put("google.cloud.auth.service.account.json.keyfile", serviceAccountFilePath);
    }
    properties.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    properties.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    String projectId = config.getProject();
    properties.put("fs.gs.project.id", projectId);
    properties.put("fs.gs.system.bucket", GCSPath.from(config.getPath()).getBucket());
    properties.put("fs.gs.path.encoding", "uri-path");
    properties.put("fs.gs.working.dir", GCSPath.ROOT_DIR);
    properties.put("fs.gs.impl.disable.cache", "true");
    return properties;
  }

  public static BigQuery getBigQuery(String project, @Nullable Credentials credentials, @Nullable String proxy, @Nullable HttpTransportFactory transportFactory) {
    BigQueryOptions.Builder bigqueryBuilder = BigQueryOptions.newBuilder().setProjectId(project);
    if (credentials != null) {
      bigqueryBuilder.setCredentials(credentials);
    }
    if (proxy != null) {
      TransportOptions transportOptions = HttpTransportOptions.newBuilder()
                    .setHttpTransportFactory(transportFactory).build();
      bigqueryBuilder.setTransportOptions(transportOptions);
    }
    return bigqueryBuilder.build().getService();
  }

  public static Storage getStorage(String project, @Nullable Credentials credentials, @Nullable String proxy, @Nullable HttpTransportFactory transportFactory) {
    StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(project);
    if (credentials != null) {
      builder.setCredentials(credentials);
    }
    if (proxy != null) {
      TransportOptions transportOptions = HttpTransportOptions.newBuilder()
                    .setHttpTransportFactory(transportFactory).build();
      builder.setTransportOptions(transportOptions);
    }
    return builder.build().getService();
  }
}
