package org.geowebcache.elasticsearch;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

/**
 * @author shimingen
 * @date 2021/9/8
 */
public class ElasticsearchManager {
    private final RestClient restClient;
    private final String x;
    private final String y;
    private final String z;
    private final String img;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ElasticsearchManager(ElasticsearchBlobStoreInfo info) {
        final Node[] nodes = Arrays.stream(info.getHosts()).map(Node::new).toArray(Node[]::new);
        final RestClientBuilder restClientBuilder = RestClient.builder(nodes)
                .setRequestConfigCallback(requestBuilder -> {
                    requestBuilder.setConnectTimeout(info.getConnectTimeout());
                    requestBuilder.setSocketTimeout(info.getSocketTimeout());
                    requestBuilder.setConnectionRequestTimeout(info.getConnectionRequestTimeout());
                    return requestBuilder;
                });
        if (StringUtils.hasText(info.getUsername()) && StringUtils.hasText(info.getPassword())) {
            final BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
            basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(info.getUsername(), info.getPassword()));
            restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                httpAsyncClientBuilder.disableAuthCaching();
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider);
            });
        }
        restClient = restClientBuilder.build();
        this.x = info.getxTile();
        this.y = info.getyTile();
        this.z = info.getZoom();
        this.img = info.getImg();
    }

    /**
     * 创建索引
     *
     * @param index 索引名
     * @return 创建结果
     * @throws IOException 异常
     */
    public boolean createIndex(String index) throws IOException {
        Request request = new Request("PUT", String.format("/%s", index));
        String body = "{" +
                "\"mappings\": {" +
                "\"doc\": {" +
                "\"properties\": {" +
                "\"id\": {" +
                "\"type\": \"text\"," +
                "\"index\": false" +
                "}," +
                "\"%s\": {" +
                "\"type\": \"text\"," +
                "\"index\": false" +
                "}," +
                "\"%s\": {" +
                "\"type\": \"long\"," +
                "\"index\": true" +
                "}," +
                "\"%s\": {" +
                "\"type\": \"long\"," +
                "\"index\": true" +
                "}," +
                "\"%s\": {" +
                "\"type\": \"long\"," +
                "\"index\": true" +
                "}" +
                "}" +
                "}" +
                "}," +
                "\"settings\": {" +
                "\"index\": {" +
                "\"number_of_replicas\": \"0\"" +
                "}" +
                "}" +
                "}";
        HttpEntity entity = new NStringEntity(String.format(body, img, x, y, z));
        request.setEntity(entity);
        final Response response = restClient.performRequest(request);
        final byte[] bytes = EntityUtils.toByteArray(response.getEntity());
        final Map<String, Object> map = objectMapper.readValue(bytes, new TypeReference<Map<String, Object>>() {
        });
        return !map.containsKey("error") && (boolean) map.get("acknowledged");
    }

    /**
     * 删除索引
     *
     * @param index 索引名
     * @return 创建结果
     */
    public boolean deleteIndex(String index) {
        try {
            Request request = new Request("DELETE", String.format("/%s", index));
            final Response response = restClient.performRequest(request);
            final byte[] bytes = EntityUtils.toByteArray(response.getEntity());
            final Map<String, Object> map = objectMapper.readValue(bytes, new TypeReference<Map<String, Object>>() {
            });
            return !map.containsKey("error") && (boolean) map.get("acknowledged");
        } catch (IOException e) {
            throw new RuntimeException("");
        }
    }

    /**
     * 检查索引是否存在
     *
     * @param index 索引名
     * @return 创建结果
     * @throws IOException 异常
     */
    public boolean checkIndex(String index) throws IOException {
        Request request = new Request("GET", String.format("/%s", index));
        final Response response = restClient.performRequest(request);
        final byte[] bytes = EntityUtils.toByteArray(response.getEntity());
        final Map<String, Object> map = objectMapper.readValue(bytes, new TypeReference<Map<String, Object>>() {
        });
        return !map.containsKey("error");
    }

    public boolean putTile(String index, long[] xyz, byte[] img0) throws IOException {
        Request request = new Request("POST", String.format("/%s/doc", index));
        HttpEntity entity = new NStringEntity(String.format("{\n" +
                "\t\"id\": \"%s\",\n" +
                "\t\"%s\": %d,\n" +
                "\t\"%s\": %d,\n" +
                "\t\"%s\": %d,\n" +
                "\t\"%s\": %s\n" +
                "}", xyz[0] + "_" + xyz[1] + "_" + xyz[2], x, xyz[0], y, xyz[1], z, xyz[2], img, Base64.getEncoder().encodeToString(img0)));
        request.setEntity(entity);
        final Response response = restClient.performRequest(request);
        final byte[] bytes = EntityUtils.toByteArray(response.getEntity());
        final Map<String, Object> map = objectMapper.readValue(bytes, new TypeReference<Map<String, Object>>() {
        });
        return !map.containsKey("error") && map.containsKey("result");
    }

    public byte[] getImg(String index, long[] xyz) throws IOException {
        Request request = new Request("POST", String.format("/%s/_search", index));
        HttpEntity entity = new NStringEntity(String.format("{" +
                "\"from\": 0," +
                "\"size\": 1," +
                "\"timeout\": \"60s\"," +
                "\"query\": {" +
                "\"bool\": {" +
                "\"must\": [{" +
                "\"match\": {" +
                "\"%s\": {" +
                "\"query\": %d," +
                "\"operator\": \"AND\"" +
                "}" +
                "}" +
                "}, {" +
                "\"match\": {" +
                "\"%s\": {" +
                "\"query\": %d," +
                "\"operator\": \"AND\"" +
                "}" +
                "}" +
                "}, {" +
                "\"match\": {" +
                "\"%s\": {" +
                "\"query\": %d," +
                "\"operator\": \"AND\"" +
                "}" +
                "}" +
                "}]," +
                "\"adjust_pure_negative\": true," +
                "\"boost\": 1.0" +
                "}" +
                "}" +
                "}", x, xyz[0], y, xyz[1], z, xyz[2]), ContentType.APPLICATION_JSON);
        request.setEntity(entity);
        final Response response = restClient.performRequest(request);
        final byte[] bytes = EntityUtils.toByteArray(response.getEntity());
        final HitsResult hitsResult = objectMapper.readValue(bytes, HitsResult.class);
        final Integer total = hitsResult.getHits().getTotal();
        if (total == 0) {
            throw new RuntimeException("Not Found");
        }
        return Base64.getDecoder().decode(hitsResult.getHits().getHits()[0].get_source().getImg());
    }

    /**
     * 坐标转换
     *
     * @param xyz xyz 坐标
     * @return 坐标
     */
    public long[] trans(long xyz[]) {
        long[] ret = new long[3];
        long y = xyz[1];
        long z = xyz[2];
        if (z > 0) {
            long val = (2L << (z - 1)) - 1;
            xyz[1] = val - y;
        }
        ret[0] = xyz[0];
        ret[1] = xyz[1];
        ret[2] = xyz[2];
        return ret;
    }

    /**
     * 坐标转换
     *
     * @param xyz xyz 坐标
     * @return 坐标
     */
    public long[] readTrans(long xyz[]) {
        long[] ret = new long[3];
        ret[0] = xyz[0];
        long z = xyz[2];
        if (z > 0) {
            long val = (2L << (z - 1)) - 1;
            ret[1] = val - xyz[1];
        } else {
            ret[1] = xyz[1];
        }
        ret[2] = xyz[2];
        return ret;
    }


}
