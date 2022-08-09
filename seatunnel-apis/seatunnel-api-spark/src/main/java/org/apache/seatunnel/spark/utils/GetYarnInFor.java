package org.apache.seatunnel.spark.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class GetYarnInFor {

    private static List<NameValuePair> setHttpParams(Map<String, String> paramMap) {
        List<NameValuePair> formparams = new ArrayList<NameValuePair>();
        Set<Map.Entry<String, String>> set = paramMap.entrySet();
        for (Map.Entry<String, String> entry : set) {
            formparams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
        }
        return formparams;
    }

    public static String getYarnInForDetail(String env, String jobName, String tags) throws IOException, JSONException {

        String yarnInForFile = "yarn_infor.properties";
        Properties properties = GetConnectMysql.loadProperties(yarnInForFile);
        String propKey = String.format("%s.yarn.api.url", env);
        String url = properties.getProperty(propKey);
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpGet httpGet = new HttpGet();
        int mapLen = "SPARK".length();
        Map<String, String> paramMap = new HashMap<String, String>(mapLen);
        paramMap.put("applicationTypes", "SPARK");
        paramMap.put("user", "bigdata");
        List<NameValuePair> formparams = setHttpParams(paramMap);
        String param = URLEncodedUtils.format(formparams, "UTF-8");
        httpGet.setURI(URI.create(url + "?" + param));
        CloseableHttpResponse response = client.execute(httpGet);
        String maxId = "null";
        if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
            HttpEntity entity = response.getEntity();
            String appsStr = IOUtils.toString(entity.getContent(), StandardCharsets.UTF_8);
            JSONObject appsJson = JSON.parseObject(appsStr);
            JSONArray app = appsJson.getJSONObject("apps").getJSONArray("app");
            ArrayList<String> appIdList = new ArrayList<>();
            for (int i = 0; i < app.size(); i++) {
                JSONObject info = app.getJSONObject(i);
                if (jobName.equals(info.getString("name")) && tags.equals(info.getString("applicationTags"))) {
                    String id = info.getString("id");
                    appIdList.add(id);
                }
            }
            if (appIdList.size() != 0) {
                maxId = Collections.max(appIdList);
            }
        }
        return maxId;
    }
}
