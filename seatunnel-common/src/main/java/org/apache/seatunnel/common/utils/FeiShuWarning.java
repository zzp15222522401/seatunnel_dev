package org.apache.seatunnel.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;

public class FeiShuWarning {

    private static final Logger LOGGER = LoggerFactory.getLogger(FeiShuWarning.class);

    private static final int CONN_TIMEOUT = 3 * 1000;

    private static final int SO_TIMEOUT = 3 * 60 * 1000;

    private static final int MAP_SIZE = 3;

    private static String sendPostWithJson(String url, String jsonStr) {
        String jsonResult = "";
        try {
            HttpClient client = new HttpClient();
            client.getHttpConnectionManager().getParams().setConnectionTimeout(CONN_TIMEOUT);
            client.getHttpConnectionManager().getParams().setSoTimeout(SO_TIMEOUT);
            client.getParams().setContentCharset("UTF-8");
            PostMethod postMethod = new PostMethod(url);
            if (null != jsonStr && !"".equals(jsonStr)) {
                StringRequestEntity requestEntity = new StringRequestEntity(jsonStr);
                postMethod.setRequestEntity(requestEntity);
            }
            int status = client.executeMethod(postMethod);
            if (status == HttpStatus.SC_OK) {
                jsonResult = postMethod.getResponseBodyAsString();
            } else {
                throw new RuntimeException("connect false!");
            }
        } catch (Exception e) {
            throw new RuntimeException("success connect!");
        }
        return jsonResult;
    }

    public static void sendFeiShuWarningInfo(String tableName, String url) {
        HashMap<String, Object> headers = new HashMap<>(MAP_SIZE);
        headers.put("msg_type", "interactive");
        HashMap<String, Object> card = new HashMap<>(MAP_SIZE);
        HashMap<String, Object> config = new HashMap<>(MAP_SIZE);
        HashMap<String, Object> content = new HashMap<>(MAP_SIZE);
        HashMap<String, Object> header = new HashMap<>(MAP_SIZE);
        HashMap<String, Object> elements = new HashMap<>(MAP_SIZE);
        HashMap<String, Object> text = new HashMap<>(MAP_SIZE);
        HashMap<String, Object> title = new HashMap<>(MAP_SIZE);
        ArrayList<Object> list = new ArrayList<>();
        text.put("content", String.format("%s the amount of data synchronized is 0", tableName));
        text.put("tag", "lark_md");
        elements.put("tag", "div");
        elements.put("text", text);
        list.add(elements);
        title.put("content", "dis synchronous data amount warning");
        title.put("tag", "plain_text");
        header.put("title", title);
        header.put("template", "red");
        config.put("wide_screen_mode", true);
        card.put("config", config);
        card.put("header", header);
        card.put("elements", list);
        headers.put("card", card);
        JSONObject jsonObject = new JSONObject(headers);
        String jsonString = JSON.toJSONString(jsonObject);
        String resultData = FeiShuWarning.sendPostWithJson(url, jsonString);
        LOGGER.info(resultData);
    }
}
