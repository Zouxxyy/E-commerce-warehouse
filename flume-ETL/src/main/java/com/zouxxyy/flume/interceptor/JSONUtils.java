package com.zouxxyy.flume.interceptor;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

/**
 * @author zxy
 */
public class JSONUtils {
    public static boolean isJson(String log) {
        try {
            JSONObject.parseObject(log);
            return true;
        } catch (JSONException ignore) {
            return false;
        }
    }
}
