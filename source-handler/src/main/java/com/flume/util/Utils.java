package com.flume.util;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author pengming  
 * @date 2016年11月22日 下午3:26
 * @description
 */
public class Utils {


    protected static Logger logger = LoggerFactory.getLogger(Utils.class);
    /** cookie 超时时间 */
    public static final int MAXAGE = 3 * 24 * 60 * 60;

    /** 生成随机ID */
    public static String generateAdmckid() {
        return DateFormatUtils.format(new Date(), "yyMMddHHmmss") + 1 + RandomStringUtils.randomNumeric(6);
    }

    public static String encode(String value){
        try {
            return URLEncoder.encode(value, "utf-8");
        } catch (Exception e) {
            logger.error("编码出错", e);
        }
        return "";
    }

    public static String decode(String value) {
        try {
            return URLDecoder.decode(value, "utf-8");
        } catch (Exception e) {
            logger.error("解码出错", e);
        }
        return "";
    }

    public static String decodeEx(String value) throws Exception {
        return URLDecoder.decode(value, "utf-8");
    }

    /**
     * 设置cookie
     * @param response
     * @param name  cookie名字
     * @param value cookie值
     */
    public static void addCookie(HttpServletRequest req, HttpServletResponse response, String name, String value){
        addCookie(req, response, name, value, MAXAGE);
    }

    /**
     * 设置cookie, 如果存在就更新， 不存在就生成
     * @param response
     * @param name  cookie名字
     * @param value cookie值
     * @param maxAge cookie生命周期  以秒为单位
     */
    public static void addCookie(HttpServletResponse response, String name, String value, int maxAge){
        String valueNew = encode(value);
        logger.info(name + "\tvalue: " + value + "\tnew: " + valueNew);
        Cookie cookie = new Cookie(name, valueNew);
        cookie.setPath("/");
        if(maxAge>0) {
            cookie.setMaxAge(maxAge);
        }
        response.addCookie(cookie);
    }

    /**
     * 设置cookie, 如果存在就更新， 不存在就生成
     * @param response
     * @param name  cookie名字
     * @param value cookie值
     * @param maxAge cookie生命周期  以秒为单位
     */
    public static void addCookie(HttpServletRequest req, HttpServletResponse response, String name, String value, int maxAge){
        Cookie cookie = getCookieByName(req, name);
        if (cookie != null) {
            cookie.setMaxAge(0);
//    		cookie.setValue(null); // 将请求中对应的cookie值设置为空.
        }
        String valueNew = encode(value);
        logger.info(name + "\togin: " + value + "\tnew: " + valueNew);
        cookie = new Cookie(name, valueNew);
        cookie.setPath("/");
        if(maxAge>0) {
            cookie.setMaxAge(maxAge);
        }
        response.addCookie(cookie);
    }

    /**
     * 根据名字获取cookie， 值需要  URLDecoder.decode(cookie.getValue(), "utf-8"); 解码
     * @param request
     * @param name cookie名字
     * @return
     */
    public static Cookie getCookieByName(HttpServletRequest request, String name){
        Map<String, Cookie> cookieMap = readCookieMap(request);
        if(cookieMap.containsKey(name)){
            Cookie cookie = (Cookie)cookieMap.get(name);
            return cookie;
        }else{
            return null;
        }
    }

    /**
     * 将cookie封装到Map里面， 其中cookie 的值需要解码
     * @param request
     * @return
     */
    public static Map<String, Cookie> readCookieMap(HttpServletRequest request){
        Map<String, Cookie> cookieMap = new HashMap<String, Cookie>();
        Cookie[] cookies = request.getCookies();
        if(null!=cookies){
            for(Cookie cookie : cookies){
                cookieMap.put(cookie.getName(), cookie);
            }
        }
        return cookieMap;
    }

}
