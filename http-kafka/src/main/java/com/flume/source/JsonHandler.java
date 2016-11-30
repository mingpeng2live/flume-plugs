package com.flume.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.flume.Constant;
import org.apache.commons.collections.MapUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.*;


/**
 * flume 处理HTTP请求源的数据 转变为内置事件.
 *
 * @author pengming  
 * @date 2016年11月15日 18:38:54
 * @description
 */
public class JsonHandler implements HTTPSourceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(JsonHandler.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(HttpServletRequest request) throws Exception {
        List<Event> eventList = new ArrayList<Event>(1);
        SimpleEvent je = new SimpleEvent();
        eventList.add(je);

        /** 获取请求中的headers */
        Map<String, String> headers = new HashMap<String, String>();
        Enumeration headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String key = (String) headerNames.nextElement();
            String value = request.getHeader(key);
            headers.put(key, value);
        }
        /** 当请求中没有cookie ID 时, 在HTTPSource中设置后需要将该值设置到 header中 */
        if (request.getAttribute(Constant.UID) != null) {
            headers.put(Constant.UID, (String) request.getAttribute(Constant.UID));
        }
        je.setHeaders(headers);

        /** 设置主体 */
        byte[] body = new byte[0];
        if (request.getMethod().equals("POST")) {
            JsonNode jsonNode = JackSonUtilities.readJsonNode(request.getInputStream());
            body = jsonNode.toString().getBytes();
        } else if (request.getMethod().equals("GET") && MapUtils.isNotEmpty(request.getParameterMap())){
            Map<String, String> parameterMap = request.getParameterMap();
            body = JackSonUtilities.toBytes(parameterMap);
        }
        je.setBody(body);

        return eventList;
    }

    @Override
    public void configure(Context context) {
    }

}