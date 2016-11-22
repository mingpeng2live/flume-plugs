package com.flume.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.flume.Constant;
import org.apache.commons.collections.MapUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.*;


/**
 * flume 处理HTTP请求源的数据.
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
        String charset = Constant.ENCODE_UTF8;
        request.setCharacterEncoding(charset);

        List<Event> eventList = new ArrayList<Event>(1);
        JSONEvent je = new JSONEvent();
        eventList.add(je);
        // 获取请求中的headers
        Map<String, String> headers = new HashMap<String, String>();
        Enumeration headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String key = (String) headerNames.nextElement();
            String value = request.getHeader(key);
            headers.put(key, value);
        }
        je.setHeaders(headers);

        // 设置主体
        byte[] body = new byte[0];
        if (request.getMethod().equals("POST")) {
            JsonNode jsonNode = JackSonUtilities.readJsonNode(request.getInputStream());
            body = jsonNode.toString().getBytes();
        } else if (request.getMethod().equals("GET") && MapUtils.isNotEmpty(request.getParameterMap())){
            Map<String, String> parameterMap = request.getParameterMap();
            body = JackSonUtilities.toBytes(parameterMap);
        }
        je.setBody(body);
        je.setCharset(charset);

        return getSimpleEvents(eventList);
    }

    @Override
    public void configure(Context context) {
    }

    private List<Event> getSimpleEvents(List<Event> events) {
        List<Event> newEvents = new ArrayList<Event>(events.size());
        for (Event e : events) {
            newEvents.add(EventBuilder.withBody(e.getBody(), e.getHeaders()));
        }
        return newEvents;
    }
}