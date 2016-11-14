package com.flume;

import org.apache.commons.collections.MapUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.nio.charset.UnsupportedCharsetException;
import java.util.*;

public class HTTPSourceJsonHandler implements HTTPSourceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HTTPSourceJsonHandler.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(HttpServletRequest request) throws Exception {
        BufferedReader reader = request.getReader();
        String charset = request.getCharacterEncoding();
        //UTF-8 is default for JSON. If no charset is specified, UTF-8 is to
        //be assumed.
        if (charset == null) {
            LOG.debug("Charset is null, default charset of UTF-8 will be used.");
            charset = "UTF-8";
        } else if (!(charset.equalsIgnoreCase("utf-8")
                || charset.equalsIgnoreCase("utf-16")
                || charset.equalsIgnoreCase("utf-32"))) {
            LOG.error("Unsupported character set in request {}. "
                    + "JSON handler supports UTF-8, "
                    + "UTF-16 and UTF-32 only.", charset);
            throw new UnsupportedCharsetException("JSON handler supports UTF-8, "
                    + "UTF-16 and UTF-32 only.");
        }

        /*
         * Gson throws Exception if the data is not parseable to JSON.
         * Need not catch it since the source will catch it and return error.
         */
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
            BufferedReader br = new BufferedReader(reader);
            StringBuffer content = new StringBuffer();
            String item = "";
            while ((item = br.readLine()) != null) {
                content.append(item);
            }
            body = content.toString().getBytes(charset);
        } else if (request.getMethod().equals("GET") && MapUtils.isNotEmpty(request.getParameterMap())){
            Map<String, String> parameterMap = request.getParameterMap();
            String content = JackSonUtilities.toJsonString(parameterMap);
            body = content.getBytes(charset);
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