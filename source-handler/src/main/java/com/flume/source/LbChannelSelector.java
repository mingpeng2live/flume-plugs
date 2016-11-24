package com.flume.source;

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.AbstractChannelSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Load balance channel selector.
 *
 * @author pengming  
 * @date 2016年11月22日 16:38:49
 * @description
 */
public class LbChannelSelector extends AbstractChannelSelector {

    /**
     * The constant CONFIG_MULTIPLEX_HEADER_NAME.
     */
    public static final String CONFIG_MULTIPLEX_HEADER_NAME = "header";
    /**
     * The constant DEFAULT_MULTIPLEX_HEADER.
     */
    public static final String DEFAULT_MULTIPLEX_HEADER = "flume.selector.header";
    /**
     * The constant CONFIG_PREFIX_MAPPING.
     */
    public static final String CONFIG_PREFIX_MAPPING = "mapping.";

    /**
     * The constant CONFIG_DEFAULT_CHANNEL.
     */
    public static final String CONFIG_DEFAULT_CHANNEL = "default";

    public static final String CONFIG_PREFIX_POLLING = "polling";
    public static final String CONFIG_PREFIX_COPY = "copy";
    /**
     * The constant CONFIG_PREFIX_OPTIONAL.
     */
    public static final String CONFIG_PREFIX_OPTIONAL = "optional";

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(LbChannelSelector.class);

    private static final List<Channel> EMPTY_LIST = Collections.emptyList();

    private String headerName;

    private Map<String, List<Channel>> channelMapping;
    private Map<String, List<Channel>> optionalChannels;
    private List<Channel> defaultChannels;


    /** 复制事件到对应的channels中 */
    private List<Channel> copyChannels;

    /** 轮询loadBalanceChannels 的下标 */
    private volatile int index = 0;

    /** 轮询负载均衡到该list 中的 channel */
    private List<Channel> loadBalanceChannels;


    @Override
    public List<Channel> getRequiredChannels(Event event) {
        /** 根据header 配置值来找到对应处理的 channel */
        String headerValue = event.getHeaders().get(headerName);
        if (StringUtils.isNotEmpty(StringUtils.trim(headerValue))) {
            List<Channel> channels = channelMapping.get(headerValue);
            if (CollectionUtils.isNotEmpty(channels)) {
                return channels;
            }
        }

        /** 复制 */
        if (CollectionUtils.isNotEmpty(copyChannels)) {
            return copyChannels;
        }

        /** 轮询选择 channel */
        if (CollectionUtils.isNotEmpty(loadBalanceChannels)) {
            return getLbChannels();
        }

        return defaultChannels;
    }


    public synchronized List<Channel> getLbChannels() {
        List<Channel> channels = new ArrayList<>(1);
        channels.add(loadBalanceChannels.get(index));
        if (index++ >= (loadBalanceChannels.size() - 1)) {
            index = 0;
        }
        return channels;
    }


    @Override
    public List<Channel> getOptionalChannels(Event event) {
        String hdr = event.getHeaders().get(headerName);
        List<Channel> channels = optionalChannels.get(hdr);

        if (channels == null) {
            channels = EMPTY_LIST;
        }
        return channels;
    }



    @Override
    public void configure(Context context) {
        Map<String, Channel> channelNameMap = getChannelNameMap();
        /** 设置header */
        this.headerName = context.getString(CONFIG_MULTIPLEX_HEADER_NAME, DEFAULT_MULTIPLEX_HEADER);
        /** 默认处理事件的channel */
        defaultChannels = getChannelListFromNames(context.getString(CONFIG_DEFAULT_CHANNEL), channelNameMap);
        if (CollectionUtils.isEmpty(defaultChannels)) {
            defaultChannels = EMPTY_LIST;
        }

        /** 配置负载均衡,轮询的channel */
        loadBalanceChannels = getChannelListFromNames(context.getString(CONFIG_PREFIX_POLLING), channelNameMap);
        if (CollectionUtils.isEmpty(loadBalanceChannels)) {
            loadBalanceChannels = EMPTY_LIST;
        }

        /** 复制事件到对应的channel 中 */
        copyChannels = getChannelListFromNames(context.getString(CONFIG_PREFIX_COPY), channelNameMap);
        if (CollectionUtils.isEmpty(copyChannels)) {
            copyChannels = EMPTY_LIST;
        }

        /** 设置header 值对应的 channel 映射处理 */
        Map<String, String> mapConfig = context.getSubProperties(CONFIG_PREFIX_MAPPING);
        channelMapping = new HashMap<String, List<Channel>>();
        for (String headerValue : mapConfig.keySet()) {
            List<Channel> configuredChannels = getChannelListFromNames(mapConfig.get(headerValue), channelNameMap);

            //This should not go to default channel(s)
            //because this seems to be a bad way to configure.
            if (configuredChannels.size() == 0) {
                throw new FlumeException("No channel configured for when " + "header value is: " + headerValue);
            }

            if (channelMapping.put(headerValue, configuredChannels) != null) {
                throw new FlumeException("Selector channel configured twice");
            }
        }
        //If no mapping is configured, it is ok.
        //All events will go to the default channel(s).


        /** 配置 其他规则, 如果与默认配置channel冲突则删除, 发送到对应的channel 与 ChannelProcessor 处理相关 */
        Map<String, String> optionalChannelsMapping = context.getSubProperties(CONFIG_PREFIX_OPTIONAL + ".");

        optionalChannels = new HashMap<String, List<Channel>>();
        for (String hdr : optionalChannelsMapping.keySet()) {
            List<Channel> confChannels = getChannelListFromNames(optionalChannelsMapping.get(hdr), channelNameMap);
            if (confChannels.isEmpty()) {
                confChannels = EMPTY_LIST;
            }
            //Remove channels from optional channels, which are already
            //configured to be required channels.

            List<Channel> reqdChannels = channelMapping.get(hdr);
            //Check if there are required channels, else defaults to default channels
            if (CollectionUtils.isEmpty(reqdChannels)) {
                reqdChannels = defaultChannels;
            }
            for (Channel c : reqdChannels) {
                if (confChannels.contains(c)) {
                    confChannels.remove(c);
                }
            }
            if (optionalChannels.put(hdr, confChannels) != null) {
                throw new FlumeException("Selector channel configured twice");
            }
        }

    }

}