package com.flume.util;

import com.flume.source.JackSonUtilities;

import java.util.*;


/**
 * 参数构建类
 *
 * @author ming.peng
 * @date 2012-12-6
 * @since 2.2.0
 */
public class JsonMap implements Map<String, Object> {

    private static final int INITIAL_CAPACITY = 10;

    protected final Map<String, Object> json;


    public JsonMap() {
        this(INITIAL_CAPACITY, false);
    }

    public JsonMap(Map<String, Object> map) {
        this.json = map;
    }

    public JsonMap(boolean isOrder) {
        this(INITIAL_CAPACITY, isOrder);
    }

    public JsonMap(int capacity) {
        this(capacity, false);
    }

    public JsonMap(int capacity, boolean isOrder) {
        if (isOrder) {
            json = new LinkedHashMap<String, Object>(capacity);
        } else {
            json = new HashMap<String, Object>(capacity);
        }
    }

    public int size() {
        return json.size();
    }

    public boolean isEmpty() {
        return json.isEmpty();
    }

    public boolean containsKey(Object key) {
        return json.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return json.containsValue(value);
    }

    public Object get(Object key) {
        return json.get(key);
    }

    public void putAll(Map<? extends String, ? extends Object> m) {
        json.putAll(m);
    }

    public void clear() {
        json.clear();
    }

    public Object remove(Object key) {
        return json.remove(key);
    }

    public Set<String> keySet() {
        return json.keySet();
    }

    public Collection<Object> values() {
        return json.values();
    }

    public Set<Entry<String, Object>> entrySet() {
        return json.entrySet();
    }

    public Object clone() {
        return new JsonMap(new HashMap<String, Object>(json));
    }

    public boolean equals(Object obj) {
        return this.json.equals(obj);
    }

    public int hashCode() {
        return this.json.hashCode();
    }

    /**
     * 转换json字符串
     *
     * @return
     */
    public String toJsonString() {
        return JackSonUtilities.toJsonString(this.json);
    }

    /**
     * 添加参数
     */
    public JsonMap put(String key, Object obj) {
        json.put(key, obj);
        return this;
    }

    /**
     * 得到map对象
     *
     * @return
     */
    public Map<String, Object> get() {
        return json;
    }

    /**
     * 转换参数
     *
     * @param keyValue 参数需以键值对的形式存在键值必须为String
     * @return
     */
    public JsonMap adds(Object... keyValue) {
        return addParams(this, keyValue);
    }

    /**
     * 参数类型初始化
     *
     * @return
     */
    public static JsonMap start() {
        return new JsonMap();
    }

    /**
     * 参数类型初始化
     *
     * @param key 键名
     * @param obj 值
     * @return
     */
    public static JsonMap start(String key, Object obj) {
        return start().put(key, obj);
    }

    /**
     * 转换参数
     *
     * @param keyValue 参数需以键值对的形式存在键值必须为String
     * @return
     */
    public static JsonMap addParams(Object... keyValue) {
        return addParams(new JsonMap(), keyValue);
    }

    /**
     * 转换参数
     *
     * @param params   类型实例
     * @param keyValue 参数
     * @return
     */
    private static JsonMap addParams(JsonMap params, Object... keyValue) {
        if (keyValue.length == 0 && keyValue.length % 2 != 0)
            throw new RuntimeException("所传递的参数个数必须为偶数且不能为0，参数长度：" + keyValue.length);
        for (int i = 0; i < keyValue.length; i += 2) {
            params.put((String) keyValue[i], keyValue[i + 1]);
        }
        return params;
    }

}
