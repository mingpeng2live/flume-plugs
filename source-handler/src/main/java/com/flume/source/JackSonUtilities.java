package com.flume.source;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flume.Constant;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

/*
 * json 反序列化工具
 */
@SuppressWarnings("deprecation")
public class JackSonUtilities {

	static Logger logger = LoggerFactory.getLogger(JackSonUtilities.class);
	private static ObjectMapper m = new ObjectMapper();

	public static <T> T toBean(String jsonAsString, Class<T> pojoClass)
			throws JsonGenerationException {
		try {
			if(StringUtils.isEmpty(jsonAsString))
				return null ;
			return m.readValue(jsonAsString, pojoClass);
		} catch (Exception e) {
			logger.error( e.getMessage() , e );
			return null ;
		}
	}

	public static String toString(Object pojo)  {
		try {
			StringWriter sw = new StringWriter();
			m.writeValue(sw, pojo);
			return sw.toString();
		} catch (Exception e) {
			logger.error( e.getMessage() , e );
			return null ;
		}

	}

	/** 转换json字符串 不包括类型信息,如果转换出现异常返回空字符串 */
	public static final String toJsonString(Object obj) {
		String json = "";
		try {
			json = m.writeValueAsString(obj);
		} catch (Exception e) {
			logger.error(obj.getClass().getName() + "对象转换为json出错！", e);
		}
		return json;
	}


	public static <T> Map<String, T> toMap(String jsonAsString)
			throws JsonGenerationException {
		try {
			return m.readValue(jsonAsString,
					new TypeReference<Map<String, T>>() {
					});
		} catch (Exception e) {
			logger.error( e.getMessage() , e );
			return null ;
		}
	}

	public static <T> Map<String, T>[] toMapArray(String jsonAsString)
			throws JsonGenerationException {
		try {
			return m.readValue(jsonAsString,
					new TypeReference<Map<String, T>[]>() {
					});
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return null;
		}
	}

	/** 转换json字符串 如果转换出现异常返回长度为0的字节数组 */
	public static final byte[] toBytes(Object obj) {
		byte[] jsonBytes = new byte[0];
		try {
			jsonBytes = m.writeValueAsBytes(obj);
		} catch (Exception e) {
			logger.error(obj.getClass().getName() + "对象转换为byte出错！", e);
		}
		return jsonBytes;
	}


    /** byte[] */
    public static final JsonNode readJsonNode(byte[] content) throws Exception {
        return m.readTree(content);
    }

    /** 将InputStream 转换为JsonNode对象 */
    public static final JsonNode readJsonNode(InputStream in) throws IOException {
        return m.readTree(read(in));
    }

    /** 将InputStream */
    public static final <T> T readToClass(InputStream in, Class<T> tClass) throws IOException {
        return m.readValue(read(in), tClass);
    }

    /** 包装request的输入流(且必须包装，转换速度有很大的区别)，必须设置UTF-8编码，不然中文会乱码 */
    public static final BufferedReader read(InputStream in) throws UnsupportedEncodingException {
        return new BufferedReader(new InputStreamReader(in, Constant.ENCODE_UTF8));
    }
}
