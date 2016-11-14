package com.flume;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.Map;

/*
 * json 反序列化工具
 */
@SuppressWarnings("deprecation")
public class JackSonUtilities {

	static Logger logger = LoggerFactory.getLogger(com.flume.JackSonUtilities.class);
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

}
