package com.flume;

import java.util.regex.Pattern;

public class Constant {

	/** 统一的编码. */
	public static String ENCODE_GBK = "GBK";

	/** The ENCOD e_ ut f8. */
	public static String ENCODE_UTF8 = "UTF-8";

	/** The ENCOD e_ iso. */
	public static String ENCODE_ISO = "ISO-8859-1";

	/** 请求方式. */
	public static String METHOD_POST = "POST";

	/** 数据传输用的分隔符. */
	public static char CUTCHAR = (char) 1;

	/** 数据内容中换行替换符. */
	public static char CUTCHAR2 = (char) 2;

	/** The CUTCHA r_ str. */
	public static String CUTCHAR_STR = String.valueOf(CUTCHAR);

	/** The CUTCHA r2_ str. */
	public static String CUTCHAR2_STR = String.valueOf(CUTCHAR2);

	/** 换行符. */
	public static String NEWLINE = "\n";

	/** tab符. */
	public static String TABLE = "\t";

	/** 短信内容的换行替换正则. */
	public static Pattern NEWLINE_REGEX = Pattern.compile("\r\n|\n|\r");

	/** sp 短信中存在CUTCHAR2_STR的短信内容正则. */
	public static Pattern CUTCHAR2_REGEX = Pattern.compile(CUTCHAR2_STR);

	/** 优惠券报告文件追加分隔符. */
	public static String SIMPLE_SPILT = ",";

	/** 文件路径中的/符号. */
	public static String FILE_SEPARATOR = "/";

	/** 短信提醒类型 用于账户余额不足的短信发送. */
	public static enum AcountRemindType {
		/** The Normal account. */
		NormalAccount,
		/** The WAP account. */
		WAPAccount
	}

	/** 日期格式. */
	public static String DATE_N_SYMBOL = "yyyyMMdd";

	/** The DAT e_ time. */
	public static String DATE_TIME = "yyyy-MM-dd HH:mm:ss";



    // ----------------------  下面为http 请求时 头可以设置的属性 ---------------------

	/** 前端cookie ID. */
	public static String UID = "admckid";

    /** isBatch 空或不存在则表示当前请求时单事件, 存在为多事件处理,目前未对多事件进行约定处理 */
	public static String ISBATCH = "isBatch";

    /** 默认为空,则发送消息到默认的topic, 如果有值则不发送消息到默认的topic 与 KafkaSink headers.get(TOPIC_HEADER); 共用. */
    public static String SEND_DEFAULT_TOPIC = "sendDefaultTopic";


}
