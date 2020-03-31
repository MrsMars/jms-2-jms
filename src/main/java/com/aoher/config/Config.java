package com.aoher.config;

import com.aoher.util.Constants;
import org.ini4j.Wini;

import static com.aoher.util.Constants.JMS_PARAMETER_EXCEPTION_FORMAT;
import static java.lang.String.format;

public class Config {

    public static final String NAME = "jms2jms.ini";

    public static class FROM {
        public static String brokerUri;
        public static String destType;
        public static String destName;
        public static String username;
        public static String password;
        public static String clientId;
        public static String subscriptionName;
    }

    public static class TO {
        public static String brokerUri;
        public static String destType;
        public static String destName;
        public static String username;
        public static String password;
        public static String clientId;
        public static String subscriptionName;
    }

    public static class COMMON {
        public static int TIMEOUT;
    }

    public static void setConfig(Wini ini) {
        if ((COMMON.TIMEOUT = ini.get(Constants.COMMON, Constants.TIMEOUT, Integer.TYPE)) == 0)
            throw new IllegalArgumentException(format(JMS_PARAMETER_EXCEPTION_FORMAT, Constants.COMMON, Constants.TIMEOUT));
        if ((FROM.brokerUri = ini.get(Constants.FROM, Constants.BROKER_URI)) == null)
            throw new IllegalArgumentException(format(JMS_PARAMETER_EXCEPTION_FORMAT, Constants.FROM, Constants.BROKER_URI));
        if ((FROM.destType = ini.get(Constants.FROM, Constants.DEST_TYPE)) == null)
            throw new IllegalArgumentException(format(JMS_PARAMETER_EXCEPTION_FORMAT, Constants.FROM, Constants.DEST_TYPE));
        if ((FROM.destName = ini.get(Constants.FROM, Constants.DEST_NAME)) == null)
            throw new IllegalArgumentException(format(JMS_PARAMETER_EXCEPTION_FORMAT, Constants.FROM, Constants.DEST_NAME));

        FROM.username = ini.get(Constants.FROM, Constants.USERNAME);
        FROM.password = ini.get(Constants.FROM, Constants.PASSWORD);
        FROM.clientId = ini.get(Constants.FROM, Constants.CLIENT_ID);
        FROM.subscriptionName = ini.get(Constants.FROM, Constants.SUBSCRIPTION_NAME);

        if ((TO.brokerUri = ini.get(Constants.TO, Constants.BROKER_URI)) == null)
            throw new IllegalArgumentException(format(JMS_PARAMETER_EXCEPTION_FORMAT, Constants.TO, Constants.BROKER_URI));
        if ((TO.destType = ini.get(Constants.TO, Constants.DEST_TYPE)) == null)
            throw new IllegalArgumentException(format(JMS_PARAMETER_EXCEPTION_FORMAT, Constants.TO, Constants.DEST_TYPE));
        if ((TO.destName = ini.get(Constants.TO, Constants.DEST_NAME)) == null)
            throw new IllegalArgumentException(format(JMS_PARAMETER_EXCEPTION_FORMAT, Constants.TO, Constants.DEST_NAME));

        TO.username = ini.get(Constants.TO, Constants.USERNAME);
        TO.password = ini.get(Constants.TO, Constants.PASSWORD);
        TO.clientId = ini.get(Constants.TO, Constants.CLIENT_ID);
        TO.subscriptionName = ini.get(Constants.TO, Constants.SUBSCRIPTION_NAME);
    }
}
