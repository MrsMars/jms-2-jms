package com.aoher.factory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.pool.PooledConnectionFactory;

import javax.jms.ConnectionFactory;

public class JMSConnectionFactory {

    public static ConnectionFactory getActiveMQFactory(String url, String username, String password) {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(username, password, url);

        RedeliveryPolicy policy = new RedeliveryPolicy();
        policy.setMaximumRedeliveries(-1);

        factory.setRedeliveryPolicy(policy);
        return factory;
    }

    public static ConnectionFactory getAWSActiveMQFactory(String url, String username, String password) {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
        if (!username.isEmpty()) {
            factory.setUserName(username);
        }
        if (!password.isEmpty()) {
            factory.setPassword(password);
        }

        PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory();
        pooledConnectionFactory.setConnectionFactory(factory);
        return pooledConnectionFactory;
    }

    private JMSConnectionFactory() {
    }
}
