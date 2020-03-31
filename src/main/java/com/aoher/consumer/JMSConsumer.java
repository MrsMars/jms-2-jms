package com.aoher.consumer;

import com.aoher.config.Config;
import com.aoher.factory.JMSConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.aoher.util.Constants.QUEUE;

public class JMSConsumer extends Thread implements ExceptionListener {

    private static final Logger log = LoggerFactory.getLogger(JMSConsumer.class);

    private Connection connection;
    private Session session;
    private Destination destination;
    private MessageConsumer consumer;
    private String destFrom;
    private boolean isConnected = false;
    private boolean isReceiving = false;
    private BlockingQueue<Message> queue;
    private Exchanger<Boolean> exchanger;

    public JMSConsumer(String destFrom, BlockingQueue<Message> queue, Exchanger<Boolean> exchanger) {
        this.destFrom = destFrom;
        this.queue = queue;
        this.exchanger = exchanger;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void connect() {
        log.info("consumer -> [{}] connect", destFrom);
        try {
            connection = JMSConnectionFactory.getActiveMQFactory(Config.FROM.brokerUri, Config.FROM.username, Config.FROM.password)
                    .createConnection();

            if (!Config.FROM.clientId.isEmpty()) {
                connection.setClientID(Config.FROM.clientId);
            }
            connection.setExceptionListener(this);
            session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);

            if (Config.FROM.destType.equals(QUEUE)) {
                destination = session.createQueue(Config.FROM.destName);
            } else {
                destination = session.createTopic(Config.FROM.destName);
            }

            if (Config.FROM.subscriptionName.isEmpty()) {
                consumer = session.createConsumer(destination);
            } else {
                consumer = session.createDurableSubscriber((Topic) destination, Config.FROM.subscriptionName);
            }

            isConnected = true;
            startReceive();
        } catch (JMSException e) {
            log.error("consumer -> [{}] connect exception: {}", destFrom, e.getMessage());
        }
    }

    public void startReceive() {
        log.debug("consumer -> [{}] start receive", destFrom);
        try {
            isReceiving = true;
            if (connection != null) {
                connection.start();
            }
        } catch (JMSException e) {
            log.error("consumer -> [{}] start receive exception: {}", destFrom, e.getMessage());
        }
    }

    public void stopReceive() {
        log.debug("consumer -> [{}] stop receive", destFrom);
        try {
            if (consumer != null) {
                consumer.setMessageListener(null);
            }
            if (connection != null) {
                connection.stop();
            }

            isReceiving = false;
        } catch (JMSException e) {
            log.error("consumer -> [{}] stop receive exception: {}", destFrom, e.getMessage());
        } finally {
            isReceiving = false;
        }
    }

    public void disconnect() {
        log.info("consumer -> [{}] disconnect", destFrom);
        try {
            stopReceive();

            if (consumer != null) {
                consumer.close();
                consumer = null;
            }

            if (session != null) {
                session.close();
                session = null;
            }

            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (JMSException e) {
            log.error("consumer -> [{}] disconnect exception: {}", destFrom, e.getMessage());
        }
    }

    @Override
    public void onException(JMSException e) {
        log.error("consumer -> [{}] onException: {}", destFrom, e.getMessage());

        queue.clear();

        disconnect();
        isConnected = false;
        Thread.currentThread().interrupt();
    }

    @Override
    public void run() {
        Thread.currentThread().setName("jms consumer");
        Message message;
        while (true) {
            try {
                if (this.isConnected) {
                    if ((message = consumer.receive()) != null) {
                        log.debug("consumer -> [{}] get: {}]", destFrom, message.getJMSMessageID());
                        queue.put(message);
                        log.debug("consumer -> exchange put {}}", message.getJMSMessageID());

                        if (exchanger.exchange(null, Config.COMMON.TIMEOUT, TimeUnit.MILLISECONDS)) {
                            session.commit();
                            log.info("consumer -> [{}}] commit: {}", destFrom, message.getJMSCorrelationID());
                        } else {
                            queue.clear();
                            session.rollback();
                            log.error("consumer -> [{}] rollback: {}", destFrom, message.getJMSMessageID());
                        }
                    }
                } else {
                    try {
                        Thread.sleep(Config.COMMON.TIMEOUT);
                    } catch (InterruptedException e) {
                    }
                }
            } catch (JMSException | InterruptedException | TimeoutException e) {
                try {
                    queue.clear();
                    session.rollback();
                    log.error("consumer -> [{}] rollback", destFrom);
                    Thread.sleep(Config.COMMON.TIMEOUT);
                } catch (JMSException | InterruptedException e1) {
                }
            }
        }
    }
}
