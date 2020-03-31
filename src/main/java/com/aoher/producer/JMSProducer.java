package com.aoher.producer;

import com.aoher.config.Config;
import com.aoher.factory.JMSConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Exchanger;

import static com.aoher.util.Constants.QUEUE;

public class JMSProducer extends Thread implements ExceptionListener {

    private static final Logger log = LoggerFactory.getLogger(JMSProducer.class);

    private Connection connection;
    private Session session;
    private Destination destination;
    private MessageProducer producer;
    private String destTo;

    private boolean isConnected = false;
    private BlockingQueue<Message> queue;
    private Exchanger<Boolean> exchanger;

    public JMSProducer(String destTo, BlockingQueue<Message> queue, Exchanger<Boolean> exchanger) {
        this.destTo = destTo;
        this.queue = queue;
        this.exchanger = exchanger;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void connect() {
        log.info("producer -> [{}] connect", destTo);
        try {
            connection = JMSConnectionFactory.getAWSActiveMQFactory(
                    Config.TO.brokerUri, Config.TO.username, Config.TO.password
            ).createConnection();

            if (!Config.TO.clientId.isEmpty()) {
                connection.setClientID(Config.TO.clientId);
            }

            connection.setExceptionListener(this);
            session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);

            if (Config.TO.destType.equals(QUEUE)) {
                destination = session.createQueue(Config.TO.destName);
            } else {
                destination = session.createTopic(Config.TO.destName);
            }

            producer = session.createProducer(destination);
            isConnected = true;
        } catch (JMSException e) {
            log.error("producer -> [{}] connect exception: {}", destTo, e.getMessage());
        }
    }

    public void disconnect() {
        log.info("producer -> [{}] disconnect", destTo);
        try {
            if (producer != null) {
                producer.close();
                producer = null;
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
            log.error("producer -> [{}] disconnect exception: {}", destTo, e.getMessage());
        }
    }

    public void send(String path, String file) throws JMSException, IOException {
        producer.send(session.createTextMessage(new String(Files.readAllBytes(Paths.get(path, file)))));
    }

    @Override
    public void onException(JMSException e) {
        log.error("producer -> [{}] onException: {}", destTo, e.getMessage());

        disconnect();
        isConnected = false;
        Thread.currentThread().interrupt();
    }

    @Override
    public void run() {
        Thread.currentThread().setName("jms producer");
        Message message = null;
        while (true) {
            try {
                if (this.isConnected) {
                    message = queue.take();

                    log.debug("producer -> exchange get {}", message.getJMSMessageID());
                    message.setJMSCorrelationID(message.getJMSMessageID());

                    producer.send(message);
                    log.debug("producer -> [{}] put: {}", destTo, message.getJMSCorrelationID());

                    session.commit();
                    log.info("producer -> [{}] commit: {}", destTo, message.getJMSCorrelationID());

                    exchanger.exchange(true);

                } else {
                    try {
                        Thread.sleep(Config.COMMON.TIMEOUT);
                    } catch (InterruptedException e) {
                        log.error("producer -> InterruptedException {}", e.getMessage());
                    }
                }
            } catch (JMSException | InterruptedException e) {
                try {
                    queue.clear();
                    exchanger.exchange(false);

                    session.rollback();
                    log.error("producer -> [{}] rollback", destTo);

                    Thread.sleep(Config.COMMON.TIMEOUT);
                } catch (JMSException | InterruptedException ex) {
                    log.error("producer -> [{}] JMSException | InterruptedException", ex.getMessage());
                }
            }
        }
    }
}
