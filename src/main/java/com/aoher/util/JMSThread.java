package com.aoher.util;

import com.aoher.config.Config;
import com.aoher.consumer.JMSConsumer;
import com.aoher.producer.JMSProducer;

import javax.jms.Message;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Exchanger;

public class JMSThread implements Runnable {

    private JMSConsumer consumer;
    private JMSProducer producer;

    private BlockingQueue<Message> exchangeData = new ArrayBlockingQueue<>(1);
    private Exchanger<Boolean> exchangeResult = new Exchanger<>();

    private boolean isClosed = false;

    public JMSThread() {
        producer = new JMSProducer(Config.TO.destName, exchangeData, exchangeResult);
        producer.start();

        consumer = new JMSConsumer(Config.FROM.destName, exchangeData, exchangeResult);
        consumer.start();
    }

    @Override
    public void run() {
        while (!isClosed) {
            if (!consumer.isConnected()) {
                consumer.connect();
            }

            if (!producer.isConnected()) {
                producer.connect();
            }

            try {
                Thread.sleep(Config.COMMON.TIMEOUT);
            } catch (InterruptedException e) {
            }
        }
    }

    public void stop() {
        consumer.stopReceive();
    }

    public void start() {
        consumer.startReceive();
    }

    public void close() {
        isClosed = true;
        consumer.disconnect();
        producer.disconnect();
    }
}
