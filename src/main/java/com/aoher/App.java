package com.aoher;

import com.aoher.config.Config;
import com.aoher.util.JMSThread;
import org.ini4j.Wini;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        Wini ini = new Wini(new File(Config.NAME));
        Config.setConfig(ini);

        log.info("Ini => {}", ini);

        JMSThread jms = new JMSThread();
        Thread jmsThread = new Thread(jms, "control thread");
        jmsThread.start();
        jmsThread.join();
    }
}
