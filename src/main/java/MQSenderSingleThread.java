import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import org.apache.commons.cli.*;

import javax.jms.*;
import java.io.*;
import java.util.concurrent.*;

public class MQSenderSingleThread {
    static final int BATCH_SIZE = 100;
    final String HOST_NAME = "gimli-a2.it.volvo.net";
    final int PORT = 1435;
    final String CHANNEL_NAME = "WINS.SRV01";
    final String QUEUE_MANAGER_NAME = "GIMLI_A2";
    final String QUEUE_NAME = "WINS.APP.HARDINDIVIDUALPRODUCTGENERAL.IN";

    MQQueueConnectionFactory mqcf;
    QueueConnection qconn;
    QueueSession session;
    Queue queue;
    static String filePath;

    public void sendMessage() throws JMSException, InterruptedException, FileNotFoundException, UnsupportedEncodingException {
        openConnection();
        String messageText = loadMessageText(filePath);

        QueueSession session = qconn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(QUEUE_NAME);
        MessageProducer sender = session.createProducer(queue);

        for (int i = 1; i <= BATCH_SIZE; i++) {
            TextMessage message = session.createTextMessage();
            message.setText(messageText);
            sender.send(message);

            if (i % 1000 == 0) {
                System.out.println(Thread.currentThread().getName() + " sent message: " + i);
            } else if (i == BATCH_SIZE) {
                System.out.println(Thread.currentThread().getName() + " sent message: " + i);
            }
        }
        closeConnection();
    }

    private String loadMessageText(String filePath) throws FileNotFoundException, UnsupportedEncodingException {
        String encoding = "UTF-8";
        File file = new File(filePath);
        Long length = file.length();
        byte[] content = new byte[length.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(content);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            return new String(content, encoding);
        } catch (UnsupportedEncodingException e) {
            System.err.println("The OS does not support " + encoding);
            e.printStackTrace();
            return null;
        }
    }

    private void openConnection() throws JMSException {
        // Obtain the factory factory
        //JmsFactoryFactory jmsFact = JmsFactoryFactory.getInstance();
        mqcf = new MQQueueConnectionFactory();
        mqcf.setHostName(HOST_NAME);
        mqcf.setPort(PORT);
        mqcf.setQueueManager(QUEUE_MANAGER_NAME);
        mqcf.setChannel(CHANNEL_NAME);
        mqcf.setTransportType(WMQConstants.WMQ_CM_CLIENT);
        qconn = mqcf.createQueueConnection();
        qconn.start();
    }

    private void closeConnection() throws JMSException {
        qconn.stop();
        qconn.close();
    }

    public static void main(String[] args) throws InterruptedException, JMSException {
        CommandLine line = parseArguments(args);

        if (line.hasOption("filename")) {
            //System.out.println(line.getOptionValue("filename"));
            filePath = line.getOptionValue("filename");
        }

        MQSenderSingleThread sender = new MQSenderSingleThread();
        try {
            sender.sendMessage();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private static CommandLine parseArguments(String[] args) {
        Options options = getOptions();
        CommandLine line = null;
        CommandLineParser parser = new DefaultParser();

        try {
            line = parser.parse(options, args);

        } catch (ParseException ex) {
            System.err.println("Failed to parse command line arguments");
            System.err.println(ex.toString());
            printAppHelp();

            System.exit(1);
        }
        return line;
    }

    private static Options getOptions() {
        Options options = new Options();
        options.addOption("f", "filename", true, "file name to load data from");
        return options;
    }

    private static void printAppHelp() {
        Options options = getOptions();
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("JavaStatsEx", options, true);
    }

}