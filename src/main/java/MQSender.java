import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import org.apache.commons.cli.*;

import javax.jms.*;
import java.io.*;
import java.util.concurrent.*;

public class MQSender {
    static final int BATCH_SIZE = 1;
    static final int TASK_COUNT = 5;
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

    private static CountDownLatch cdl = new CountDownLatch(TASK_COUNT);

    public void sendMessage() throws JMSException, InterruptedException, FileNotFoundException, UnsupportedEncodingException {
        openConnection();
        String messageText = loadMessageText(filePath);

        ExecutorService threadPool = new ThreadPoolExecutor(2,
                5,
                5,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(5),
                new NamedThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy()
        );

        for(int i=0;i<TASK_COUNT;i++){
            threadPool.execute(()->{
                for(int j=0;j<BATCH_SIZE;j++){
                    try {
                        //create producer
                        session = qconn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                        queue = session.createQueue(QUEUE_NAME);
                        MessageProducer sender = session.createProducer(queue);

                        //create message
                        TextMessage message = session.createTextMessage();
                        message.setText(messageText);

                        sender.send(message);
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                    finally {
                        cdl.countDown();
                    }
                }
            });
        }
        cdl.await();
        threadPool.shutdown();
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

        MQSender sender = new MQSender();
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