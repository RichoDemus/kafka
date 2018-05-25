package org.apache.kafka.common.internals;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class TopicInfo2 {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {

        final String topic = "test";
        final KafkaFuture<KafkaFuture<Long>> future = new TopicInfo2().getInfo("localhost:9092", topic);

        final Long size = future.get(1, MINUTES).get(1, MINUTES);

        LoggerFactory.getLogger("Main").info("Topic {} is {} bytes", topic, size);
        System.out.println("Topic " + topic + " is " + size + " bytes");
    }

    public KafkaFuture<KafkaFuture<Long>> getInfo(String bootstrapServers, String topic) {
        logger.info("Getting info for {}", bootstrapServers);

        final AdminClient adminClient = createAdminClient(bootstrapServers);

        // Get a list of brokers
        final KafkaFuture<Collection<Node>> describeCluster = adminClient.describeCluster().nodes();


        // Once we have a list of brokers, get the topic/partition info for each broker
        final KafkaFuture<KafkaFuture<Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>>>> describedirsFuture = describeCluster
                .thenApply(nodes -> {
                    final List<Integer> brokers = nodes.stream().map(Node::id).collect(toList());

                    final DescribeLogDirsResult x = adminClient.describeLogDirs(brokers);

                    // a future for each broker, all() wraps those into a future
                    return x.all();
                });


        final KafkaFuture<KafkaFuture<Long>> resultfuture = describedirsFuture.thenApply(resultOuter -> resultOuter.thenApply(result -> {
            final LongAdder size = new LongAdder();
            result.forEach((key, logDirInfoMap) -> {
                logDirInfoMap.forEach((key2, logDirInfo) -> {
                    logDirInfo.replicaInfos.forEach((topicPartition, replicaInfo) -> {
                        if (topicPartition.topic().equalsIgnoreCase(topic)) {
                            size.add(replicaInfo.size);
                        }
                    });
                });
            });
            System.out.println("Done!!!");
            return size.longValue();
        }));

        resultfuture.whenComplete((val, e) -> {
            System.out.println("closing");
            new Thread(() -> adminClient.close(1L, SECONDS)).start();
            System.out.println("closed");
        });

        return resultfuture;


    }

    private AdminClient createAdminClient(String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(props);
    }
}
