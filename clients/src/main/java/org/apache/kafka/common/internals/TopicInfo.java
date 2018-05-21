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

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class TopicInfo {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {

        final KafkaFuture<Long> future = new TopicInfo().getInfo("sta-kafka01-combined01.nix.cydmodule.com:9092");

        final Long size = future.get();

        System.out.println("internal store is " + size + " bytes");
    }

    public KafkaFuture<Long> getInfo(String bootstrapServers) {
        logger.info("Getting info for {}", bootstrapServers);

        final AdminClient adminClient = createAdminClient(bootstrapServers);

        // Get a list of brokers
        final KafkaFuture<Collection<Node>> describeCluster = adminClient.describeCluster().nodes();

//        describeCluster.thenApply(asd -> {
//            System.out.println(asd);
//            return 1;
//        });

        // Once we have a list of brokers, get the topic/partition info for each broker
        final KafkaFuture<Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>>> describedirsFuture = describeCluster
                .thenCompose(nodes -> {
                    final List<Integer> brokers = nodes.stream().map(Node::id).collect(toList());

                    final DescribeLogDirsResult x = adminClient.describeLogDirs(brokers);

                    // a future for each broker, all() wraps those into a future
                    return x.all();
                });


        final KafkaFuture<Long> resultfuture = describedirsFuture.thenApply(result -> {
//            adminClient.close();
            final LongAdder size = new LongAdder();
            result.forEach((key, logDirInfoMap) -> {
//            logger.info("" + key);
                logDirInfoMap.forEach((key2, logDirInfo) -> {
//                logger.info("\t" + key2);
                    logDirInfo.replicaInfos.forEach((topicPartition, replicaInfo) -> {
//                    logger.info("\t\t" + topicPartition);
//                    logger.info("\t\t\t" + replicaInfo);
//                    logger.info("{}-{} is {}b", topicPartition.topic(), topicPartition.partition(), replicaInfo.size);
                        if (topicPartition.topic().equalsIgnoreCase("gaming-fort-fort-internal-store-changelog")) {
                            size.add(replicaInfo.size);
                        }
                    });
                });
            });
            System.out.println("Done!!!");
            return size.longValue();
        });

        resultfuture.whenComplete((val, e) -> {
            System.out.println("closing");
            new Thread(() -> adminClient.close(1L, SECONDS)).start();
//            adminClient.close(1L, SECONDS);
            System.out.println("closed");
        });

//        try {
//            System.out.println("Wait");
//            System.out.println(describedirsFuture.get());
//            System.out.println("done");
//        } catch (Throwable e) {
//            e.printStackTrace();
//        }

        return resultfuture;

//        describeCluster.thenApply(nodes -> {
//            final List<Integer> brokers = nodes.stream().map(Node::id).collect(toList());
//
//            final DescribeLogDirsResult x = adminClient.describeLogDirs(brokers);
//            adminClient.close();
//
//            final KafkaFuture<Long> sizeFutures = x.all().thenApply(result -> {
//                final LongAdder size = new LongAdder();
//                result.forEach((key, logDirInfoMap) -> {
////            logger.info("" + key);
//                    logDirInfoMap.forEach((key2, logDirInfo) -> {
////                logger.info("\t" + key2);
//                        logDirInfo.replicaInfos.forEach((topicPartition, replicaInfo) -> {
////                    logger.info("\t\t" + topicPartition);
////                    logger.info("\t\t\t" + replicaInfo);
////                    logger.info("{}-{} is {}b", topicPartition.topic(), topicPartition.partition(), replicaInfo.size);
//                            if (topicPartition.topic().equalsIgnoreCase("gaming-fort-fort-internal-store-changelog")) {
//                                size.add(replicaInfo.size);
//                            }
//                        });
//                    });
//                });
//                return size.longValue();
//            });
//            return 1L;
//        });

        /*.thenApply(result -> {
            final LongAdder size = new LongAdder();
            adminClient.close();
            result.forEach((key, logDirInfoMap) -> {
//            logger.info("" + key);
                logDirInfoMap.forEach((key2, logDirInfo) -> {
//                logger.info("\t" + key2);
                    logDirInfo.replicaInfos.forEach((topicPartition, replicaInfo) -> {
//                    logger.info("\t\t" + topicPartition);
//                    logger.info("\t\t\t" + replicaInfo);
//                    logger.info("{}-{} is {}b", topicPartition.topic(), topicPartition.partition(), replicaInfo.size);
                        if (topicPartition.topic().equalsIgnoreCase("gaming-fort-fort-internal-store-changelog")) {
                            size.add(replicaInfo.size);
                        }
                    });
                });
            });
        })*/
//        final List<Integer> brokers;
//        try {
//            brokers = describeCluster.get().stream().map(Node::id).collect(toList());
//        } catch (InterruptedException | ExecutionException e) {
//            throw new RuntimeException();
//        }
//
//        final String[] split = bootstrapServers.split(",");
//        final DescribeLogDirsResult x = adminClient.describeLogDirs(brokers);
//
//        try {
//            final Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> result = x.all().get();
//        } catch (InterruptedException | ExecutionException e) {
//            throw new RuntimeException();
//        }
//
//        final LongAdder size = new LongAdder();
//        adminClient.close();
//        result.forEach((key, logDirInfoMap) -> {
////            logger.info("" + key);
//            logDirInfoMap.forEach((key2, logDirInfo) -> {
////                logger.info("\t" + key2);
//                logDirInfo.replicaInfos.forEach((topicPartition, replicaInfo) -> {
////                    logger.info("\t\t" + topicPartition);
////                    logger.info("\t\t\t" + replicaInfo);
////                    logger.info("{}-{} is {}b", topicPartition.topic(), topicPartition.partition(), replicaInfo.size);
//                    if (topicPartition.topic().equalsIgnoreCase("gaming-fort-fort-internal-store-changelog")) {
//                        size.add(replicaInfo.size);
//                    }
//                });
//            });
//        });


    }

    private AdminClient createAdminClient(String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(props);
    }
}
