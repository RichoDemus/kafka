package org.apache.kafka.common.internals;

import org.apache.kafka.common.KafkaFuture;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;

public class TopicInfoTest {
    @Test
    public void name() throws InterruptedException, ExecutionException, TimeoutException {
        final KafkaFuture<Long> future = new TopicInfo().getInfo("sta-kafka01-combined01.nix.cydmodule.com:9092");

//        final Long size = future.get(30, SECONDS);
        final Long size = future.get();

        System.out.println("internal store is " + size + " bytes");
    }
}