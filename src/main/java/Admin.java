import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Admin {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"ip:9092");
        AdminClient adminClient = AdminClient.create(properties);
        ListTopicsResult topics =adminClient.listTopics();
        //topics.names().get().forEach(System.out::println);
        Set<String> t = topics.names().get();
        String[] array = {};
        array = t.toArray(new String[t.size()]);


        DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList(array));
        Map<String, KafkaFuture<TopicDescription>>  values = result.values();
        //KafkaFuture<TopicDescription> topicDescription = values.get("cis.router.lightind");
        //int partitions = topicDescription.get().partitions().size();
        //System.out.println(topicDescription.get().name()+ " " + partitions);
        for (int i =0; i<array.length;i++) {
            KafkaFuture<TopicDescription> topicDescription = values.get(array[i]);
            //System.out.println(topicDescription.get().name()+ " " + topicDescription.get().partitions().size());
            for (int j=0; j<(topicDescription.get().partitions().size()-1); j++) {
                Map<TopicPartition, Optional<NewPartitionReassignment>> reassignment = new HashMap<>();
                reassignment.put(new TopicPartition(array[i], j),
                Optional.of(new NewPartitionReassignment(Arrays.asList(1,2,3,5,6,7))));
                adminClient.alterPartitionReassignments(reassignment).all().get();
            }
        }
//
        //System.out.println(result.all().get());
    //    System.out.println(result.values().get("test.topic").get());
//
//        reassignment.put(new TopicPartition("test.topic", 3),
//                Optional.of(new NewPartitionReassignment(Arrays.asList(1,2,3))));
//        adminClient.alterPartitionReassignments(reassignment).all().get();

    }
}
