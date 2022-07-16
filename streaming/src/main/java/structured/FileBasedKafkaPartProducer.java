package structured;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class FileBasedKafkaPartProducer {
	public static void main(String[] args) throws InterruptedException, IOException {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "structured.CustomJavaPartitioner");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		File folder = new File(args[1]);
		File[] loF = folder.listFiles();
		for (File file : loF) {
			System.out.println(file.getName());
			BufferedReader reader = new BufferedReader(new FileReader(file));
			reader.readLine();
			String line;
			while ((line = reader.readLine()) != null) {
				 System.out.println(line.substring(0, line.indexOf(",")) + " , " + line);
				String rkey = line.substring(0, line.indexOf(","));

				ProducerRecord<String, String> record = new ProducerRecord<String, String>("nsecmdpart", rkey, line);
				producer.send(record);
				Thread.sleep(Integer.parseInt(args[2]));
			}
		}
	}
}
