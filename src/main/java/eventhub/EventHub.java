package eventhub;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.EventHubRuntimeInformation;
import com.microsoft.azure.eventhubs.EventPosition;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.ReceiverOptions;
import com.microsoft.azure.eventhubs.impl.EventPositionImpl;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;
import com.microsoft.azure.eventprocessorhost.ExceptionReceivedEventArgs;

public class EventHub {

	public static void main(String[] args)
			throws EventHubException, IOException, InterruptedException, ExecutionException {
		// TODO Auto-generated method stub

		final ConnectionStringBuilder connStr = new ConnectionStringBuilder().setNamespaceName("eventhub")
				.setEventHubName("import").setSasKeyName("RDP").setSasKey("test");

		final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);
		final Gson gson = new GsonBuilder().create();

		// Each EventHubClient instance spins up a new TCP/SSL connection, which is
		// expensive.
		// It is always a best practice to reuse these instances. The following sample
		// shows this.
		final EventHubClient ehClient = EventHubClient.createSync(connStr.toString(), executorService);

		try {
			for (int i = 0; i < 4; i++) {

				String payload = "Sending Message to Eventhub" + Integer.toString(i);
				byte[] payloadBytes = gson.toJson(payload).getBytes(Charset.defaultCharset());
				EventData sendEvent = EventData.create(payloadBytes);
				// Send - not tied to any partition
				// Event Hubs service will round-robin the events across all Event Hubs
				// partitions.
				// This is the recommended & most reliable way to send to Event Hubs.
				// ehClient.sendSync(sendEvent);
			}

			EventHubRuntimeInformation runTimeInfo = ehClient.getRuntimeInformation().get();
			int numPartitions = runTimeInfo.getPartitionCount();
			for (int partition = 0; partition < numPartitions; partition++) {
				PartitionReceiver receiver = ehClient.createReceiverSync(EventHubClient.DEFAULT_CONSUMER_GROUP_NAME,
						String.valueOf(partition), EventPosition.fromStartOfStream());
				receiver.receive(2).handle((records, throwable) -> handleComplete(receiver, records, throwable));
			}
			// receiveEvents(ehClient);
			System.out.println(ehClient.getRuntimeInformation() + ": Send Complete...");
			System.out.println("Press Enter to stop.");
			System.in.read();
		} finally {
			ehClient.closeSync();
			executorService.shutdown();
		}

	}

	private static Object handleComplete(PartitionReceiver receiver, Iterable<EventData> records, Throwable throwable) {
		for (EventData record : records) {
			System.out.println(
					String.format("Partition %s, Event %s", receiver.getPartitionId(), new String(record.getBytes())));
		}

		receiver.receive(10).handle((r, t) -> handleComplete(receiver, r, t));
		return null;
	}

	private static void receiveEvents(EventHubClient ehClient)
			throws InterruptedException, ExecutionException, EventHubException {
		String consumerGroupName = "$Default";
		String namespaceName = "eventhub";
		String eventHubName = "import";
		String sasKeyName = "test";
		String sasKey = "test.....";
		String storageConnectionString = null;
		String storageContainerName = "----StorageContainerName----";
		String hostNamePrefix = "----HostNamePrefix----";

		ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder().setNamespaceName(namespaceName)
				.setEventHubName(eventHubName).setSasKeyName(sasKeyName).setSasKey(sasKey);

		EventProcessorHost host = new EventProcessorHost(EventProcessorHost.createHostName(hostNamePrefix),
				eventHubName, consumerGroupName, eventHubConnectionString.toString(), storageConnectionString,
				storageContainerName);

		System.out.println("Registering host named " + host.getHostName());
		EventProcessorOptions options = new EventProcessorOptions();
		options.setExceptionNotification(new ErrorNotificationHandler());

		host.registerEventProcessor(EventProcessor.class, options).whenComplete((unused, e) -> {
			if (e != null) {
				System.out.println("Failure while registering: " + e.toString());
				if (e.getCause() != null) {
					System.out.println("Inner exception: " + e.getCause().toString());
				}
			}
		}).thenAccept((unused) -> {
			System.out.println("Press enter to stop.");
			try {
				System.in.read();
			} catch (Exception e) {
				System.out.println("Keyboard read failed: " + e.toString());
			}
		}).thenCompose((unused) -> {
			return host.unregisterEventProcessor();
		}).exceptionally((e) -> {
			System.out.println("Failure while unregistering: " + e.toString());
			if (e.getCause() != null) {
				System.out.println("Inner exception: " + e.getCause().toString());
			}
			return null;
		}).get(); // Wait for everything to finish before exiting main!

		System.out.println("End of sample");
	}

}
