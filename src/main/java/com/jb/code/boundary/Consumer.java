package com.jb.code.boundary;

import com.microsoft.azure.sdk.iot.device.DeviceClient;
import com.microsoft.azure.sdk.iot.device.IotHubClientProtocol;
import com.microsoft.azure.sdk.iot.device.IotHubMessageResult;
import com.microsoft.azure.sdk.iot.device.Message;
import com.microsoft.azure.sdk.iot.device.MessageCallback;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import java.io.IOException;
import javax.enterprise.event.Observes;
import org.jboss.logging.Logger;

public class Consumer implements MessageCallback {

  private static final Logger LOG = Logger.getLogger(Consumer.class);
  private static final String CONNECTION_STRING = "<CONNECTION_STRING_FROM_DEVICE>";
  private static final IotHubClientProtocol PROTOCOL = IotHubClientProtocol.MQTT;
  private DeviceClient client;


  /**
   * Method will be trigger by Quarkus lifecycle when starting the application.
   * @see <a href="https://quarkus.io/guides/lifecycle">Quarkus Lifecycle</a>
   *
   * @param ev StartupEvent with context
   * @throws Exception
   */
  void onStart(@Observes StartupEvent ev) throws Exception {
    LOG.info("Service is starting...");
    this.client = new DeviceClient(CONNECTION_STRING, PROTOCOL);
    this.client.open();
    this.client.setMessageCallback(this, null);
    LOG.info("Starting to consume Events from IoT Hub");
  }

  /**
   * Method will be invoked when the application is shutting down according to Quarkus lifecycle.
   * @see <a href="https://quarkus.io/guides/lifecycle">Quarkus Lifecycle</a>
   *
   * @param ev ShutdownEvent with context
   * @throws IOException
   */
  void onStop(@Observes ShutdownEvent ev) throws IOException {
    this.client.closeNow();
    LOG.info("The application is stopping...");
  }





  /**
   * Method used to handle the event that was consume from IoT Hub.
   *
   * @param message represents an event in format of {@link String}
   * @param messageId represents an ID of type {@link String} in format of {@link java.util.UUID}
   */
  private void eventHandler(final String message, final String messageId) {
    LOG.infov("Message ID {0}\n  Message: {1}", messageId, message);
  }

  /**
   * Method provided by the implementation of {@link MessageCallback}.
   * @see <a href="https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.sdk.iot.device?view=azure-java-stable">Azure SDK</a>
   *
   * @param message the message.
   * @param callbackContext a custom context given by the developer.
   *
   * @return {@link IotHubMessageResult#COMPLETE} after processing the event
   */
  @Override
  public IotHubMessageResult execute(Message message, Object callbackContext) {
    String msg = new String(message.getBytes(), Message.DEFAULT_IOTHUB_MESSAGE_CHARSET);
    eventHandler(msg, message.getMessageId());

    return IotHubMessageResult.COMPLETE;
  }
}
