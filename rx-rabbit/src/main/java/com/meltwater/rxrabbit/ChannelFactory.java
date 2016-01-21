package com.meltwater.rxrabbit;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * A factory for creating channels
 */
public interface ChannelFactory {

    /**
     * Creates a consume channel connected to an already existing queue.
     *
     * @param queue the queue the consume channel is connected to
     *
     * @return a consume channel connected to an already existing queue.
     *
     */
    ConsumeChannel createConsumeChannel(String queue) throws IOException;

    /**
     * Creates a consume channel connected to a server-named exclusive, autodelete, non-durable queue bound to the
     * given exchange and with the supplied routing key.
     *
     * NOTE a side effect of this consume channel creation is that this temporary queue will be created.
     * The queue will then be deleted as soon as the channels connection to rabbit is closed for any reason.
     *
     * @param exchange the exchange this consume channel is connected to
     * @param routingKey the routingKey between the exchange and the temporary queue
     *
     * @return a consume channel connected to an ad hoc created temporary queue.
     *
     */
    ConsumeChannel createConsumeChannel(String exchange, String routingKey) throws IOException;

    /**
     *
     * @return a publish channel that can be used to publish messages to existing exchanges
     *
     */
    PublishChannel createPublishChannel() throws IOException;

    /**
     *
     * @return an admin channel that can be used to declare and remove queues, exchanges and bindings
     */
    AdminChannel createAdminChannel() throws IOException;

}
