package com.meltwater.rxrabbit;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface ChannelFactory {

    ConsumeChannel createConsumeChannel(String queue) throws IOException, TimeoutException;

    PublishChannel createPublishChannel(String exchange) throws IOException, TimeoutException;

    AdminChannel createAdminChannel() throws IOException, TimeoutException;

}
