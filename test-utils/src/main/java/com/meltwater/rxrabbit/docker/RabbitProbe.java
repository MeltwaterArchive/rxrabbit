package com.meltwater.rxrabbit.docker;


import com.meltwater.rxrabbit.AdminChannel;
import com.meltwater.rxrabbit.BrokerAddresses;
import com.meltwater.rxrabbit.ConnectionSettings;
import com.meltwater.rxrabbit.impl.DefaultChannelFactory;

class RabbitProbe {

    private final String hostname = "localhost";
    private final BrokerAddresses addresses;
    private final String tcpPort;
    private Exception exception = new RuntimeException("dummy");

    RabbitProbe(String tcpPort) {
        this.addresses = new BrokerAddresses("amqp://"+hostname+":"+tcpPort);
        this.tcpPort = tcpPort;
    }

    Boolean isSatisfied() {
        AdminChannel adminChannel  = null;
        try {
            adminChannel = new DefaultChannelFactory(addresses, new ConnectionSettings()).createAdminChannel();
            if (!adminChannel.isOpen()) {
                throw new RuntimeException("Rabbit connection still not open");
            }
        } catch (Exception ex) {
            exception = ex;
            return false;
        }finally{
            if (adminChannel != null) {
                adminChannel.closeWithError();
            }
        }
        return true;
    }

    String getLatestError() {
        return "Failed to create connection with rabbit on "+ hostname +":"+tcpPort+" error: "+exception.getMessage();
    }

}
