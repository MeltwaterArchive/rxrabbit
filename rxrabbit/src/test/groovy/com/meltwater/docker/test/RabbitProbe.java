package com.meltwater.docker.test;


import com.meltwater.rxrabbit.AdminChannel;
import com.meltwater.rxrabbit.BrokerAddresses;
import com.meltwater.rxrabbit.ConnectionSettings;
import com.meltwater.rxrabbit.impl.DefaultChannelFactory;

public class RabbitProbe {

    private final String hostname = "docker.local";
    private final BrokerAddresses addresses;
    private final String tcpPort;
    private Exception exception = new RuntimeException("dummy");

    public RabbitProbe(String tcpPort) {
        this.addresses = new BrokerAddresses("amqp://"+hostname+":"+tcpPort);
        this.tcpPort = tcpPort;
    }

    public Boolean isSatisfied() {
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

    public String getLatestError() {
        return "Failed to create connection with rabbit on "+ hostname +":"+tcpPort+" error: "+exception.getMessage();
    }

}
