package com.meltwater.rxrabbit.util;


import java.net.InetAddress;
import java.net.UnknownHostException;

//TODO this class should not be open sourced??
public class DockerAwareHostnameProvider {
    static Logger logger = new Logger(DockerAwareHostnameProvider.class);

    public static String getApplicationHostName() {
        String hostName;
        // Decide if this is a docker container by checking an otherwise unused environment variable
        if (System.getenv("HOST") != null) {
            hostName = System.getenv("HOST");
        }
        else {
            try {
                hostName = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                logger.warnWithParams("Unable to determine hostname", e);
                hostName = "unknownHost";
            }
        }
        return hostName;
    }
}
