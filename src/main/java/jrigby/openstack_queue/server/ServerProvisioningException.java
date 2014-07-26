package jrigby.openstack_queue.server;

/**
 * Created by jrigby on 26/07/2014.
 */
public class ServerProvisioningException extends Exception {
    public ServerProvisioningException() {
    }

    public ServerProvisioningException(String message) {
        super(message);
    }

    public ServerProvisioningException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServerProvisioningException(Throwable cause) {
        super(cause);
    }

    public ServerProvisioningException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
