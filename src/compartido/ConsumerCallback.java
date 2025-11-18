package compartido;
import java.rmi.Remote;
import java.rmi.RemoteException;
/**
 * Interfaz remota para el callback del consumidor.
 * Permite al broker notificar al consumidor cuando hay un nuevo mensaje disponible.
 */
public interface ConsumerCallback extends Remote {
    boolean onMessage(String mensaje,Brokerasinc broker) throws RemoteException;
}