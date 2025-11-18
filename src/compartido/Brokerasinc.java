package compartido;
import java.rmi.Remote;
import java.rmi.RemoteException;

public abstract interface Brokerasinc extends Remote {

    /**
    * Este metodo envia un ACK al broker se utiliza desde el consumidor
    * @return siempre true 
    */
    boolean signalACK() throws RemoteException;
    /**
    * Declara una nueva cola en el broker.
    * @param nombre_cola Nombre de la cola a declarar.
    */
    void declararCola(String nombre_cola) throws RemoteException;

    /**
    * Elimina una cola del broker.
    * @param nombre_cola Nombre de la cola a eliminar.
    */
    void eliminarCola(String nombreCola) throws RemoteException;

    /**
    * Desuscribe un consumidor de una cola.
    * @param nombreCola Nombre de la cola de la que se desea desuscribirse.
    * @param consumidor Consumidor a desuscribir.
    * @return Mensaje indicando el resultado de la desuscripción.
    */
    String desuscribirCola(String nombreCola, ConsumerCallback consumidor) throws RemoteException;

    /**
    * Publica un mensaje en el broker.
    * @param mensaje Mensaje a publicar.
    * @param nombre_cola Nombre de la cola donde se publicará el mensaje.
    */
    void publicar(String mensaje, String nombre_cola) throws RemoteException;

    /**
    * Consume un mensaje de la cola del broker.
    * @param nombre_cola Nombre de la cola de la que se desea consumir.
    * @param consumidor Consumidor que recibirá el mensaje.
    */
    void consumir(String nombre_cola, ConsumerCallback consumidor) throws RemoteException;

    /**
     * Muestra el estado actual del broker (colas y consumidores).
     * @return Representación en cadena del estado del broker.  
     */
    String mostrarEstado() throws RemoteException;
}