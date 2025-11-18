package broker;

//imports necesarios
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import compartido.*;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class BrokerasincImpl extends UnicastRemoteObject implements Brokerasinc {
    private  final Map<String, Cola> colas;
    private static final String SAVE_FILE = "broker_data.dat";
    private static final String DUR_PREFIX = "DUR_"; // Prefijo para identificar colas duraderas
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean signalACK() throws RemoteException {
        return true; 
    }


    /**
     * Constructor por defecto.
     */
    public BrokerasincImpl() throws RemoteException {
        super();
        this.colas = cargarEstado();
    }

    /** 
     * Carga el estado del broker desde un archivo serializado.
     * @return Un mapa con las colas del broker(solo las duraderas).
     */
    @SuppressWarnings("unchecked")// Sin esta anotación, el compilador mostraría una advertencia aquí (necesaria para la deserialización)
    private Map<String, Cola> cargarEstado() {
        try {
            File file = new File(SAVE_FILE);
            if (file.exists()) {
                try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
                    return (Map<String, Cola>) ois.readObject();
                }
            }
            Map<String, Cola> estado = new ConcurrentHashMap<>();
            return estado;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return new ConcurrentHashMap<>();
        }
    }

    /**
     * Guarda el estado del broker en un archivo serializado (Solo las colas duraderas).
     */
    private void guardarEstado() {
        // Filtrar las colas duraderas
        Map<String, Cola> colasDuraderas = new ConcurrentHashMap<>();
        for (Map.Entry<String, Cola> entry : colas.entrySet()) {
            if (entry.getKey().startsWith(DUR_PREFIX)) {
                colasDuraderas.put(entry.getKey(), entry.getValue());
            }
        }
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(SAVE_FILE))) {
            oos.writeObject(colasDuraderas);
            System.out.println("Estado del broker guardado.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declararCola(String nombreCola) throws RemoteException {
        Cola prev = colas.putIfAbsent(nombreCola, new Cola(nombreCola));//crea la cola si no existe
        if (prev == null) {
            System.out.println("Cola '" + nombreCola + "' creada.");
            // Si la cola es duradera, guardar el estado
            if (nombreCola.startsWith(DUR_PREFIX)) {
                guardarEstado();
            }
        } else {
            System.out.println("Cola '" + nombreCola + "' ya existia.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void eliminarCola(String nombreCola) throws RemoteException {
        Cola removed = colas.remove(nombreCola);
        if (removed != null) {
            System.out.println("Cola '" + nombreCola + "' eliminada.");
            // Si la cola es duradera, guardar el estado
            if (nombreCola.startsWith(DUR_PREFIX)) {
                guardarEstado();
            }
        } else {
            System.out.println("La cola '" + nombreCola + "' no existe.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String desuscribirCola(String nombreCola, ConsumerCallback consumidor) throws RemoteException {
        String respuesta;
        //comprobar que la cola existe
        if (colas.containsKey(nombreCola)){
            Cola queue = colas.get(nombreCola);
            //comprobar que el consumidor está registrado en la cola
            if (queue.getConsumidores().contains(consumidor)){
                queue.removeConsumer(consumidor);
                respuesta = "Desuscripcion de la cola '" + nombreCola + "' realizada con exito.";
                System.out.println("Consumidor desuscrito de la cola '" + nombreCola + "'.");
                // Si la cola es duradera, guardar el estado
                if (nombreCola.startsWith(DUR_PREFIX)) {
                    guardarEstado();
                }
            }
            else {
                respuesta = "El consumidor no esta suscrito a la cola '" + nombreCola + "'.";
            }
        }
        else {
            respuesta = "La cola '" + nombreCola + "' no existe.";
        }
        return respuesta;
    }

        /**
     * {@inheritDoc}
     */
    @Override
    public void publicar(String mensaje, String cola) throws RemoteException {
        // Implementación del método publicar
        Cola queue = colas.get(cola);
        String mensajeTemp;
            if (queue != null) {
                queue.addMensaje(mensaje);
                System.out.println("Mensaje publicado en la cola '" + cola + "': " + mensaje);
                // Si la cola es duradera, guardar el estado
                if (cola.startsWith(DUR_PREFIX)) {
                    guardarEstado();
                }
                // Si hay consumidores registrados, vacia la cola.
                if (!queue.getConsumidores().isEmpty()) {
                    while (!queue.colaVacia()) {
                        mensajeTemp = queue.consumirMensaje();
                        ConsumerCallback consumer = queue.getnextconsumer();
                        int trys = 0;
                        try{
                            boolean ack = consumer.onMessage(mensajeTemp, this);
                            if (ack){
                                System.out.println("ACK recibido");
                            }
                            else{
                                if (trys < 3) {
                                    queue.addMensaje(mensajeTemp);
                                    System.out.println("Mensaje reenviado a la cola debido a falta de ACK");
                                    trys++;
                                } else {
                                    System.out.println("No se recibió ACK después de 3 intentos. Consumidor eliminado de la cola.");
                                    queue.removeConsumer(consumer);
                                    break;
                                }
                            }
                        } catch (RemoteException e) {
                            queue.addMensaje(mensajeTemp);
                            System.out.println("Error al enviar el mensaje al consumidor. Mensaje reenviado a la cola.");
                        }
                        // Si la cola es duradera, guardar el estado
                    if (cola.startsWith(DUR_PREFIX)) {
                        guardarEstado();
                    }                   
                    }
                }
                else {
                    scheduler.schedule(() -> {
                        while (!queue.colaVacia()) {
                            if (queue.getConsumidores().isEmpty()) {
                                queue.eliminarMensaje();
                                System.out.println("No hay consumidores registrados en la cola '" + cola + "'. Mensaje eliminado por limite de tiempo.");
                                break; // Salir si no hay consumidores registrados
                            }
                            // Si la cola es duradera, guardar el estado
                            if (cola.startsWith(DUR_PREFIX)) {
                                guardarEstado();
                            }                   
                        }
                    }, 5, TimeUnit.MINUTES);
                }
            } else {
                System.out.println("La cola '" + cola + "' no existe. No se pudo publicar el mensaje.");
            }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void consumir(String cola, ConsumerCallback consumidor) {
        // si la cola existe, consume un mensaje de la cola sino la crea
        if (!colas.containsKey(cola)) {
            System.out.println("La cola '" + cola + "' no existe.");
            return;
        } 
        Cola queue = colas.get(cola);
        queue.registrarConsumidor(consumidor);
        String mensaje;
        while (!queue.colaVacia()) {
            mensaje = queue.consumirMensaje();
            if (mensaje!=null) {
                try {
                    boolean ack = consumidor.onMessage(mensaje, this);
                        if (ack){
                            System.out.println("ACK recibido");
                        }
                        else{
                            queue.addMensaje(mensaje);
                            System.out.println("Mensaje reenviado a la cola debido a falta de ACK");
                        }
                } 
                catch (RemoteException e) {
                }
            }
            // Si la cola es duradera, guardar el estado
            if (cola.startsWith(DUR_PREFIX)) {
                guardarEstado();
            }
            }
        }

        /**
     * {@inheritDoc}
     */
    @Override
    public String mostrarEstado() throws RemoteException {
        StringBuilder estado = new StringBuilder();
        estado.append("Estado actual del broker:\n");
        for (Map.Entry<String, Cola> entry : colas.entrySet()) {
            Cola cola = entry.getValue();
            estado.append("Cola: ").append(cola.getNombre()).append("\n");
            estado.append("  Mensajes en cola: ").append(cola.numMensajes()).append("\n");
            estado.append("  Consumidores suscritos: ").append(cola.getConsumidores().size()).append("\n");
        }
        return estado.toString();
    }

        public static void main(String[] args) {
        try {
            //crear e iniciar el registro RMI
            BrokerasincImpl broker = new BrokerasincImpl();
            Registry reg = LocateRegistry.createRegistry(32005);
            reg.rebind("BrokerService", broker);
            System.out.println("Broker listo y esperando conexiones...");
        } catch (Exception e) {
            e.printStackTrace();
        }
        }
}