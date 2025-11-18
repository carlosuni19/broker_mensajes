package compartido;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.ArrayList;
import java.io.Serializable;

public class Cola implements Serializable{
    private final Queue<String> mensajes;
    private final List<ConsumerCallback> consumidores;
    private final String nombre_cola;
    private ConsumerCallback nextConsumidor;
    private static final long serialVersionUID = 1L;

    public Cola(String nombre_cola) {
        this.mensajes = new ConcurrentLinkedQueue<>();
        this.consumidores = new ArrayList<>();
        this.nombre_cola = nombre_cola;
        this.nextConsumidor = null;
    }

    public void registrarConsumidor(ConsumerCallback consumidor) {
        if (!consumidores.contains(consumidor)) {
            consumidores.add(consumidor);
        }
    }

    public int numMensajes() {
        return mensajes.size();
    }

    public String getNombre(){
        return nombre_cola;
    }

    public List<ConsumerCallback> getConsumidores() {
        return consumidores;
    }

    public ConsumerCallback getnextconsumer(){
        return nextConsumidor;
    }

    public void addMensaje(String mensaje) {
        mensajes.add(mensaje);
    }

    public void eliminarMensaje() {
        mensajes.poll();
    }

    public boolean colaVacia() {
        return mensajes.isEmpty();
    }

    public void removeConsumer(ConsumerCallback consumidor) {
        consumidores.remove(consumidor);
    }

    public String consumirMensaje() {
        if (mensajes.isEmpty()) {
            return null; // No hay mensajes
        }
        // Implementación de round-robin para seleccionar el siguiente consumidor
        if (nextConsumidor == null || !consumidores.contains(nextConsumidor)) {
            nextConsumidor = consumidores.get(0);
        } else {
            int currentIndex = consumidores.indexOf(nextConsumidor);
            nextConsumidor = consumidores.get((currentIndex + 1) % consumidores.size());
        }
        
        if (nextConsumidor != null) {
            return mensajes.poll();
        }
        return null;
    }
}