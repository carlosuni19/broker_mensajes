package consumidor;
import compartido.ConsumerCallback;
import compartido.*;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.NotBoundException;
import java.util.Scanner;

    
public class ConsumidorImpl extends UnicastRemoteObject implements ConsumerCallback {
    // Constructor por defecto
    protected ConsumidorImpl() throws RemoteException { super(); }

    @Override
    public boolean onMessage(String mensaje,Brokerasinc broker) throws RemoteException {
        try {
            System.out.println("Mensaje recibido: " + mensaje);
            return broker.signalACK();// Indica que el mensaje fue procesado correctamente
        } catch (Exception e) {
            return false;
        }
    }


    
    public static void main(String[] args) {
        try {
            System.out.println("----------------------CONSUMIDOR----------------------");
            Scanner scanner = new Scanner(System.in);
            System.out.println("Ingrese la direccion del broker:");
            String brokerhost = scanner.nextLine();
            int brokerport = 32005; // Puerto fijo para el broker asíncrono
            String brokerurl = "//" + brokerhost + ":" + brokerport + "/BrokerService";
            
            // Conectar al broker
            Brokerasinc broker = (Brokerasinc) Naming.lookup(brokerurl);
            ConsumerCallback consumer = new ConsumidorImpl();
            System.out.println("Conectado al broker en " + brokerurl); 
            while (true) {
                System.out.println("------------MENU------------");
                System.out.println("1. Suscribirse a una cola");
                System.out.println("2. Desuscribirse de una cola");
                System.out.println("3. Estado del broker");
                System.out.println("4. Reconectar al broker");
                System.out.println("5. salir");
                String accion = scanner.nextLine();
                String cola;
                switch (accion) {
                    case "1":
                        System.out.println("Nombre de la cola:");
                        cola = scanner.nextLine();
                        broker.consumir(cola, consumer);
                        break;
                    case "2":
                        System.out.println("Nombre de la cola:");
                        cola = scanner.nextLine();
                        String respuesta = broker.desuscribirCola(cola, consumer);
                        System.out.println(respuesta);
                        break;
                    case "3":
                        try {
                            String estado = broker.mostrarEstado();
                            System.out.println(estado);
                            break;
                        } catch (RemoteException e) {
                            System.out.println("Error al obtener el estado del broker: " + e.getMessage());
                            break;
                        }
                    case "4":
                        // Reintentar la conexión al broker
                        try {
                            broker = (Brokerasinc) Naming.lookup(brokerurl);
                            System.out.println("Reconectado al broker: " + brokerurl);
                        } catch (Exception e) {
                            System.out.println("Error al reconectar al broker: " + e.getMessage());
                        }
                        break;
                    case "5":
                        System.exit(0);
                        break;
                    default:
                        break;
                }
            }
        }
        catch (NotBoundException e) {
        System.out.println("El servicio del broker no esta disponible: " + e.getMessage());
        }
        catch (RemoteException e) {
            System.out.println("Error de comunicacion remota: " + e.getMessage());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
