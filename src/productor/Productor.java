package productor;

//imports necesarios
import compartido.*;
import java.rmi.RemoteException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.util.Scanner;

public class Productor {
    public static void main(String[] args) {
        try {
            System.out.println("----------------------PRODUCTOR----------------------");
            Scanner scanner = new Scanner(System.in);
            System.out.println("Ingrese la direccion del broker:");
            String brokerhost = scanner.nextLine();
            int brokerport = 32005; // Puerto fijo para el broker asíncrono
            String brokerurl = "rmi://" + brokerhost + ":" + brokerport + "/BrokerService";
            
            // Conectar al broker
            Brokerasinc broker = (Brokerasinc) Naming.lookup(brokerurl);
            System.out.println("Conectado al broker: " + brokerurl); 
            boolean menu = true;
            while (menu) {
                System.out.println("------------MENU------------");
                System.out.println("1. Crear una cola");
                System.out.println("2. Eliminar una cola");
                System.out.println("3. Enviar mensaje a una cola");
                System.out.println("4. Mostrar estado del broker");
                System.out.println("5. Reconectar al broker");
                System.out.println("6. Salir");
                String accion = scanner.nextLine();
                String cola;
                String mensaje;
                switch (accion) {
                    case "1":
                        System.out.println("Ingrese el nombre de la cola a crear:");
                        cola = scanner.nextLine();
                        try {
                            broker.declararCola(cola);
                            System.out.println("Cola '" + cola + "' creada.");
                            break;
                        } catch (RemoteException e) {
                            System.out.println("Error al crear la cola: " + e.getMessage());
                            break;
                        }
                    case "2":
                        System.out.println("Ingrese el nombre de la cola a eliminar:");
                        cola = scanner.nextLine();
                        try {
                            broker.eliminarCola(cola);
                            System.out.println("Cola '" + cola + "' eliminada.");
                            break;
                        } catch (RemoteException e) {
                            System.out.println("Error al eliminar la cola: " + e.getMessage());
                            break;
                        }
                    case "3":
                        System.out.println("Ingrese el nombre de la cola a la que desea enviar el mensaje:");
                        cola = scanner.nextLine();
                        System.out.println("Ingrese el mensaje:");
                        mensaje = scanner.nextLine();
                        try {
                            broker.publicar(mensaje, cola);
                            System.out.println("Mensaje enviado a la cola '" + cola + "'.");
                            break;
                        } catch (RemoteException e) {
                            System.out.println("Error: No se pudo enviar el mensaje: " + e.getMessage());
                            break;
                        }
                    case "4":
                        try {
                            String estado = broker.mostrarEstado();
                            System.out.println(estado);
                            break;
                        } catch (RemoteException e) {
                            System.out.println("Error al obtener el estado del broker: " + e.getMessage());
                            break;
                        }
                    case "5":
                        // Reintentar la conexión al broker
                        try {
                            broker = (Brokerasinc) Naming.lookup(brokerurl);
                            System.out.println("Reconectado al broker: " + brokerurl);
                        } catch (Exception e) {
                            System.out.println("Error al reconectar al broker: " + e.getMessage());
                        }
                        break;
                    case "6":
                        menu = false;
                        break;
                    default:
                        System.out.println("Acción no reconocida.");
                        break;
                }
            }
            scanner.close();
        } catch (NotBoundException e) {
            System.out.println("Error: El broker no está disponible. " + e.getMessage());
        } catch (RemoteException e) {
            System.out.println("Error de comunicación con el broker: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Error inesperado: " + e.getMessage());
        }

    } 
} 