import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class bnserver {
    private ServerSocket server = null;
    SortedMap<Integer, String> lookup = new TreeMap<>();
    Server nameServer = null;

    public static void main(String[] args) {
        try {
            bnserver serverSocket = new bnserver();
            serverSocket.connection(args[0]);
        } catch (IOException ignored) {
        }
    }

    // connection is a function that establishes a connection with the client
    public void connection(String file) throws IOException {
        try {
            File configFile = new File(file);
            Scanner fileScanner = new Scanner(configFile);

            int id = fileScanner.nextInt();
            int port = fileScanner.nextInt();
            while(fileScanner.hasNext()){
                int key = fileScanner.nextInt();
                String value = fileScanner.next();
                lookup.put(key, value);
            }
            server = new ServerSocket(port);

            System.out.println("Bootstrap server is Running... ");


            new Thread(() -> {
                while (true){
                    Scanner scanner = new Scanner(System.in);
                    String line = scanner.nextLine().trim();
                    String[] commands = line.split(" ");
                    String command = commands[0];
                    if (command.equals("lookup")) {
                        int key = Integer.parseInt(commands[1]);
                        if(key < 0 || key > 1023){
                            System.out.println("Key is out of range");
                        }
                        else if(nameServer != null && nameServer.id <= key){
                            nameServer.lookup(key, " 0 ");
                        }else{
                            String value = lookup.get(key);
                            if(value == null){
                                System.out.println("Key not found in bootstrap");
                            }else{
                                System.out.println("Value is "+ value+"\nValue found in ID 0 (bootstrap)");
                            }
                            System.out.println("Server lookup sequence"+ " 0 ");
                        }
                    } else if (command.equals("insert")) {
                        int key = Integer.parseInt(commands[1]);
                        String value = commands[2];
                        if(key < 0 || key > 1023){
                            System.out.println("Key is out of range");
                        }
                        else if(nameServer != null && nameServer.id <= key){
                            nameServer.insert(key,value, " 0 ");
                        }
                        else{
                            lookup.put(key, value);
                            System.out.println("Key was inserted in ID 0 (bootstrap)");
                            System.out.println("Server lookup sequence"+ " 0 ");
                        }
                    } else if (command.equals("delete")) {
                        int key = Integer.parseInt(commands[1]);
                        if(key < 0 || key > 1023){
                            System.out.println("Key is out of range");
                        }
                        else if(nameServer != null && nameServer.id <= key){
                            nameServer.delete(key, " 0 ");
                        }else{
                            String value = lookup.remove(key);
                            if(value == null){
                                System.out.println("Key not found in bootstrap");
                            }else{
                                System.out.println("Successful deletion from bootstrap");
                            }
                            System.out.println("Server lookup sequence"+ " 0 ");
                        }

                    }
                }
            }).start();

            while (true) {
                Socket socket = server.accept();
                CommandParser thread = new CommandParser(socket,this);
                thread.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
            server.close();
        }
    }


}

class CommandParser extends Thread {
    Socket socket = null;
    DataOutputStream outputStream = null;
    DataInputStream inputStream = null;
    bnserver bootstrap;

    //Constructor
    public CommandParser(Socket socket, bnserver bootstrap) {
        try {
            this.socket = socket;
            outputStream = new DataOutputStream(socket.getOutputStream());
            inputStream = new DataInputStream(socket.getInputStream());
            this.bootstrap = bootstrap;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            String line = inputStream.readUTF();
            String[] commands = line.split(" ");
            String command = commands[0];

            if(command.equals("enter")){
                int sId = Integer.parseInt(commands[1]);
                int port = Integer.parseInt(commands[2]);
                Server ns = new Server(sId, socket.getInetAddress(), port);
                Server next = bootstrap.nameServer;
                if(next == null || next.id > ns.id){
                    outputStream.writeUTF("Successful entry\nServer lookup sequence"+ " 0 ");
                    if(next == null){
                        outputStream.writeUTF("Handling range of "+ sId + "-1023" + "\nPredecessor ID is 0(bootstrap) and Successor ID is 0(bootstrap)");
                        Map<Integer, String> subMap = bootstrap.lookup.tailMap(ns.id);
                        ns.insertAll(subMap);
                    }
                    else{
                        outputStream.writeUTF("Handling range of "+ sId + "-" + (next.id - 1) + "\nPredecessor ID is 0(bootstrap) and Successor ID is "+next.id);
                        next.setPre(ns);
                        ns.setPost(next);
                        Map<Integer, String> subMap = bootstrap.lookup.subMap(ns.id, next.id);
                        ns.insertAll(subMap);
                    }
                    bootstrap.nameServer = ns;
                }
                else{
                    next.enter(ns,"0 ");
                }
            } else if(command.equals("insertAll")){
                int id = 0;
                while(inputStream.available() > 0){
                    int key = inputStream.readInt();
                    String value = inputStream.readUTF();
                    if(key >=  id){
                        bootstrap.lookup.put(key, value);
                    }
                }
            } else if(command.equals("exit")){
                int id = Integer.parseInt(commands[1]);
                String ip = commands[2];
                int port = Integer.parseInt(commands[3]);

                Server ns = new Server(id, InetAddress.getByName(ip), port);
                Server next = bootstrap.nameServer;
                if(next == null || next.id > ns.id){
                    ns.close(false," 0 ");
                }
                else if(next.id == ns.id){
                    ns.close(true, " 0 ");
                }
                else{
                    next.exit(ns, " 0 ");
                }
            } else if (command.equals("post")) {
                int id = 0;
                int nId = Integer.parseInt(commands[1]);
                String ip = commands[2];
                int port = Integer.parseInt(commands[3]);
                if(nId == 0){
                    bootstrap.nameServer = null;
                }
                else if(id < nId){
                    bootstrap.nameServer = new Server(nId, InetAddress.getByName(ip), port);
                }
            } else if(command.equals("message")){
                System.out.println(line.substring(line.indexOf(" ")+1));
            } else{
                System.out.println("Command doesn't exist, please try again");
            }
        } catch (Exception e){
        }
    }
}
