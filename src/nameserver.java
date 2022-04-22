import java.io.*;
import java.net.*;
import java.util.*;

public class nameserver {
    private ServerSocket server = null;
    SortedMap<Integer, String> lookup = new TreeMap<>();
    Server post = null;
    Server pre = null;
    Server bootstrap;
    Server me;
    int id;

    public static void main(String[] args) {
        try {
            nameserver serverSocket = new nameserver();
            serverSocket.connection(args[0]);
        } catch (IOException ignored) {
        }
    }

    // connection is a function that establishes a connection with the client
    public void connection(String file) throws IOException {
        try {
            File configFile = new File(file);
            Scanner fileScanner = new Scanner(configFile);

            id = fileScanner.nextInt();
            int port = fileScanner.nextInt();
            InetAddress bIp = InetAddress.getByName(fileScanner.next());
            int bPort = fileScanner.nextInt();
            bootstrap = new Server(0, bIp, bPort);
            server = new ServerSocket(port);

            System.out.println("Name Server with id " + id +" is Running... ");

            new Thread(() -> {
                while (true){
                    Scanner scanner = new Scanner(System.in);
                    String line = scanner.nextLine().trim();
                    String[] commands = line.split(" ");
                    String command = commands[0];
                    if (command.equals("enter")) {
                        new Thread(() -> {
                            try (Socket socket = new Socket(bIp, bPort)) {
                                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                                me = new Server(id, socket.getLocalAddress() ,port);
                                outputStream.writeUTF("enter " + id + " " + port);
                                DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                                System.out.println(inputStream.readUTF());
                                System.out.println(inputStream.readUTF());
                            } catch (IOException ignored) {
                            }
                        }).start();
                    } else if (command.equals("exit")) {
                        new Thread(() -> {
                            try (Socket socket = new Socket(bIp, bPort)) {
                                if(me == null){
                                    System.out.println("Failed exit, Cannot exit before enter");
                                }
                                else{
                                    bootstrap.exit(me, "");
                                    DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                                    System.out.println(inputStream.readUTF());
                                }
                            } catch (IOException ignored) {
                            }
                        }).start();
                    }
                }
            }).start();

            while (true) {
                Socket socket = server.accept();
                Parser thread = new Parser(socket,this);
                thread.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
            server.close();
        }
    }
}

class Parser extends Thread {
    Socket socket = null;
    DataOutputStream outputStream = null;
    DataInputStream inputStream = null;
    nameserver server;

    //Constructor
    public Parser(Socket socket, nameserver server) {
        try {
            this.socket = socket;
            outputStream = new DataOutputStream(socket.getOutputStream());
            inputStream = new DataInputStream(socket.getInputStream());
            this.server = server;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            String line = inputStream.readUTF();
            String[] commands = line.split(" ");
            String command = commands[0];

            if (command.equals("lookup")) {
                int key = Integer.parseInt(commands[1]);
                String sequence = inputStream.readUTF();
                Server next = server.post;
                int id = server.id;
                if(next != null && next.id <= key){
                    next.lookup(key, sequence + id + " ");
                }else{
                    String value = server.lookup.get(key);
                    if(value == null){
                        server.bootstrap.message("Key not found in ID " + id + "\n" + "Server lookup sequence " + sequence);
                    }else{
                        server.bootstrap.message("Value is "+ value +"\nValue found in ID " + id + "\n" + "Server lookup sequence " + sequence);
                    }
                }

            } else if (command.equals("insert")) {
                int key = Integer.parseInt(commands[1]);
                Server next = server.post;
                int id = server.id;
                String value = commands[2];
                String sequence = inputStream.readUTF();
                if(next != null && next.id <= key){
                    next.insert(key,value, sequence + id + " ");
                }else{
                    server.lookup.put(key, value);
                    server.bootstrap.message("Key was inserted in ID " + id + "\n" + "Server lookup sequence " + sequence);
                }
            } else if (command.equals("delete")) {
                int key = Integer.parseInt(commands[1]);
                String sequence = inputStream.readUTF();
                Server next = server.post;
                int id = server.id;
                if(next != null && next.id <= key){
                    next.delete(key, sequence + id + " ");
                }else{
                    String value = server.lookup.remove(key);
                    if(value == null){
                        server.bootstrap.message("Key not found in ID " + id + "\nServer lookup sequence " + sequence);
                    }else{
                        server.bootstrap.message("Successful deletion in ID " + id + "\nServer lookup sequence " + sequence);
                    }
                }
            } else if(command.equals("enter")){
                int sId = Integer.parseInt(commands[1]);
                String ip = commands[2];
                int port = Integer.parseInt(commands[3]);
                String sequence = inputStream.readUTF();
                int id = server.id;
                Server ns = new Server(sId, InetAddress.getByName(ip), port);
                Server next = server.post;
                if(id == ns.id){
                    System.out.println("Failed Entry\nCannot enter twice\n" + "Server lookup sequence "+ sequence + id + " ");
                }
                else if(next == null || next.compareTo(ns) > 0){
                    ns.message("Successful entry\n" + "Server lookup sequence "+ sequence + id + " ");
                    if(next == null){
                        ns.message("Handling range of "+ sId + "-1023" + "\nPredecessor ID is " + id + " and Successor ID is 0(bootstrap)");
                        ns.setPre(server.me);
                        Map<Integer, String> subMap = server.lookup.tailMap(ns.id);
                        ns.insertAll(subMap);
                    }
                    else{
                        ns.message("Handling range of "+ sId + "-" + (next.id - 1) + "\nPredecessor ID is "+ id +" and Successor ID is "+ next.id);
                        next.setPre(ns);
                        ns.setPost(next);
                        ns.setPre(server.me);
                        Map<Integer, String> subMap = server.lookup.subMap(ns.id, next.id);
                        ns.insertAll(subMap);
                        subMap.clear();
                    }
                    server.post = ns;
                }
                else{
                    next.enter(ns,sequence + id + " ");
                }
            }else if(command.equals("exit")){
                int nId = Integer.parseInt(commands[1]);
                String ip = commands[2];
                int port = Integer.parseInt(commands[3]);
                String sequence = inputStream.readUTF();
                int id = server.id;

                Server ns = new Server(nId, InetAddress.getByName(ip), port);
                Server next = server.post;
                if(next == null || next.id > ns.id){
                    ns.close(false,sequence + id + " ");
                }
                else if(next.id == ns.id){
                    ns.close(true, sequence + id + " ");
                }
                else{
                    next.exit(ns, sequence + id + " ");
                }
            } else if (command.equals("close")) {
                boolean success = Boolean.parseBoolean(commands[1]);
                String sequence = inputStream.readUTF();
                if(success){
                    Server pre = server.pre;
                    Server post = server.post;
                    int postRange = 1023;
                    if(post != null){
                        post.setPre(pre);
                        postRange = post.id-1;
                    }
                    if(pre == null){
                        pre = server.bootstrap;
                    }
                    pre.insertAll(server.lookup);
                    pre.setPost(post);

                    server.post = null;
                    server.pre = null;
                    server.me = null;
                    System.out.println("Successful exit\n" + "Range of " + server.id + "-" + postRange + " is handed over to Predecessor ID "+ pre.id + "\nServer lookup sequence"+ sequence);
                }else{
                    System.out.println("Failed exit\n" + "Server lookup sequence"+ sequence);
                }
            }else if (command.equals("insertAll")) {
                int id = server.id;
                while(inputStream.available() > 0){
                    int key = inputStream.readInt();
                    String value = inputStream.readUTF();
                    if(key >=  id){
                        server.lookup.put(key, value);
                    }
                }
            } else if (command.equals("pre")) {
                int id = server.id;
                int nId = Integer.parseInt(commands[1]);
                String ip = commands[2];
                int port = Integer.parseInt(commands[3]);
                if(nId == 0){
                    server.pre = null;
                }
                else if(id > nId){
                    server.pre = new Server(nId, InetAddress.getByName(ip), port);
                }
            } else if (command.equals("post")) {
                int id = server.id;
                int nId = Integer.parseInt(commands[1]);
                String ip = commands[2];
                int port = Integer.parseInt(commands[3]);
                if(nId == 0){
                    server.post = null;
                }
                else if(id < nId){
                    server.post = new Server(nId, InetAddress.getByName(ip), port);
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