import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Map;

class Server implements Comparable<Server>{
    InetAddress ip;
    int port;
    int id;

    Server(int id, InetAddress ip, int port){
        this.ip = ip;
        this.port = port;
        this.id = id;
    }


    @Override
    public int compareTo(Server o) {
        return Integer.compare(id, o.id);
    }

    public void lookup(int key, String sequence){
        new Thread(() -> {
            try (Socket socket = new Socket(ip, port)) {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.writeUTF("lookup "+ key);
                outputStream.writeUTF(sequence);
            } catch (IOException ignored) {
            }
        }).start();
    }

    public void insert(int key, String value, String sequence){
        new Thread(() -> {
            try (Socket socket = new Socket(ip, port)) {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.writeUTF("insert "+ key + " " + value);
                outputStream.writeUTF(sequence);
            } catch (IOException ignored) {
            }
        }).start();
    }

    public void delete(int key, String sequence){
        new Thread(() -> {
            try (Socket socket = new Socket(ip, port)) {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.writeUTF("delete "+ key);
                outputStream.writeUTF(sequence);
            } catch (IOException ignored) {
            }
        }).start();
    }

    public void enter(Server ns, String sequence){
        new Thread(() -> {
            try (Socket socket = new Socket(ip, port)) {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.writeUTF("enter " +  ns.id + " " + ns.ip.getHostAddress() + " " + ns.port);
                outputStream.writeUTF(sequence);
            } catch (IOException ignored) {
            }
        }).start();
    }

    public void exit(Server ns, String sequence){
        new Thread(() -> {
            try (Socket socket = new Socket(ip, port)) {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.writeUTF("exit " + ns.id + " " + ns.ip.getHostAddress() + " " + ns.port);
                outputStream.writeUTF(sequence);
            } catch (IOException ignored) {
            }
        }).start();
    }

    public void close(Boolean success, String sequence){
        new Thread(() -> {
            try (Socket socket = new Socket(ip, port)) {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.writeUTF("close " + success);
                outputStream.writeUTF(sequence);
            } catch (IOException ignored) {
            }
        }).start();
    }

    public void insertAll(Map<Integer, String> values){
        new Thread(() -> {
            try (Socket socket = new Socket(ip, port)) {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.writeUTF("insertAll");
                for (Map.Entry<Integer, String> entry : values.entrySet()){
                    outputStream.writeInt( entry.getKey());
                    outputStream.writeUTF( entry.getValue());
                }
                values.clear();
            } catch (IOException ignored) {
            }
        }).start();
    }

    public void setPost(Server ns){
        new Thread(() -> {
            try (Socket socket = new Socket(ip, port)) {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                int id = 0;
                String ip = "";
                int port = 0;
                if(ns != null){
                    id = ns.id;
                    ip = ns.ip.getHostAddress();
                    port = ns.port;
                }
                outputStream.writeUTF("post " +  id + " " + ip + " " + port);
            } catch (IOException ignored) {
            }
        }).start();
    }

    public void setPre(Server ns){
        new Thread(() -> {
            try (Socket socket = new Socket(ip, port)) {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                int id = 0;
                String ip = "";
                int port = 0;
                if(ns != null){
                    id = ns.id;
                    ip = ns.ip.getHostAddress();
                    port = ns.port;
                }
                outputStream.writeUTF("pre " +  id + " " + ip + " " + port);
            } catch (IOException ignored) {
            }
        }).start();
    }

    public void message(String message){
        new Thread(() -> {
            try (Socket socket = new Socket(ip, port)) {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.writeUTF("message " + message);
            } catch (IOException ignored) {
            }
        }).start();
    }
}