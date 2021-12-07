package org.insight_centre.aceis.io.streams;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

public class DataStream implements Runnable{
    boolean stop = false;
    ServerSocket serverSocket;
    Socket socket;
    public Set<OutputStream> out;
    String streamURL;
    int portNumber;

    public DataStream(int portNumber, String streamURL) {
        this.portNumber = portNumber;
        this.streamURL = streamURL;
    }

    public void send(File file) throws Exception {
        // sendfile
        byte[] mybytearray = new byte[(int) file.length() + 1];
        InputStream in = new BufferedInputStream(new FileInputStream(file));
        in.read(mybytearray, 0, mybytearray.length);
        for(OutputStream o : out) {
            o.write(mybytearray, 0, mybytearray.length);
            o.flush();
        }
    }

    public void send(String string) throws Exception {
        for(OutputStream o : out) {
            o.write(string.getBytes(StandardCharsets.UTF_8));
            o.flush();
        }
    }

    public void stop() {
        stop = true;
    }

    @Override
    public void run() {
        try {
            out = new HashSet<>();
            serverSocket = new ServerSocket(portNumber);
            while (!stop) {
                socket = serverSocket.accept();
                out.add(socket.getOutputStream());
            }
            serverSocket.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
