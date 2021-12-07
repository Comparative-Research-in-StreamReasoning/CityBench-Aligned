package org.insight_centre.aceis.utils.server;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class FileServer implements HttpHandler
{
    public HttpServer httpServer;
    public HttpContext context;
    public String content;
    public String path;
    public FileServer(int portNumber, String path, File file) throws IOException, InterruptedException
    {
        System.out.println("Path: " + path);
        this.path = path;
        this.content = Files.readString(file.toPath(), StandardCharsets.UTF_8);
        httpServer = HttpServer.create(new InetSocketAddress(portNumber), 0);
        String cxt;
        if(path.matches("(.)*http://localhost:(\\d)+(.)*"))
            cxt = path.split("http://localhost:(\\d)+")[1];
        else if(path.matches("(.)*http://127\\.0\\.0\\.1:(\\d)+(.)*"))
            cxt = path.split("http://127\\.0\\.0\\.1:(\\d)+")[1];
        else {
            System.out.println("Unexpected Failure");
            cxt = "";
            System.exit(1);
        }
        System.out.println(cxt);
        context = httpServer.createContext(cxt);
        context.setHandler(this);
        httpServer.start();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, content.getBytes(StandardCharsets.UTF_8).length);
        OutputStream os = exchange.getResponseBody();
        os.write(content.getBytes(StandardCharsets.UTF_8));
        os.close();
    }

    public void stop() {
        this.httpServer.stop(0);
    }
}
