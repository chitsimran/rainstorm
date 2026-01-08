package com.example.rainstorm;

import rainstorm.Rainstorm;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class NetworkUtils {

    public static void sendProtoMessage(String ip, int port, com.google.protobuf.Message msg) {
        try (Socket socket = new Socket(ip, port)) {
            OutputStream out = socket.getOutputStream();
            msg.writeDelimitedTo(out);
            out.flush();
        } catch (Exception e) {
            System.err.println("[RainStorm] ERROR sending proto message to " + ip + ":" + port);
            e.printStackTrace();
        }
    }

    public static Rainstorm.ControlMessage sendAndReceiveProto(String ip, int port, Rainstorm.ControlMessage request, int timeoutMs) {
        try (Socket socket = new Socket(ip, port)) {
            socket.setSoTimeout(timeoutMs);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();
            request.writeDelimitedTo(out);
            out.flush();
            return Rainstorm.ControlMessage.parseDelimitedFrom(in);
        } catch (Exception e) {
            System.err.println("[NetworkUtils] sendAndReceiveProto FAILED: " + e.getMessage());
            return null;
        }
    }

    public static Rainstorm.Message sendAndReceiveProto(String ip, int port, Rainstorm.Message message) {
        try (Socket socket = new Socket(ip, port)) {
            socket.setSoTimeout(400);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();
            message.writeDelimitedTo(out);
            out.flush();
            return Rainstorm.Message.parseDelimitedFrom(in);
        } catch (Exception e) {
            System.err.println("[NetworkUtils] sendAndReceiveProto FAILED: " + e.getMessage());
            return null;
        }
    }
}
