package ch.hostettler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Latency {
    
    public void calculateLatency(String url) {
        final String regex = "jdbc:sqlserver:\\/\\/(.+):([0-9]+);";
    
        final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);
        final Matcher matcher = pattern.matcher(url);
    
        while (matcher.find()) {
            String host = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            latency(host, port);
        }
    }
    
    private void latency(String host, int port) {
        for (int i = 0; i < 10; i++) {
            Socket s = new Socket();
            SocketAddress a = new InetSocketAddress(host, port);
            int timeoutMillis = 2000;
            long start = System.nanoTime();
            try {
                s.connect(a, timeoutMillis);
            } catch (SocketTimeoutException e) {
                // timeout
            } catch (IOException e) {
                // some other exception
            }
            long stop = System.nanoTime();
            System.out.println("ping " + ((double) stop - (double) start) / (double) 1_000_000 + "ms");
            try {
                s.close();
            } catch (IOException e) {
                // closing failed
            }
        }
    }
}

