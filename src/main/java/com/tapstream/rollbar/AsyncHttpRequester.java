package com.tapstream.rollbar;

import io.prometheus.client.Gauge;
import org.asynchttpclient.*;
import org.asynchttpclient.uri.Uri;

import java.io.IOException;
import java.util.Map;

public class AsyncHttpRequester implements IHttpRequester {
    private static final Gauge ACTIVE_CONNECTIONS_GAUGE = Gauge.build()
            .name("rollbar_logback_active_connections")
            .help("Total active connections to rollbar system.")
            .register();

    private static AsyncHttpClient asyncHttpClient = new DefaultAsyncHttpClient();

    private int timeout = 5000;

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    @Override
    public int send(HttpRequest request) throws IOException {
        RequestBuilder requestBuilder = new RequestBuilder()
                .setMethod(request.getMethod())
                .setUri(Uri.create(request.getUrl().toString()))
                .setRequestTimeout(timeout)
                .setBody(request.getBody());

        for (Map.Entry<String, String> pair : request.getHeaders().entrySet()) {
            requestBuilder.addHeader(pair.getKey(), pair.getValue());
        }

        //TODO limit max concurrent connections!
        ACTIVE_CONNECTIONS_GAUGE.inc();
        asyncHttpClient.prepareRequest(requestBuilder
        ).execute(new AsyncCompletionHandler<Response>() {
            @Override
            public Response onCompleted(Response response) throws Exception {
                ACTIVE_CONNECTIONS_GAUGE.dec();
                int statusCode = response.getStatusCode();
                if (statusCode >= 200 && statusCode <= 299) {
                    // Everything went OK
                } else {
                    System.err.println("Non-2xx response from Rollbar: " + statusCode);
                }

                // ignore.
                return response;
            }

            @Override
            public void onThrowable(Throwable t) {
                ACTIVE_CONNECTIONS_GAUGE.dec();
                t.printStackTrace();
                // ignore.
            }
        });

        return 200;
    }
}
