package com.orderbook.rebuilder;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderbook.rebuilder.dto.OrderBookMultiCurrencyEvent;
import com.orderbook.rebuilder.model.Currency;
import com.orderbook.rebuilder.model.OrderBookSingleCurrencyEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.log4j.Logger;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.net.http.WebSocket.Builder;
import java.net.http.WebSocket.Listener;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;

import static com.orderbook.rebuilder.converters.OrderBookMultiCurrencyEventConverter.splitEventByCurrency;

public class OrderBookEventSource extends RichSourceFunction<OrderBookSingleCurrencyEvent> {

    private static final Logger logger = Logger.getLogger(OrderBookEventSource.class.getName());

    private static final URI url = URI.create("wss://api.hitbtc.com/api/3/ws/public");

    private boolean running = true;
    private boolean subscriptionSet = false;

    private final BlockingQueue<OrderBookMultiCurrencyEvent> messages = new ArrayBlockingQueue<>(100000);
    private StringBuilder buffer = new StringBuilder();

    private final ObjectMapper mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false)
        .configure(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, true);

    @Override
    public void run(SourceContext<OrderBookSingleCurrencyEvent> ctx) throws Exception {
        logger.info("run function called");

        subscribe(Arrays.asList(Currency.BTCUSDT, Currency.ETHBTC));

        final Object lock = ctx.getCheckpointLock();

        while (running) {
            synchronized (lock) {
                splitEventByCurrency(messages.take()).forEach(ctx::collect);
            }
        }

        running = false;
    }

    @Override
    public void cancel() {
        logger.info("Cancel function called");
        running = false;
    }

    private void subscribe(List<Currency> currencies) {
        HttpClient httpClient = HttpClient.newBuilder().build();
        Builder webSocketBuilder = httpClient.newWebSocketBuilder();

        WebSocket webSocket = webSocketBuilder
            .buildAsync(url, new WebSocketListener())
            .join();

        webSocket.sendText(
            "{\n" +
                "    \"method\": \"subscribe\",\n" +
                "    \"ch\": \"orderbook/full\",\n" +
                "    \"params\": {\n" +
                "        \"symbols\": [\"" + StringUtils.join(currencies, "\", \"") + "\"]\n" +
                "    }\n" +
                "}",
            true
        );
    }

    private class WebSocketListener implements Listener {

        @Override
        public void onOpen(WebSocket webSocket) {
            System.out.println("CONNECTED");
            Listener.super.onOpen(webSocket);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            System.out.println("Error occurred" + error.getMessage());
            Listener.super.onError(webSocket, error);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            //System.out.println("onText: " + data);
            if (subscriptionSet) {
                buffer.append(data);
                if (last) {
                    try {
                        OrderBookMultiCurrencyEvent event = mapper.readValue(buffer.toString(), OrderBookMultiCurrencyEvent.class);
                        event.setCreatedAt(Instant.now());
                        //logger.info("onText: event: " + event);
                        messages.put(event);
                        buffer = new StringBuilder();
                    } catch (InterruptedException e) {
                        logger.error("Interrupted!", e);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        logger.error("Something went wrong!", e);
                    }
                }
            } else {
                subscriptionSet = true;
            }
            return Listener.super.onText(webSocket, data, last);
        }

        @Override
        public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
            System.out.println("onPong: " + new String(message.array()));
            return WebSocket.Listener.super.onPong(webSocket, message);
        }
    }
}
