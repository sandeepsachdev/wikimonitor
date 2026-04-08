package com.wikipedia.monitor.controller;

import com.wikipedia.monitor.model.CryptoTick;
import com.wikipedia.monitor.service.CryptoPriceService;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class CryptoPriceController {

    private final CryptoPriceService cryptoPriceService;

    public CryptoPriceController(CryptoPriceService cryptoPriceService) {
        this.cryptoPriceService = cryptoPriceService;
    }

    @GetMapping(value = "/stream/crypto", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<CryptoTick>> streamPrices() {
        return cryptoPriceService.getTickStream()
                .map(tick -> ServerSentEvent.<CryptoTick>builder()
                        .event("tick")
                        .data(tick)
                        .build());
    }
}
