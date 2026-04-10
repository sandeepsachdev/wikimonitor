package com.wikipedia.monitor.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.monitor.model.DailyVerse;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.time.LocalDate;
import java.util.List;

@Service
public class VerseService {

    private List<DailyVerse> verses;
    private final ObjectMapper objectMapper;

    public VerseService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() throws Exception {
        InputStream is = getClass().getClassLoader().getResourceAsStream("verses.json");
        verses = objectMapper.readValue(is, new TypeReference<>() {});
    }

    public DailyVerse getTodayVerse() {
        int dayOfYear = LocalDate.now().getDayOfYear();
        int index = (dayOfYear - 1) % verses.size();
        return verses.get(index);
    }
}
