package com.wikipedia.monitor.controller;

import com.wikipedia.monitor.model.DailyVerse;
import com.wikipedia.monitor.service.VerseService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

@RestController
public class VerseController {

    private final VerseService verseService;

    public VerseController(VerseService verseService) {
        this.verseService = verseService;
    }

    @GetMapping("/api/verse/today")
    public DailyVerse getVerse(@RequestParam(required = false) String date) {
        if (date != null && !date.isBlank()) {
            try {
                return verseService.getVerseForDate(LocalDate.parse(date));
            } catch (Exception ignored) {}
        }
        return verseService.getTodayVerse();
    }
}
