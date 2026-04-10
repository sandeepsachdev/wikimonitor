package com.wikipedia.monitor.controller;

import com.wikipedia.monitor.model.DailyVerse;
import com.wikipedia.monitor.service.VerseService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class VerseController {

    private final VerseService verseService;

    public VerseController(VerseService verseService) {
        this.verseService = verseService;
    }

    @GetMapping("/api/verse/today")
    public DailyVerse getTodayVerse() {
        return verseService.getTodayVerse();
    }
}
