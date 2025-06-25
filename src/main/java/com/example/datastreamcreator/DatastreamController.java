package com.example.datastreamcreator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DatastreamController {

    @Autowired
    private DatastreamService datastreamService;

    @GetMapping("/create-datastream")
    public String createDatastream() {
        return datastreamService.createDatastream();
    }
    
    @GetMapping("/vaildata")
    public String vaildata() {
        return datastreamService.validateConfiguration();
    }
}