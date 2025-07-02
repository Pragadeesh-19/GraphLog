package com.graphDB;

import com.graphDB.core.CausalLedger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class GraphLogApplication {

    private static final String LOG_FILE_PATH = "data/events.log";
    private static final int INITIAL_GRAPH_CAPACITY = 1000;

    public static void main(String[] args) {
        SpringApplication.run(GraphLogApplication.class, args);
    }

    @Bean
    public CausalLedger causalLedger() {
        return new CausalLedger(LOG_FILE_PATH, INITIAL_GRAPH_CAPACITY);
    }
}
