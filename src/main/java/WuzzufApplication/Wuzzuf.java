package WuzzufApplication;

import WuzzufApplication.service.spark;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Wuzzuf {

    public static void main(String[] args) {
        spark s = new spark();
        SpringApplication.run(Wuzzuf.class, args);
    }
}
