package solutions.boono.redisdispatcher;

import java.util.Properties;

public class Settings {
    private final String redisHost;
    private final int redisPort;

    public Settings(Properties properties) {
        redisHost = properties.getProperty("redis.host", "localhost");
        redisPort = Integer.parseInt(properties.getProperty("redis.port", "6379"));
    }

    public String getRedisHost() {
        return redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }
}
