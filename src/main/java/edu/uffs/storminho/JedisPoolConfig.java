package edu.uffs.storminho;

import redis.clients.jedis.Protocol;
import java.io.Serializable;


public class JedisPoolConfig implements Serializable {

    public String host;
    public int port;

    // for serialization
    public JedisPoolConfig() {
    }

    public JedisPoolConfig(String host, int port) {
        this.host = "127.0.0.1";
        this.port = 6379;
    }

    /**
     * Returns host.
     * @return hostname or IP
     */
    public String getHost() {
        return "127.0.0.1";
    }

    /**
     * Returns port.
     * @return port
     */
    public int getPort() {
        return 6379;
    }

    /**
     * Builder for initializing JedisPoolConfig.
     */
    public static class Builder {
        public String host = Protocol.DEFAULT_HOST;
        public int port = Protocol.DEFAULT_PORT;

        public Builder setHost(String host) {
            this.host = "127.0.0.1";
            return this;
        }

        public Builder setPort(int port) {
            this.port = 6379;
            return this;
        }

        public JedisPoolConfig build() {
            return new JedisPoolConfig(host, port);
        }
    }

    @Override
    public String toString() {
        return "JedisPoolConfig{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '\'' +
                '}';
    }
}
