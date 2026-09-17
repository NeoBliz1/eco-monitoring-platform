package me.neobliz1.ecomonitoring.platform.history.infrastructure.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "spring")
public class HistoryInfrastructureProperties {

    private final Data data = new Data();
    private final Kafka kafka = new Kafka();
    private final Datasource datasource = new Datasource();

    @Getter
    @Setter
    public static class Datasource {

        private final DataPool dataPool = new DataPool();
        private String database;
        private String schemaName;
        private String serviceName;

        @Getter
        @Setter
        public static class DataPool {

            private String name;
            private int minIdle;
            private int maxPoolSize;
            private int idleTimeout;
            private int connectionTimeout;
        }
    }

    @Getter
    @Setter
    public static class Data {

        private final Redis redis = new Redis();
        private final Hibernate hibernate = new Hibernate();

        @Getter
        @Setter
        public static class Hibernate {

            private final Cache cache = new Cache();

            @Getter
            @Setter
            public static class Cache {

                private final Level1 level1 = new Level1();
                private final Level2 level2 = new Level2();

                @Getter
                @Setter
                public static class Level1 {

                    private int l1CacheMaxSize;
                    private int l1CacheTtlMinutes;
                    private String invalidationTopicName;
                }

                @Getter
                @Setter
                public static class Level2 {

                    private final Region region = new Region();

                    @Getter
                    @Setter
                    public static class Region {

                        private final L2CacheConfig weatherMapBucket = new L2CacheConfig();
                        private final L2CacheConfig weatherGridCells = new L2CacheConfig();
                        private final L2CacheConfig spatialQueryResults = new L2CacheConfig();
                    }

                    @Getter
                    @Setter
                    public static class L2CacheConfig {

                        private int l2CacheMaxSize;
                        private int l2CacheTtlAfterWriteMinutes;
                        private int l2CacheTtlAfterAccessMinutes;
                    }
                }
            }
        }

        @Getter
        @Setter
        public static class Redis {

            private String password;
            private String serviceName;
        }
    }

    @Getter
    @Setter
    public static class Kafka {

        private final Streams streams = new Streams();
        private final Consumer consumer = new Consumer();
        private String serviceName;

        @Getter
        @Setter
        public static class Consumer {

            private final Backoff backoff = new Backoff();

            @Getter
            @Setter
            public static class Backoff {

                private int interval;
                private int maxAttempts;
            }
        }

        @Getter
        @Setter
        public static class Streams {

            private final Properties properties = new Properties();

            @Getter
            @Setter
            public static class Properties {

                private final Schema schema = new Schema();

                @Getter
                @Setter
                public static class Schema {

                    private final Registry registry = new Registry();

                    @Getter
                    @Setter
                    public static class Registry {

                        private String url;
                    }
                }
            }
        }
    }
}