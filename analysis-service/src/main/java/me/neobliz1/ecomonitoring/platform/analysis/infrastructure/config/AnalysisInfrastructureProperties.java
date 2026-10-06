package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "spring")
public class AnalysisInfrastructureProperties {

    private final Redis redis = new Redis();
    private final Kafka kafka = new Kafka();
    private final Grpc grpc = new Grpc();

    @Getter
    @Setter
    public static class Redis {

        private final Records records = new Records();

        @Getter
        @Setter
        public static class Records {

            private Integer ttl;
        }
    }

    @Getter
    @Setter
    public static class Kafka {

        private final Topic topic = new Topic();
        private final Streams streams = new Streams();
        private String serviceName;

        @Getter
        @Setter
        public static class Topic {

            private String weatherLive;
            private String weatherRaw;
            private String weatherHistory;
        }

        @Getter
        @Setter
        public static class Streams {

            private final Properties properties = new Properties();
            private final Pipeline pipeline = new Pipeline();

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

            @Getter
            @Setter
            public static class Pipeline {

                private final Name name = new Name();

                @Getter
                @Setter
                public static class Name {

                    private final AggregationProcessor aggregationProcessor = new AggregationProcessor();
                    private final DeduplicationProcessor deduplicationProcessor = new DeduplicationProcessor();

                    @Getter
                    @Setter
                    public static class AggregationProcessor {

                        private Integer interval;
                    }

                    @Getter
                    @Setter
                    public static class DeduplicationProcessor {

                        private Long interval;
                    }
                }
            }
        }
    }

    @Getter
    @Setter
    public static class Grpc {

        private final Client client = new Client();

        @Getter
        @Setter
        public static class Client {

            private final Channel channel = new Channel();

            @Getter
            @Setter
            public static class Channel {

                private final HistoryService historyService = new HistoryService();

                @Getter
                @Setter
                public static class HistoryService {

                    private String serviceName;
                    private String servicePort;
                }
            }
        }
    }
}