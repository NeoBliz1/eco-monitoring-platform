package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Parser;
import io.swagger.v3.core.jackson.ModelResolver;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import jakarta.annotation.PostConstruct;
import me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.adapter.inbound.web.docs.ProtobufMessageMixIn;
import org.springdoc.core.utils.SpringDocUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class SwaggerProtobufConfig {

    @Bean
    public ModelResolver protobufModelResolver() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.addMixIn(Message.class, ProtobufMessageMixIn.class);
        mapper.addMixIn(MessageOrBuilder.class, ProtobufMessageMixIn.class);
        return new ModelResolver(mapper);
    }

    @Bean
    public OpenAPI ecomonitoringPipelineOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("EcoMonitoring Telemetry Ingestion Pipeline")
                        .version("1.0.0")
                        .description("High-performance sensory data serialization ingestion engine running on Reactive Streams & Virtual Threads.")
                        .license(new License().name("Apache 2.0").url("https://springdoc.org")));
    }

    @PostConstruct
    public void bypassProtobufInternalReflectionLoops() {
        SpringDocUtils.getConfig().replaceWithClass(ByteString.class, String.class);
        SpringDocUtils.getConfig().replaceWithClass(Descriptors.Descriptor.class, Map.class);
        SpringDocUtils.getConfig().replaceWithClass(Descriptors.FileDescriptor.class, Map.class);
        SpringDocUtils.getConfig().replaceWithClass(Parser.class, Map.class);

        try {
            SpringDocUtils.getConfig().addSimpleTypesForParameterObject(
                    Class.forName("com.google.protobuf.GeneratedMessageV3"),
                    Class.forName("com.google.protobuf.MessageOrBuilder"),
                    Class.forName("com.google.protobuf.Message")
            );
            SpringDocUtils.getConfig().addRequestWrapperToIgnore(Class.forName("com.google.protobuf.GeneratedMessageV3"));
        } catch(ClassNotFoundException e) {
            // Safe fallback if class hierarchy differs in specific protobuf version stream
        }
    }
}