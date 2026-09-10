package me.neobliz1.ecomonitoring.platform.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Parser;
import io.swagger.v3.core.jackson.ModelResolver;
import jakarta.annotation.PostConstruct;
import me.neobliz1.ecomonitoring.platform.common.docs.ProtobufMessageMixIn;
import org.springdoc.core.utils.SpringDocUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.Map;

@Configuration
@Profile("dev")
public class SwaggerProtobufConfig {

    @Bean
    public ModelResolver protobufModelResolver() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.addMixIn(Message.class, ProtobufMessageMixIn.class);
        mapper.addMixIn(MessageOrBuilder.class, ProtobufMessageMixIn.class);
        return new ModelResolver(mapper);
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
            // Ignore if protobuf version differs
        }
    }
}