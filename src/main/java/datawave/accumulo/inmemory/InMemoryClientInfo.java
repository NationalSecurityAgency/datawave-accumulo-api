package datawave.accumulo.inmemory;

import java.util.Optional;
import java.util.Properties;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientInfoImpl;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.ClientProperty;

public class InMemoryClientInfo extends ClientInfoImpl {
    
    public InMemoryClientInfo(Credentials credentials, Optional<AuthenticationToken> tokenOpt) {
        super(toProperties(credentials), tokenOpt);
    }
    
    private static Properties toProperties(Credentials credentials) {
        Properties props = new Properties();
        props.put(ClientProperty.INSTANCE_NAME.getKey(), InMemoryAccumulo.INSTANCE_NAME);
        props.put(ClientProperty.AUTH_PRINCIPAL.getKey(), credentials.getPrincipal());
        props.put(ClientProperty.AUTH_TOKEN.getKey(), new String(((PasswordToken) (credentials.getToken())).getPassword()));
        props.put(ClientProperty.AUTH_TYPE.getKey(), "password");
        return props;
    }
}
