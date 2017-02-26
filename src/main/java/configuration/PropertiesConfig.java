package configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Tommaso Garuglieri on 19/09/2016.
 */
public class PropertiesConfig implements IConfiguration {

    private Properties properties;

    private PropertiesConfig(String file) {
        this.properties = new Properties();
        try {
            this.load(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void load(String file) throws IOException {
        InputStream input = new FileInputStream(file);
        properties.load(input);
    }

    @Override
    public String get(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    @Override
    public boolean contains(String... keys) {
        for (String key : keys)
            if (!properties.containsKey(key))
                return false;
        return true;
    }

    public static IConfiguration fromFile(String file) {
        return new PropertiesConfig(file);
    }
}
