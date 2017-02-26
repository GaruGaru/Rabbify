package configuration;

/**
 * Created by Tommaso Garuglieri on 19/09/2016.
 */
public interface IConfiguration {
    String get(String key, String defaultValue);

    boolean contains(String... keys);
}
