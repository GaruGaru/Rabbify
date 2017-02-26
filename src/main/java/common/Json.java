package common;

import com.google.gson.Gson;

/**
 * Created by Tommaso Garuglieri on 21/09/2016.
 */
public class Json {

    private static Gson gson = null;

    public static Gson getInstance() {
        if (gson == null)
            gson = new Gson();
        return gson;
    }

}
