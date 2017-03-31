import java.io.Serializable;

public class StartupParams implements Serializable {

    public String filePath;
    public int listingId;
    public Mode runMode;

    public enum Mode {LISTING, NEIGHBOURHOOD}

}

