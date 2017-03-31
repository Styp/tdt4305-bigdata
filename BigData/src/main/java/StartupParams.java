import java.io.Serializable;

public class StartupParams implements Serializable {

    public String filePath;
    public int listingId;
    public String neighborhood;
    public Mode runMode;
    public Double percentage;
    public Double kiloMeters;
    public Integer topN;
    public String date;

    public enum Mode {LISTING, NEIGHBOURHOOD}

}

