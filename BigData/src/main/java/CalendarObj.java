import java.io.Serializable;

/**
 * Created by jonas on 31.03.17.
 */
public class CalendarObj implements Serializable{

    public int id;
    public String date;
    public boolean availability;


    public CalendarObj(String line) {
        String[] parts = line.split("\t");
        this.id = ParserHelper.integerParse(parts[0]);
        this.date = parts[1];
        this.availability = parts[2].equals("t");
    }
}
