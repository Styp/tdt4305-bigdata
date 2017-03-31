import java.io.Serializable;

/**
 * Created by martin on 31.03.17.
 */
public class NeighborhoodObj implements Serializable {
    public String name;
    public Integer id;

    public NeighborhoodObj(String line) {
        String[] parts = line.split("\t");

        this.name = tryToAssign(parts, 1);
        this.id = ParserHelper.integerParse(parts[0]);
    }

    @Override
    public String toString() {
        return "NeighborhoodObj{" +
                "name='" + name + '\'' +
                ", id=" + id +
                '}';
    }

    private String tryToAssign(String parts[], int pos){
        try{
            return parts[pos];
        } catch(ArrayIndexOutOfBoundsException e){
            return "";
        }
    }
}
