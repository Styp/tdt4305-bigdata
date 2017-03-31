import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;

/**
 * Created by martin on 31.03.17.
 */
public class ParserHelper {

    public static int integerParse(String s) {
        Integer maybeValue = Ints.tryParse(s);
        if(maybeValue == null){
            return -1;
        }
        return (int) maybeValue;
    }

    public static double doubleParse(String s) {
        Double maybeValue = Doubles.tryParse(s);
        if(maybeValue == null){
            return -1.0;
        }
        return (double) maybeValue;
    }

}
