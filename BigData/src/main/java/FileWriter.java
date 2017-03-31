import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import scala.Tuple2;
import scala.Tuple5;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Created by martin on 31.03.17.
 */
public class FileWriter {

    public static void write2(String filename, List<Tuple2> list) throws IOException {
        File file = new File(filename);

        List<String> trimmed = Lists.transform(list, t -> t._1() + "\t" + t._2());
        String outputStr = Joiner.on("\n").join(trimmed);

        Files.write(outputStr, file, Charsets.UTF_8);
    }



    public static void write5(String filename, List<Tuple5> list) throws IOException {
        File file = new File(filename);

        List<String> trimmed = Lists.transform(list, t -> t._1() + "\t" + t._2() + "\t" + t._3() + "\t" + t._4() + "\t" + t._5());
        String outputStr = Joiner.on("\n").join(trimmed);

        Files.write(outputStr, file, Charsets.UTF_8);
    }

}
