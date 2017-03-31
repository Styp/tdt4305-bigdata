import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by martin on 30.03.17.
 */
public class ListingsObj implements Serializable {

    public int listingsId;
    public int neighborhoodId;
    public String description;
    public double price;
    public double longitude;
    public double latitude;
    public String room_type;

    private HashMap<String, Double> termFrequencyHashMap;
    private HashMap<String, Double> weightedTermFrequencyHashMap;
    private int totalWords = -1;

    public boolean isHeader(){
        return this.description.equals("description");
    }

    public int getTotalWords(){
        if(totalWords == -1){
            if(description == null){
                throw new RuntimeException("Field Description is EMPTY!");
            }

            List<String> strings = Arrays.asList(description.split(" "));
            totalWords = strings.size();

        }
        return totalWords;

    }

    public HashMap<String, Double> getTermFrequency(){
        if(termFrequencyHashMap == null){
            List<String> strings = Arrays.asList(description.split(" "));

            HashMap<String, Double> termFrequencyMap = new HashMap<>();

            for(String s : strings){
                if(termFrequencyMap.containsKey(s)){
                    Double wordCount = termFrequencyMap.get(s);
                    termFrequencyMap.put(s,wordCount+1);
                } else{
                    termFrequencyMap.put(s,1.0);
                }
            }
            this.termFrequencyHashMap = termFrequencyMap;
        }

        return this.termFrequencyHashMap;
    }

    public HashMap<String, Double> getWeightedTermFrequency(){
        int totalWords = getTotalWords();

        HashMap<String, Double> termFrequency = getTermFrequency();

        for (Map.Entry<String, Double> entry : termFrequency.entrySet()) {
            entry.setValue(entry.getValue() / totalWords);
        }
        return termFrequency;
    }

  //  public Double getTermOccurance(String term){
  //      if(!getTermFrequency().containsKey(term)){
  //          return 0.0;
  //      }
  //      return getTermFrequency().get(term);
  //  }

    public boolean containsTerm(String term){
        return getTermFrequency().containsKey(term);
    }


    @Override
    public String toString() {
        return "ListingsObj{" +
                "listingsId=" + listingsId +
                ", neighborhoodId=" + neighborhoodId +
                ", description='" + description + '\'' +
                ", price=" + price +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", room_type='" + room_type + '\'' +
                ", termFrequencyHashMap=" + termFrequencyHashMap +
                ", weightedTermFrequencyHashMap=" + weightedTermFrequencyHashMap +
                ", totalWords=" + totalWords +
                '}';
    }
}
