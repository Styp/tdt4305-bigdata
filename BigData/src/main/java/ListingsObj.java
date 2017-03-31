import com.google.common.base.CharMatcher;
import com.google.common.collect.Lists;

import javax.swing.text.html.parser.Parser;
import java.io.Serializable;
import java.util.*;

/**
 * Created by martin on 30.03.17.
 */
public class ListingsObj implements Serializable {

    public int listingsId;
    public int neighborhoodId;
    public String description = "";
    public double price;
    public double longitude;
    public double latitude;
    public String room_type;
    public List<String> amenities;
    public double distance;
    public String name;

    private HashMap<String, Double> termFrequencyHashMap;
    private HashMap<String, Double> weightedTermFrequencyHashMap;
    private int totalWords = -1;

    public ListingsObj(String line) {

        String[] parts = line.split("\t");

        //Set fields
        this.price = ParserHelper.doubleParse(parts[65].replace("$", "").replace(",", ""));
        this.room_type = parts[81];
        this.longitude = ParserHelper.doubleParse(parts[54]) * Math.PI / 180;
        this.latitude = ParserHelper.doubleParse(parts[51]) * Math.PI / 180;
        this.name = parts[60];

        String tmpStringOnlyLetters = CharMatcher.is(' ')
                .or(CharMatcher.is(','))
                .or(CharMatcher.javaLetter())
                .retainFrom(parts[2]).toLowerCase();

        amenities = Arrays.asList(tmpStringOnlyLetters.split(","));

        String tmpString = Arrays.asList(parts[19]).toString();
        String descriptionStringOnlyLetters = CharMatcher.is(' ')
                .or(CharMatcher.javaLetter())
                .retainFrom(tmpString).toLowerCase();
        this.description = descriptionStringOnlyLetters;

        this.listingsId = ParserHelper.integerParse(parts[43]);

    }

    public Set<String> getDescriptionAsSet(){
        List<String> descriptionList = new ArrayList<String>(Arrays.asList(this.description.split(" ")));
        Set<String> descriptionSet = new HashSet<>(descriptionList);

        return descriptionSet;
    }

    public boolean isHeader() {
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

    public boolean containsTerm(String term){
        return getTermFrequency().containsKey(term);
    }

    @Override
    public String toString() {
        return "ListingsObj{" +
                "listingsId=" + listingsId +
                ", amenities=" + amenities.toString() +
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

    public int numberOfMatchingAmenities(ListingsObj listingsObj){
        int counter = 0;
        for(String s : this.amenities){
            if(listingsObj.amenities.contains(s)){
                counter++;
            }
        }

        return counter;
    }
    public double getDistance(ListingsObj obj) {
        double dlon = longitude - obj.longitude;
        double dlat = latitude - obj.latitude;
        double a = (Math.sin(dlat / 2) * Math.sin(dlat / 2)) + Math.cos(obj.latitude) * Math.cos(latitude) * (Math.sin(dlon / 2) * Math.sin(dlon / 2));
        double c = 2 * Math.asin(Math.sqrt(a));
        int r = 6371;
        return c * r;
    }
}
