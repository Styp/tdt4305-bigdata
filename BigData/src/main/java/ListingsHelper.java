import org.apache.spark.api.java.JavaRDD;

public class ListingsHelper {

    public static ListingsObj getObjectForListingsId(JavaRDD<ListingsObj> allListings, int listingsId){

        ListingsObj listingsObj;
        try {
            listingsObj = allListings
                    .filter(currentObj-> currentObj.listingsId == listingsId).first();
        } catch(UnsupportedOperationException e){
            throw new RuntimeException("Listings ID NOT FOUND!");
        }

        return listingsObj;
    }

}
