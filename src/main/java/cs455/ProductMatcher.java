package cs455;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

//import org.apache.commons.csv.*;

public class ProductMatcher {

    static HashMap<String, ArrayList<Product>> productsMap = new HashMap<>();

    public static void main(String[] args) throws IOException {
//        initBfpd();
//        BufferedWriter out = new BufferedWriter(new FileWriter("/Users/keegan/insta-bfpd.csv"));
//        CSVPrinter printer = new CSVPrinter(out,
//                CSVFormat.DEFAULT.withHeader("instacart_id", "instacart_name", "bfpd_id", "bfpd_name", "match_weight"));
//
//        Reader in = new FileReader("/Users/keegan/instacart/products.csv");
//        Iterator<CSVRecord> it  = CSVFormat.DEFAULT.parse(in).iterator();
//        it.next();
//        while(it.hasNext()) {
//            CSVRecord record = it.next();
//            Product product = new Product();
//            product.id = Integer.parseInt(record.get(0));
//            product.name = record.get(1);
//            for(String word : product.name.split(" "))
//                product.words.add(word.toLowerCase());
//
//            Match bestMatch = findBestMatch(product);
//
//            printer.printRecord(product.id, product.name, bestMatch.bfpdProduct.id, bestMatch.bfpdProduct.name, bestMatch.matchWeight);
//        }
//        out.close();
    }

    static Match findBestMatch(Product instaP) {
        Match best = new Match(0, new Product());
        for(String word : instaP.words) {
            if(!productsMap.containsKey(word))
                continue;

            for(Product bfpdP : productsMap.get(word)) {
                double matchWeight = getMatchWeight(instaP, bfpdP);
                if(matchWeight > best.matchWeight) {
                    best = new Match(matchWeight, bfpdP);
                }
            }
        }
        return best;
    }

    static double getMatchWeight(Product p1, Product p2) {
        int intersect = 0;

        for(String word : p1.words) {
            if(p2.words.contains(word))
                ++intersect;
        }

        return (double) intersect / ( p1.words.size() + p2.words.size() - intersect);
    }

    static void initBfpd() throws IOException {
//        Reader bfpdIn = new FileReader("/Users/keegan/bfpd/Products.csv");
//        Iterator<CSVRecord> it  = CSVFormat.DEFAULT.parse(bfpdIn).iterator();
//        it.next();
//        while(it.hasNext()) {
//            CSVRecord record = it.next();
//            Product bfpdProduct = new Product();
//            bfpdProduct.id = Integer.parseInt(record.get(0));
//            bfpdProduct.name = record.get(1);
//            for(String word : bfpdProduct.name.split(" ")) {
//                String wordLower = word.toLowerCase();
//                bfpdProduct.words.add(wordLower);
//                addProduct(wordLower, bfpdProduct);
//            }
//        }
    }

    static void addProduct(String word, Product product) {
        if(!productsMap.containsKey(word))
            productsMap.put(word, new ArrayList<>());

        productsMap.get(word).add(product);
    }

}

class Match {
    double matchWeight;
    Product bfpdProduct;

    public Match(double matchWeight, Product bfpdProduct) {
        this.matchWeight = matchWeight;
        this.bfpdProduct = bfpdProduct;
    }
}

class Product {
    int id = 0;
    String name = "";
    HashSet<String> words = new HashSet<>();
}
