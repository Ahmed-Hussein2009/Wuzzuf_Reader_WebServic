package WuzzufApplication.service;

import WuzzufApplication.model.JobDetails;

import java.util.*;
import java.util.stream.Collectors;

public class JobsDataService {

    public static Map<String, Long> FilterJobsByTitle(List<JobDetails> jobs) {
        HashMap<String, Long> titles = new HashMap<>();
        for (JobDetails j : jobs) {
            String title = j.getTitle();
            if (titles.containsKey(title))
                titles.replace(title, titles.get(title) + 1L);
            else
                titles.put(title, 1L);
        }
//        for (Map.Entry<String, Long> p : titles.entrySet())
//            System.out.println(p.getValue() + ", " + p.getKey());
        return titles;
    }

    public static Map<String, Long> FilterJobsByCountry(List<JobDetails> jobs) {
        HashMap<String, Long> countries = new HashMap<>();
        for (JobDetails j : jobs) {
            String country = j.getCountry();
            if (countries.containsKey(country))
                countries.replace(country, countries.get(country) + 1L);
            else
                countries.put(country, 1L);
        }
//        for (Map.Entry<String, Long> p : countries.entrySet())
//            System.out.println(p.getValue() + ", " + p.getKey());
        return countries;
    }

    public static Map<String, Long> FilterJobsByLevel(List<JobDetails> jobs) {
        HashMap<String, Long> levels = new HashMap<>();
        for (JobDetails j : jobs) {
            String level = j.getLevel();
            if (levels.containsKey(level))
                levels.replace(level, levels.get(level) + 1L);
            else
                levels.put(level, 1L);
        }
//        for (Map.Entry<String, Long> p : levels.entrySet())
//            System.out.println(p.getValue() + ", " + p.getKey());
        return levels;
    }

    public static Map<String, Long> FilterJobsByYearsExp(List<JobDetails> jobs) {
        HashMap<String, Long> yearsExp = new HashMap<>();
        for (JobDetails j : jobs) {
            String exp = j.getYearsExp();
            if (yearsExp.containsKey(exp))
                yearsExp.replace(exp, yearsExp.get(exp) + 1L);
            else
                yearsExp.put(exp, 1L);
        }
//        for (Map.Entry<String, Long> p : yearsExp.entrySet())
//            System.out.println(p.getValue() + ", " + p.getKey());
        return yearsExp;
    }

    public static LinkedHashMap<String, Long> sortMap(Map<String, Long> map, int topElements) {
        // https://mkyong.com/java/how-to-sort-a-map-in-java/
        List<Map.Entry<String, Long>> list = new LinkedList<>(map.entrySet());

        list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        int i = 0;
        if (topElements == -1)
            topElements = list.size();
        LinkedHashMap<String, Long> sortedMap = new LinkedHashMap<>();
        for (Map.Entry<String, Long> m : list){
            sortedMap.put(m.getKey(), m.getValue());
            ++i;
            if (i >= topElements) break;
        }

        return sortedMap;
    }

    public static void printMap(Map<String, Long> map) {
        for (Map.Entry<String, Long> t : map.entrySet())
            System.out.println(t.getKey() + ", " + t.getValue());

        String sep = new String(new char[77]).replace("\0", "=");
        System.out.println(sep);
    }
}
