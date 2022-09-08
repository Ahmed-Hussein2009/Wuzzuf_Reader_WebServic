package WuzzufApplication.model;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WuzzufJobsCSVReader {

    public WuzzufJobsCSVReader() {}

    public static List<JobDetails> ReadCSVFile(String filename){

        List<JobDetails> jobs = new ArrayList<>();

        try {
            BufferedReader br = new BufferedReader(new FileReader(filename)); // src/main/resources/Wuzzuf_jobs.csv
            String line = br.readLine();

            if (line != null)
                line = br.readLine();

            while (line != null) {
                String[] features = line.split(",");

                // clean features input, if found a "..,..,...", that is a
                // multi-value feature, or text with commas in text,
                // this value is escaped with a " character, then
                // concat this element and subsequent elements until
                // find another element ends with "
                // after some observation, there are some records with
                // "" double double-quotations, not sure why is this
                // even a thing, but anyway that leads to some strings
                // to start and end with a " and followed by a comma,
                // such attributes doesn't need concat with subsequent
                // elements and searching for element that ends with "
                // All this handling might not be enough, I know it.
                // But I tested this handling against what nulls could
                // happen while input data, and only nulls found are
                // null Yrs of Exp, which is a thing in the data came in.
                // So, this handling is good enough.
                if (features.length > 8) {
                    String[] clean = new String[8];
                    int j = 0;
                    for (int i = 0; i < features.length; ++i)
                        if (features[i].startsWith("\"")) {
                            if (features[i].endsWith("\"")) {
                                clean[j++] = features[i].substring(1,features[i].length()-1);
                                continue;
                            }
                            clean[j] = features[i].substring(1) + ",";
                            i += 1;
                            while (!features[i].endsWith("\""))
                                clean[j] += features[i++] + ",";
                            clean[j++] += features[i].substring(0, features[i].length()-1);
                        } else
                            clean[j++] = features[i];
                    features = clean;
                }

                jobs.add(createJob(features));

                line = br.readLine();
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return jobs;
    }

    public static JobDetails createJob(String[] features){

        String title = features[0];
        String company = features[1];
        String location = features[2];
        String type = features[3];
        String level = features[4];
        String yearsExp = features[5];
        String country = features[6];
        String skills = features[7];

        return new JobDetails(title, company, location, type,
                            level, yearsExp, country, skills);
    }
}
