package WuzzufApplication.model;

public class JobDetails {
    String title;
    String company;
    String location;
    String type;
    String level;
    String yearsExp;
    String country;
    String skills;

    public JobDetails(String title, String company, String location, String type,
                      String level, String yearsExp, String country, String skills){
        this.title = title;
        this.company = company;
        this.location = location;
        this.type = type;
        this.level = level;
        this.yearsExp = yearsExp;
        this.country = country;
        this.skills = skills;
    }

    @Override
    public String toString(){
        return String.format("title = %s, company = %s, location = %s, type = %s, " +
                "level = %s, yearsExp = %s, country = %s, skills = %s",
                    title, company, location, type, level, yearsExp, country, skills);
    }

    public String getTitle(){
        return title;
    }

    public String getCompany() {
        return company;
    }

    public String getLocation() {
        return location;
    }

    public String getType() {
        return type;
    }

    public String getLevel() {
        return level;
    }

    public String getYearsExp() {
        return yearsExp;
    }

    public String getCountry() {
        return country;
    }

    public String getSkills() {
        return skills;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public void setYearsExp(String yearsExp) {
        this.yearsExp = yearsExp;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setSkills(String skills) {
        this.skills = skills;
    }
}
