package WuzzufApplication.service;

public class spark {

    public static String[] Tasks = null;
    public static String[] N = null;

    public spark(){
        if (Tasks != null && N != null)
            return;
        else{
            if (Tasks == null){
                int tasks = 17;
                Tasks = new String[tasks];
                for (int i = 0; i < tasks; ++i)
                    Tasks[i] = "";
            }
            if(N == null){
                int tasks = 17;
                N = new String[tasks];
                for (int i = 0; i < tasks; ++i)
                    N[i] = "";
            }
        }
    }
}
