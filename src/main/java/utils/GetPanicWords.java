package utils;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;


public class GetPanicWords
{
    public static List<String> PANIC_WORDS_LIST;

    public GetPanicWords() throws Exception
    {
        PANIC_WORDS_LIST = new ArrayList<String>();
        this.parseWords(this.getPanicWords());
    }

    public String getPanicWords() throws Exception
    {
        String url = "https://api.datamuse.com//words?rel_trg=fear";
        URL website = new URL(url);
        URLConnection connection = website.openConnection();
        BufferedReader in = new BufferedReader( new InputStreamReader(connection.getInputStream(),"UTF8"));

        StringBuilder response = new StringBuilder();
        String inputLine;

        while ((inputLine = in.readLine()) != null)
            response.append(inputLine);

        in.close();

        return response.toString();
    }

    public void parseWords(String jsonMessage)
    {
        JSONArray jsonArray = new JSONArray(jsonMessage);

        for (int i = 0; i < jsonArray.length(); i++)
        {
            JSONObject jb = jsonArray.getJSONObject(i);
            String word = jb.getString("word");
            int score = jb.getInt("score");

            System.out.println(word + ": " + score );
            PANIC_WORDS_LIST.add(word);
        }
    }

    public List<String> getPanicWordsList()
    {
        return PANIC_WORDS_LIST;
    }

    public static void main(String[] args) throws Exception
    {
        GetPanicWords obj = new GetPanicWords();
        String jsonArray = obj.getPanicWords();
        obj.parseWords(jsonArray);

        System.out.println("\nList size: " + obj.getPanicWordsList().size());
    }
}
