import org.szymie.BlockingMap;

public class TT {

    public static void main(String[] args) {

        BlockingMap<String, String> b = new BlockingMap<>();

        System.err.println(b.put("a", "a"));

        System.err.println(b.get("a"));
        System.err.println(b.get("a"));

        b.remove("a");

        System.err.println(b.get("a"));

        System.err.println("end");
    }
}
