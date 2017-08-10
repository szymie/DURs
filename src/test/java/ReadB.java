public class ReadB {

    public static void main(String[] args) {

        NettySerializableTransaction t = new NettySerializableTransaction();

        t.begin();
        String a = t.read("a");
        System.err.println("a: " + a);
        t.commit();
    }

}
