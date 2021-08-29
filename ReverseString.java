import org.junit.Test;

public class ReverseString {

    public String reverseStr(String str) {
        char[] ch = str.toCharArray();
        int right = str.length() - 1;
        for (int left = 0; left < right; left++, right--) {
            char temp = ch[left];
            ch[left] = ch[right];
            ch[right] = temp;
        }
        return String.copyValueOf(ch);
    }

    @Test
    public void testReverseStr() {
        String str = "hello world";
        System.out.println("input string: " + str);
        System.out.println("reversed string: " + reverseStr(str));
        System.out.println("get back original input string: " + reverseStr(reverseStr(str)));
    }
}
