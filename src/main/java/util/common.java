package util;

/**
 * Created by wangxufeng on 2015/1/22.
 */
public class common {

    public void common() {

    }

    public static boolean isNumeric(String str){
        int iStart = str.length();
        for (int i = iStart; --i>=0 ; ){
            if (!Character.isDigit(str.charAt(i))){
                return false;
            }
        }
        return true;
    }
}
