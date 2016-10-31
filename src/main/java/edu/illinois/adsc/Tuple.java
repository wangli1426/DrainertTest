package edu.illinois.adsc;

import java.util.Random;

/**
 * Created by robert on 31/10/16.
 */
public class Tuple {
    static public int tupleLength = 1024;
    private byte[] bytes = new byte[tupleLength];
    public Integer tupleId = new Random().nextInt();
}
