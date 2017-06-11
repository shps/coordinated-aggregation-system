package se.kth.stream;

/**
 * Created by Hooman on 2017-06-11.
 */
public class Poisson {

    /**
     *
     * @param lambda
     * @return
     */
    public static int getPoisson(double lambda) {
        double L = Math.exp(-lambda);
        double p = 1.0;
        int k = 0;

        do {
            k++;
            p *= Math.random();
        } while (p > L);

        return k - 1;
    }

}
